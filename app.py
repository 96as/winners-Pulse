#!/usr/bin/env python3
"""
News Pulse — Streamlit Dashboard (T4 + Bonuses)
All-in-one: starts Spark streaming IN-PROCESS so memory sinks are visible,
displays charts, LLM summary, GDELT join, and topic clustering.
Run with:  streamlit run app.py
"""

import os
import time
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.clustering import KMeans

from llm_summary import get_llm_summary
from gdelt_loader import fetch_gdelt_events, gdelt_country_summary, join_gdelt_rss

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="News Pulse", page_icon="📰", layout="wide")
st.title("📰 News Pulse — Live Dashboard")
st.caption("Real-time RSS headlines → Spark Structured Streaming → LLM insight")

# ── Ensure data dir exists ───────────────────────────────────────────────────
os.makedirs("data/incoming", exist_ok=True)


# ── Spark session (created once, cached across Streamlit reruns) ─────────────
@st.cache_resource
def get_spark():
    spark = (
        SparkSession.builder
        .appName("NewsPulse")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions",
                "--add-opens java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens java.base/java.lang=ALL-UNNAMED "
                "--add-opens java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens java.base/java.io=ALL-UNNAMED "
                "--add-opens java.base/java.net=ALL-UNNAMED "
                "--add-opens java.base/java.nio=ALL-UNNAMED "
                "--add-opens java.base/java.util=ALL-UNNAMED "
                "--add-opens java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.cs=ALL-UNNAMED "
                "--add-opens java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens java.base/sun.util.calendar=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions",
                "--add-opens java.base/javax.security.auth=ALL-UNNAMED "
                "--add-opens java.base/java.lang=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = get_spark()

# ── Schema matching the ingester output ──────────────────────────────────────
SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("title",  StringType(), True),
    StructField("url",    StringType(), True),
    StructField("ts",     TimestampType(), True),
])

STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "it", "its", "as", "are", "was",
    "were", "be", "been", "being", "have", "has", "had", "do", "does",
    "did", "will", "would", "shall", "should", "may", "might", "can",
    "could", "not", "no", "nor", "so", "if", "then", "than", "that",
    "this", "these", "those", "what", "which", "who", "whom", "how",
    "when", "where", "why", "all", "each", "every", "both", "few",
    "more", "most", "other", "some", "such", "only", "own", "same",
    "too", "very", "just", "about", "above", "after", "again", "also",
    "any", "because", "before", "between", "during", "into", "over",
    "through", "under", "until", "up", "out", "off", "down", "he",
    "she", "we", "they", "me", "him", "her", "us", "them", "my", "your",
    "his", "our", "their", "says", "said", "new", "news", "get", "one",
    "two", "now", "even", "still", "much", "back", "first", "last",
    "many", "well", "way", "day", "man", "part", "long", "make",
    "like", "just", "know", "take", "come", "made", "find", "here",
    "thing", "going", "around", "year", "years", "say", "video",
}


# ── Start streaming queries IN-PROCESS (runs once) ──────────────────────────
@st.cache_resource
def start_streaming(_spark):
    """Launch the three streaming queries inside the Streamlit JVM
    so their memory sinks are queryable via spark.sql()."""
    # Guard: skip if queries already running (e.g. after cache clear)
    active = {q.name for q in _spark.streams.active}
    if "by_source" in active:
        return True

    stream = (
        _spark.readStream
        .schema(SCHEMA)
        .option("maxFilesPerTrigger", 5)
        .json("data/incoming")
    )

    # Q1 — headline count per source
    stream.groupBy("source").count() \
        .writeStream.outputMode("complete") \
        .format("memory").queryName("by_source").start()

    # Q2 — headline count per 1-hour tumbling window
    stream.withWatermark("ts", "2 hours") \
        .groupBy(F.window("ts", "1 hour")).count() \
        .writeStream.outputMode("complete") \
        .format("memory").queryName("by_window").start()

    # Q3 — top keywords
    stream.select(
        F.explode(
            F.regexp_extract_all(F.lower(F.col("title")), F.lit(r"([a-z]{3,})"))
        ).alias("word")
    ).filter(~F.col("word").isin(STOP_WORDS)) \
     .filter(F.length("word") >= 3) \
     .groupBy("word").count() \
     .writeStream.outputMode("complete") \
     .format("memory").queryName("top_words").start()

    return True


start_streaming(spark)


# ── Helper: safe SQL → Pandas ────────────────────────────────────────────────
def safe_query(sql: str) -> pd.DataFrame:
    try:
        return spark.sql(sql).toPandas()
    except Exception:
        return pd.DataFrame()


# ── Topic clustering (Bonus) ────────────────────────────────────────────────
def run_topic_clustering(n_clusters: int = 3) -> pd.DataFrame | None:
    """Batch TF-IDF + KMeans on accumulated headlines."""
    try:
        df = spark.read.schema(SCHEMA).json("data/incoming")
        if df.count() < n_clusters * 3:
            return None

        tokenizer = Tokenizer(inputCol="title", outputCol="raw_words")
        remover = StopWordsRemover(inputCol="raw_words", outputCol="filtered")
        htf = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=256)
        idf_model = IDF(inputCol="raw_features", outputCol="features")

        df2 = tokenizer.transform(df)
        df3 = remover.transform(df2)
        df4 = htf.transform(df3)
        df5 = idf_model.fit(df4).transform(df4)

        km = KMeans(k=n_clusters, seed=42, featuresCol="features")
        preds = km.fit(df5).transform(df5)
        return preds.select("source", "title", "prediction").toPandas() \
                     .rename(columns={"prediction": "topic"})
    except Exception as e:
        print(f"[Clustering] {e}")
        return None


# ═══════════════════════════  DASHBOARD LAYOUT  ═════════════════════════════

# ── Row 1: two charts ───────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Headlines per Source")
    df_source = safe_query("SELECT source, count FROM by_source ORDER BY count DESC")
    if not df_source.empty:
        st.bar_chart(df_source.set_index("source"))
    else:
        st.info("Waiting for data…")

with col2:
    st.subheader("Volume per Hour")
    df_window = safe_query(
        "SELECT window.start AS hour, count FROM by_window ORDER BY hour"
    )
    if not df_window.empty:
        df_window["hour"] = pd.to_datetime(df_window["hour"])
        st.line_chart(df_window.set_index("hour"))
    else:
        st.info("Waiting for data…")

# ── Row 2: keywords + LLM summary ───────────────────────────────────────────
st.subheader("🔑 Top 10 Keywords")
df_words = safe_query(
    "SELECT word, count FROM top_words ORDER BY count DESC LIMIT 10"
)
if not df_words.empty:
    st.dataframe(df_words, width="stretch", hide_index=True)
else:
    st.info("Waiting for data…")

st.subheader("🤖 AI News Summary")
if not df_words.empty:
    keywords = df_words["word"].tolist()
    summary = get_llm_summary(keywords)
    st.success(summary)
else:
    st.info("Waiting for enough keywords to generate a summary…")

# ── Row 3: Bonus — GDELT + Topic Clustering ────────────────────────────────
st.divider()
st.subheader("🌍 Bonus: GDELT Global Events Slice")

gdelt_col1, gdelt_col2 = st.columns(2)

with gdelt_col1:
    with st.spinner("Fetching latest GDELT data…"):
        gdf = fetch_gdelt_events()
    if not gdf.empty:
        summary_gdf = gdelt_country_summary(gdf)
        if not summary_gdf.empty:
            st.caption("Top countries by event count (last 15 min)")
            st.bar_chart(summary_gdf.set_index("country")["events"])
        else:
            st.info("No country data in GDELT slice.")
    else:
        st.warning("Could not fetch GDELT data.")

with gdelt_col2:
    st.caption("RSS headlines matching GDELT actors / locations")
    if not gdf.empty:
        rss_df = safe_query("SELECT source, title FROM by_source_detail") \
            if False else pd.DataFrame()
        # Read batch headlines for join
        try:
            rss_batch = spark.read.schema(SCHEMA).json("data/incoming") \
                            .select("source", "title").toPandas()
        except Exception:
            rss_batch = pd.DataFrame()
        joined = join_gdelt_rss(gdf, rss_batch)
        if not joined.empty:
            st.dataframe(joined.head(15), width="stretch", hide_index=True)
        else:
            st.info("No overlapping entities found between GDELT and RSS headlines.")
    else:
        st.info("GDELT data unavailable — skipping join.")

# ── Bonus: Topic Clustering (TF-IDF + KMeans) ──────────────────────────────
st.subheader("🧩 Bonus: Topic Clustering (TF-IDF + KMeans)")
clusters_df = run_topic_clustering(n_clusters=3)
if clusters_df is not None:
    for topic_id in sorted(clusters_df["topic"].unique()):
        subset = clusters_df[clusters_df["topic"] == topic_id]
        with st.expander(f"Topic {topic_id}  ({len(subset)} headlines)", expanded=True):
            st.dataframe(
                subset[["source", "title"]].head(10),
                width="stretch", hide_index=True,
            )
else:
    st.info("Not enough headlines yet for clustering — wait for more ingestion ticks.")

# ── Reflection (T5) ─────────────────────────────────────────────────────────
st.divider()
with st.expander("📝 Reflection (T5)"):
    st.markdown(
        """
At 1 000× input volume the **top-words stateful aggregation** would break first:
it maintains an unbounded in-memory hash map of every distinct token ever seen,
so memory would explode. The fix is to switch from a `complete`-mode `groupBy`
to a **windowed aggregation with a watermark** so Spark can evict old state, and
to repartition the stream by word hash across a real cluster, leveraging Spark's
built-in **shuffle partitioning** and **RocksDB state-store backend** for
disk-spilling state management.
"""
    )

# ── Auto-refresh every 10 seconds (Bonus) ───────────────────────────────────
time.sleep(10)
st.rerun()

#!/usr/bin/env python3
"""
News Pulse — Streamlit Dashboard (T4)
Displays live-updating charts and an LLM-generated summary.
Run with:  streamlit run app.py
"""

import time
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

from llm_summary import get_llm_summary

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="News Pulse", page_icon="📰", layout="wide")
st.title("📰 News Pulse — Live Dashboard")
st.caption("Real-time RSS headlines → Spark Structured Streaming → LLM insight")

# ── Connect to the SAME Spark session (memory sinks live in-process) ─────────
spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ── Helper: safe SQL query to Pandas ─────────────────────────────────────────
def safe_query(sql: str) -> pd.DataFrame:
    """Run Spark SQL and return a Pandas DataFrame; empty DF on error."""
    try:
        return spark.sql(sql).toPandas()
    except Exception:
        return pd.DataFrame()


# ── Layout: two columns for charts, full-width for words + summary ───────────
col1, col2 = st.columns(2)

# 1️⃣  Bar chart — headlines per source
with col1:
    st.subheader("Headlines per Source")
    df_source = safe_query("SELECT source, count FROM by_source ORDER BY count DESC")
    if not df_source.empty:
        st.bar_chart(df_source.set_index("source"))
    else:
        st.info("Waiting for data…")

# 2️⃣  Line chart — headlines per 1-hour window
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

# 3️⃣  Top keywords table
st.subheader("🔑 Top 10 Keywords")
df_words = safe_query(
    "SELECT word, count FROM top_words ORDER BY count DESC LIMIT 10"
)
if not df_words.empty:
    st.dataframe(df_words, use_container_width=True, hide_index=True)
else:
    st.info("Waiting for data…")

# 4️⃣  LLM thematic summary
st.subheader("🤖 AI News Summary")
if not df_words.empty:
    keywords = df_words["word"].tolist()
    summary = get_llm_summary(keywords)
    st.success(summary)
else:
    st.info("Waiting for enough keywords to generate a summary…")

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

# ── Auto-refresh every 10 seconds (bonus) ───────────────────────────────────
time.sleep(10)
st.rerun()

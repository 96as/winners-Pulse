#!/usr/bin/env python3
"""
News Pulse — Spark Structured Streaming Job (T2)
Watches data/incoming/ for new JSONL files and maintains three streaming
aggregations in memory sinks that Streamlit can query.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)

# ── Spark session (local mode, single JVM) ───────────────────────────────────
spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")   # keep small for laptop
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
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ── Schema matching the ingester output ──────────────────────────────────────
schema = StructType([
    StructField("source", StringType(), True),
    StructField("title",  StringType(), True),
    StructField("url",    StringType(), True),
    StructField("ts",     TimestampType(), True),
])

# ── Read stream — file source watching the incoming folder ───────────────────
stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 5)
    .json("data/incoming")
)

# ── Query 1: headline count per source ───────────────────────────────────────
q_source = (
    stream
    .groupBy("source")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("by_source")
    .start()
)

# ── Query 2: headline count per 1-hour tumbling window ───────────────────────
q_window = (
    stream
    .withWatermark("ts", "2 hours")
    .groupBy(F.window("ts", "1 hour"))
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("by_window")
    .start()
)

# ── Query 3: top keywords across all headlines ───────────────────────────────
# English stop words + very short tokens are filtered out.
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

stop_array = F.array(*[F.lit(w) for w in STOP_WORDS])

q_words = (
    stream
    .select(
        F.explode(
            F.regexp_extract_all(F.lower(F.col("title")), F.lit(r"([a-z]{3,})"))
        ).alias("word")
    )
    .filter(~F.col("word").isin(STOP_WORDS))
    .filter(F.length("word") >= 3)
    .groupBy("word")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("top_words")
    .start()
)

# ── Keep the JVM alive ───────────────────────────────────────────────────────
print("Spark Structured Streaming is running. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()

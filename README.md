# 📰 News Pulse — Big Data Challenge (SE446)

Real-time RSS news monitoring pipeline using PySpark Structured Streaming,
OpenAI LLM summarisation, and Streamlit.

## Team Members
- Abdulrahman Salameh
- Abdullah Damati

## Prerequisites

```bash
java -version                    # Must show 11 or 17
pip install pyspark==3.5.0 feedparser pandas streamlit requests
export OPENAI_API_KEY=sk-...     # Your OpenAI key
```

## How to Run

Open **three terminals** in the project root:

```bash
# Terminal 1 — Ingester (pulls RSS every 60s)
python ingester.py

# Terminal 2 — Spark Streaming (keep alive the whole time)
python streaming_job.py

# Terminal 3 — Dashboard
streamlit run app.py
```

## Architecture

```
RSS Feeds ──► ingester.py ──► data/incoming/*.json
                                      │
                            Spark readStream (file source)
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                  ▼
              by_source          by_window          top_words
            (memory sink)      (memory sink)      (memory sink)
                    │                 │                  │
                    └─────────────────┼──────────────────┘
                                      │
                              Streamlit (app.py)
                              + OpenAI LLM summary
```

## Reflection (T5)

At 1000× input volume the top-words stateful aggregation would break first:
it maintains an unbounded in-memory hash map of every distinct token ever seen,
so memory would explode. The fix is to switch from a complete-mode groupBy to a
windowed aggregation with a watermark so Spark can evict old state, and to
repartition the stream across a real cluster using Spark's shuffle partitioning
and RocksDB state-store backend for disk-spilling state management.
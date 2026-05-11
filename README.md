# 📰 News Pulse — Big Data Challenge (SE446)

Real-time RSS news monitoring pipeline using PySpark Structured Streaming,
OpenAI LLM summarisation, and Streamlit.

## Team Members
- Abdulrahman Salameh
- Abdullah Damati

## Prerequisites

- **Java 17** (PySpark 3.5 is not compatible with Java 23+)
  ```bash
  brew install openjdk@17
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
  ```
- **Python dependencies**
  ```bash
  pip install -r requirements.txt
  ```
- **OpenAI API key** (optional — LLM summary falls back to keywords without it)
  ```bash
  export OPENAI_API_KEY=sk-...
  ```

## How to Run

Open **two terminals** in the project root:

```bash
# Terminal 1 — Ingester (pulls RSS every 60s)
python ingester.py

# Terminal 2 — Dashboard + Spark Streaming (all-in-one)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
streamlit run app.py
```

> **Note:** Spark Structured Streaming runs inside `app.py` so the memory sinks
> are in the same JVM as the dashboard. No separate `streaming_job.py` process
> is needed (it is kept as a standalone reference).

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
                              + GDELT global events join    (bonus)
                              + TF-IDF / KMeans clustering  (bonus)
                              + Auto-refresh every 10s      (bonus)
```

## Reflection (T5)

At 1000× input volume the top-words stateful aggregation would break first:
it maintains an unbounded in-memory hash map of every distinct token ever seen,
so memory would explode. The fix is to switch from a complete-mode groupBy to a
windowed aggregation with a watermark so Spark can evict old state, and to
repartition the stream across a real cluster using Spark's shuffle partitioning
and RocksDB state-store backend for disk-spilling state management.
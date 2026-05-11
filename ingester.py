#!/usr/bin/env python3
"""
News Pulse — RSS Ingester (T1)
Pulls headlines from 5+ RSS feeds every 60 seconds and writes each batch
as a JSON-lines file into data/incoming/.
"""

import os
import json
import time
from datetime import datetime, timezone

import feedparser

# ── Configuration ────────────────────────────────────────────────────────────
INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

FEEDS = {
    "BBC":        "http://feeds.bbci.co.uk/news/rss.xml",
    "Reuters":    "https://feeds.reuters.com/reuters/topNews",
    "AlJazeera":  "https://www.aljazeera.com/xml/rss/all.xml",
    "CNN":        "http://rss.cnn.com/rss/edition.rss",
    "TechCrunch": "https://techcrunch.com/feed/",
}

POLL_INTERVAL = 60  # seconds between pulls


# ── Core logic ───────────────────────────────────────────────────────────────
def pull_once(tick: int) -> None:
    """Fetch all feeds and write one JSONL batch file."""
    rows = []
    for source, url in FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                # Use the published timestamp if available, otherwise now()
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    ts = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                else:
                    ts = datetime.now(timezone.utc).isoformat()

                rows.append({
                    "source": source,
                    "title":  entry.get("title", ""),
                    "url":    entry.get("link", ""),
                    "ts":     ts,
                })
            print(f"[tick {tick}] {source}: {len(feed.entries)} headlines")
        except Exception as e:
            # Tolerate dead / slow feeds — never crash the ingester
            print(f"[tick {tick}] {source}: FAILED ({e})")

    # Write JSONL file
    path = os.path.join(INCOMING, f"batch_{tick}.json")
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"[tick {tick}] Wrote {len(rows)} records → {path}\n")


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("News Pulse Ingester — press Ctrl+C to stop")
    tick = 0
    while True:
        pull_once(tick)
        tick += 1
        time.sleep(POLL_INTERVAL)

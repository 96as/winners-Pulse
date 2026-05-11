#!/usr/bin/env python3
"""
News Pulse — LLM Summary Helper (T3)
Sends top keywords to OpenAI and returns a one-paragraph thematic summary.
Falls back to a keyword-only summary if the API call fails.
"""

import os
import requests

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o-mini"  # fast + cheap; swap to "gpt-4o" if you prefer
OPENAI_URL = "https://api.openai.com/v1/chat/completions"


def get_llm_summary(keywords: list[str]) -> str:
    """
    Ask OpenAI to summarise the current news pulse from the top keywords.
    Returns a single paragraph (≤80 words, ≥3 named storylines).
    Falls back gracefully on any failure.
    """
    if not keywords:
        return "No keywords available yet — waiting for data."

    keyword_str = ", ".join(keywords[:15])

    # ── Fallback: keyword-only summary ───────────────────────────────────
    fallback = (
        f"Top trending topics right now: {keyword_str}. "
        "Refresh in a few seconds for an AI-generated narrative summary."
    )

    if not OPENAI_API_KEY:
        return fallback + " (Set OPENAI_API_KEY for LLM summaries.)"

    prompt = (
        "You are a concise news analyst. Given these trending keywords from "
        "live RSS feeds, write exactly ONE paragraph of no more than 80 words "
        "summarising the current news pulse. You MUST mention at least three "
        "specific named storylines (people, places, or events). Do NOT use "
        "bullet points. Keywords:\n\n"
        f"{keyword_str}"
    )

    try:
        resp = requests.post(
            OPENAI_URL,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 150,
                "temperature": 0.7,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()

    except Exception as e:
        print(f"[LLM] OpenAI call failed: {e}")
        return fallback

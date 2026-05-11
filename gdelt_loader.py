#!/usr/bin/env python3
"""
News Pulse — GDELT Loader (Bonus)
Downloads the latest 15-minute GDELT v2 events CSV and returns
a Pandas DataFrame for joining with RSS data.
"""

import io
import zipfile
import requests
import pandas as pd

GDELT_LAST_UPDATE = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# GDELT v2 Events: 61 tab-separated columns, no header row.
GDELT_COLS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
    "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat",
    "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]


def fetch_gdelt_events() -> pd.DataFrame:
    """Download the most recent 15-min GDELT v2 events export."""
    try:
        resp = requests.get(GDELT_LAST_UPDATE, timeout=15)
        resp.raise_for_status()
        export_url = None
        for line in resp.text.strip().splitlines():
            parts = line.split()
            if len(parts) >= 3 and "export" in parts[2].lower():
                export_url = parts[2]
                break
        if not export_url:
            return pd.DataFrame()

        zresp = requests.get(export_url, timeout=30)
        zresp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(zresp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(
                    f, sep="\t", header=None, names=GDELT_COLS,
                    dtype=str, on_bad_lines="skip",
                )

        for col in ("AvgTone", "NumArticles", "GoldsteinScale"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    except Exception as exc:
        print(f"[GDELT] fetch failed: {exc}")
        return pd.DataFrame()


def gdelt_country_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Top-15 countries by GDELT event count."""
    if df.empty or "ActionGeo_CountryCode" not in df.columns:
        return pd.DataFrame()
    return (
        df.groupby("ActionGeo_CountryCode")
        .agg(events=("ActionGeo_CountryCode", "size"),
             avg_tone=("AvgTone", "mean"))
        .sort_values("events", ascending=False)
        .head(15)
        .reset_index()
        .rename(columns={"ActionGeo_CountryCode": "country"})
    )


def join_gdelt_rss(gdelt_df: pd.DataFrame, rss_df: pd.DataFrame) -> pd.DataFrame:
    """Join GDELT actors/locations with RSS headlines on shared keywords."""
    if gdelt_df.empty or rss_df.empty:
        return pd.DataFrame()

    names = set()
    for col in ("Actor1Name", "Actor2Name", "ActionGeo_FullName"):
        if col in gdelt_df.columns:
            for v in gdelt_df[col].dropna().unique():
                v = str(v).strip()
                if len(v) > 2:
                    names.add(v.lower())

    rows = []
    for _, r in rss_df.iterrows():
        t = str(r.get("title", "")).lower()
        hits = [n for n in names if n in t]
        if hits:
            rows.append({"headline": r["title"],
                         "source": r.get("source", ""),
                         "gdelt_match": ", ".join(hits[:3])})
    return pd.DataFrame(rows)

"""arXiv daily crawler.

Bronze → raw API entries as JSON
Silver → cleaned, typed Parquet
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import pandas as pd
import requests
import yaml

from ingestion.utils.lake import (
    bronze_partition,
    load_ingested_ids,
    save_ingested_ids,
    write_bronze_json,
    write_silver_parquet,
)
from ingestion.utils.log import get_logger
from ingestion.utils.retry import retry_call

_DIR = Path(__file__).parent


def _load_config() -> dict:
    with open(_DIR / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _build_query(category: str, days_back: int) -> str:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days_back)
    date_range = f"[{start.strftime('%Y%m%d')}0000 TO {now.strftime('%Y%m%d')}2359]"
    return f"cat:{category} AND submittedDate:{date_range}"


def _fetch_entries(cfg: dict, category: str) -> list[dict]:
    api = cfg["api"]
    params = {
        "search_query": _build_query(category, cfg["days_back"]),
        "start": 0,
        "max_results": cfg["max_results_per_category"],
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    resp = retry_call(
        lambda: requests.get(api["base_url"], params=params, timeout=api["timeout"]),
        max_retries=api["max_retries"],
        backoff=api["retry_backoff"],
        request_delay=api["request_delay"],
        label=f"arXiv/{category}",
    )
    resp.raise_for_status()
    feed = feedparser.parse(resp.content)
    return [
        {
            "arxiv_id": e.id.split("/abs/")[-1],
            "title": e.title.replace("\n", " ").strip(),
            "authors": ", ".join(a.name for a in e.get("authors", [])),
            "abstract": e.summary.replace("\n", " ").strip(),
            "categories": ", ".join(t.term for t in e.get("tags", [])),
            "published": e.published,
            "updated": e.updated,
            "pdf_url": next(
                (lk.href for lk in e.get("links", []) if lk.get("type") == "application/pdf"),
                "",
            ),
            "primary_category": category,
        }
        for e in feed.entries
    ]


def _to_silver_df(entries: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(entries)
    if df.empty:
        return df
    for col in ("published", "updated"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    df["crawled_at"] = datetime.now(timezone.utc).isoformat()
    return df


def main() -> dict:
    cfg = _load_config()
    log = get_logger("arxiv")
    log.info("=" * 50)
    log.info("arXiv crawler starting | categories=%s", cfg["categories"])

    dt = datetime.now(timezone.utc)
    seen_ids = load_ingested_ids("arxiv")
    all_entries: list[dict] = []
    errors: list[str] = []

    for category in cfg["categories"]:
        log.info("Crawling %s", category)
        try:
            entries = _fetch_entries(cfg, category)
            new = [e for e in entries if e["arxiv_id"] not in seen_ids]
            log.info("  fetched=%d  new=%d", len(entries), len(new))
            all_entries.extend(new)
            seen_ids.update(e["arxiv_id"] for e in new)
        except Exception as exc:
            msg = f"{category}: {exc}"
            log.error("  Error — %s", msg)
            errors.append(msg)

    # deduplicate across categories (same paper can appear in cs.LG and cs.CV)
    seen: set[str] = set()
    unique = [e for e in all_entries if not (e["arxiv_id"] in seen or seen.add(e["arxiv_id"]))]  # type: ignore[func-returns-value]

    count = len(unique)
    log.info("Total new papers: %d", count)

    if unique:
        write_bronze_json("arxiv", unique, filename="entries.json", dt=dt)
        silver_path = write_silver_parquet("arxiv", _to_silver_df(unique), dt=dt)
        save_ingested_ids("arxiv", [e["arxiv_id"] for e in unique])
        log.info("Silver → %s", silver_path)

    log.info("arXiv done | papers=%d errors=%d", count, len(errors))
    return {
        "source": "arxiv",
        "status": "ok" if not errors else "partial",
        "count": count,
        "errors": errors,
    }

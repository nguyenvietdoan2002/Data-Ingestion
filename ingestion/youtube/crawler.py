"""YouTube daily crawler.

Bronze → media files (video / audio / subs / info.json) + search_results.json
Silver → metadata catalog Parquet (one row per downloaded video)
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yaml
import yt_dlp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ingestion.utils.dotenv import load_dotenv
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
_ISO8601_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


def _load_config() -> dict:
    with open(_DIR / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_duration(s: str) -> int | None:
    if not s:
        return None
    m = _ISO8601_RE.match(s.strip())
    if not m:
        return None
    return int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60 + int(m.group(3) or 0)


def _is_likely_english(snippet: dict) -> bool:
    for field in ("defaultAudioLanguage", "defaultLanguage"):
        lang = snippet.get(field) or ""
        if lang.lower().startswith("en"):
            return True
        if lang and not lang.lower().startswith("en"):
            return False
    title = snippet.get("title", "")
    latin = len(re.findall(r"[A-Za-z]", title))
    non_latin = len(re.findall(r"[^\x00-\x7F\s\d\W]", title))
    return latin >= 8 and latin >= non_latin


def _search_videos(
    youtube,
    cfg: dict,
    skip_ids: set[str],
    log,
) -> list[str]:
    search = cfg["search"]
    api = cfg["api"]
    max_results = search["max_results"]
    now = datetime.now(timezone.utc)
    days_back = search["days_back"]
    start = now - timedelta(days=days_back) if days_back > 0 else now.replace(hour=0, minute=0, second=0, microsecond=0)
    published_after = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    published_before = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    deadline = time.time() + max(1, search["search_timeout_seconds"])
    page_size = min(max(max_results * 4, 20), 50)

    video_urls: list[str] = []
    next_page_token = None
    page = 0

    while len(video_urls) < max_results and time.time() < deadline:
        page += 1
        log.info("[search] page=%d  collected=%d/%d", page, len(video_urls), max_results)

        search_params: dict = {
            "part": "id,snippet",
            "q": search["query"],
            "type": "video",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "relevanceLanguage": search["relevance_language"],
            "maxResults": page_size,
            "order": "date",
        }
        if search.get("video_duration"):
            search_params["videoDuration"] = search["video_duration"]
        if search.get("region_code"):
            search_params["regionCode"] = search["region_code"]

        # capture current token in a default arg to avoid late-binding in the lambda
        token = next_page_token
        resp = retry_call(
            lambda t=token: youtube.search().list(**search_params, pageToken=t).execute(),
            max_retries=api["max_retries"],
            backoff=api["retry_backoff"],
            request_delay=api["request_delay"],
            label="search.list",
        )
        items = resp.get("items", [])
        if not items:
            break

        candidate_ids = [
            item["id"]["videoId"]
            for item in items
            if item["id"]["videoId"] not in skip_ids
            and item["id"]["videoId"] not in {u.split("v=")[-1] for u in video_urls}
        ]
        if candidate_ids:
            ids_str = ",".join(candidate_ids)
            details_resp = retry_call(
                lambda s=ids_str: youtube.videos().list(part="snippet,contentDetails", id=s).execute(),
                max_retries=api["max_retries"],
                backoff=api["retry_backoff"],
                request_delay=api["request_delay"],
                label="videos.list",
            )
            details = {item["id"]: item for item in details_resp.get("items", [])}
        else:
            details = {}

        min_dur = int(search.get("min_duration_seconds", 0))
        max_dur = int(search.get("max_duration_seconds", 86400))

        for video_id in candidate_ids:
            item = details.get(video_id)
            if not item:
                continue
            snippet = item.get("snippet") or {}
            content = item.get("contentDetails") or {}
            title = snippet.get("title", "")

            if search["require_english"] and not _is_likely_english(snippet):
                log.info("Skip (not English): %s", title)
                continue
            dur_s = _parse_duration(content.get("duration", ""))
            if dur_s is None or not (min_dur <= dur_s <= max_dur):
                log.info("Skip (duration %ss): %s", dur_s, title)
                continue

            video_urls.append(f"https://www.youtube.com/watch?v={video_id}")
            log.info("Found: %s", title)
            if len(video_urls) >= max_results:
                break

        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    if time.time() >= deadline:
        log.warning(
            "Search timeout (%ds). Collected %d/%d videos.",
            search["search_timeout_seconds"], len(video_urls), max_results,
        )
    return video_urls


def _download_assets(
    urls: list[str],
    bronze_dir: Path,
    cfg: dict,
    log,
) -> list[dict]:
    dl = cfg["download"]
    subs_cfg = dl.get("subtitles") or {}
    video_dir = bronze_dir / "video"
    audio_dir = bronze_dir / "audio"
    info_dir = bronze_dir / "info"
    subs_dir = bronze_dir / "subs"
    for d in (video_dir, audio_dir, info_dir, subs_dir):
        d.mkdir(parents=True, exist_ok=True)

    records: list[dict] = []

    for url in urls:
        video_id = url.split("v=")[-1]
        rec: dict = {
            "video_id": video_id,
            "url": url,
            "title": "",
            "_info": {},
            "video_path": None,
            "audio_path": None,
            "info_path": None,
        }

        # ── meta + subtitles ──────────────────────────────────────────────────
        try:
            ydl_opts = {
                "skip_download": True,
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "outtmpl": {
                    "infojson": str(info_dir / "%(id)s.%(ext)s"),
                    "subtitle": str(subs_dir / "%(id)s.%(language)s.%(ext)s"),
                },
                "writeinfojson": bool(dl.get("write_info_json", True)),
                "writesubtitles": bool(subs_cfg.get("enabled", True)),
                "writeautomaticsub": bool(subs_cfg.get("write_auto_subs", True)),
                "subtitleslangs": subs_cfg.get("languages") or ["en"],
                "subtitlesformat": subs_cfg.get("formats", "vtt/srt"),
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                rec["title"] = info.get("title", "")
                rec["_info"] = info
                if cfg["search"].get("exclude_shorts", True) and "/shorts/" in (
                    info.get("webpage_url") or ""
                ):
                    log.info("Skip Shorts: %s", rec["title"])
                    continue
                info_path = info_dir / f"{info.get('id')}.info.json"
                if info_path.exists():
                    rec["info_path"] = str(info_path)
        except Exception as exc:
            log.warning("[meta/subs] %s: %s", video_id, exc)

        # ── video ─────────────────────────────────────────────────────────────
        try:
            ydl_opts = {
                "format": dl["video_format"],
                "outtmpl": str(video_dir / "%(id)s.%(ext)s"),
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                fp = video_dir / f"{info['id']}.{info.get('ext', 'mp4')}"
                if fp.exists():
                    rec["video_path"] = str(fp)
        except Exception as exc:
            log.warning("[video] %s: %s", video_id, exc)

        # ── audio ─────────────────────────────────────────────────────────────
        try:
            ydl_opts = {
                "format": dl["audio_format"],
                "outtmpl": str(audio_dir / "%(id)s.%(ext)s"),
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                fp = audio_dir / f"{info['id']}.{info.get('ext', 'm4a')}"
                if fp.exists():
                    rec["audio_path"] = str(fp)
        except Exception as exc:
            log.warning("[audio] %s: %s", video_id, exc)

        if rec["video_path"] or rec["audio_path"] or rec["info_path"]:
            records.append(rec)

    return records


def _to_silver_df(records: list[dict]) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for rec in records:
        info = rec.pop("_info", {}) or {}
        rows.append({
            "video_id": rec["video_id"],
            "title": rec["title"],
            "channel_id": info.get("channel_id", ""),
            "channel_title": info.get("channel", ""),
            "upload_date": info.get("upload_date", ""),
            "duration_s": info.get("duration"),
            "view_count": info.get("view_count"),
            "like_count": info.get("like_count"),
            "language": info.get("language", ""),
            "bronze_video_path": rec.get("video_path"),
            "bronze_audio_path": rec.get("audio_path"),
            "bronze_info_path": rec.get("info_path"),
            "crawled_at": now,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["duration_s"] = pd.to_numeric(df["duration_s"], errors="coerce").astype("Int64")
        df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce").astype("Int64")
        df["like_count"] = pd.to_numeric(df["like_count"], errors="coerce").astype("Int64")
    return df


def main() -> dict:
    load_dotenv(_DIR / ".env")

    cfg = _load_config()
    log = get_logger("youtube")

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        log.error("Missing YouTube API key — set 'YOUTUBE_API_KEY' in ingestion/youtube/.env")
        return {"source": "youtube", "status": "error", "count": 0, "errors": ["Missing API key"]}

    log.info("=" * 50)
    log.info("YouTube crawler starting")

    dt = datetime.now(timezone.utc)
    seen_ids = load_ingested_ids("youtube")
    log.info("Skip list: %d already ingested", len(seen_ids))

    try:
        youtube = build("youtube", "v3", developerKey=api_key)
        urls = _search_videos(youtube, cfg, seen_ids, log)
    except HttpError as exc:
        msg = f"YouTube API error: HTTP {exc.resp.status}"
        log.error(msg)
        return {"source": "youtube", "status": "error", "count": 0, "errors": [msg]}

    bronze_dir = bronze_partition("youtube", dt)
    write_bronze_json("youtube", [{"url": u} for u in urls], filename="search_results.json", dt=dt)

    records = _download_assets(urls, bronze_dir, cfg, log)
    count = len(records)

    if records:
        silver_path = write_silver_parquet("youtube", _to_silver_df(records), dt=dt)
        save_ingested_ids("youtube", [r["video_id"] for r in records])
        log.info("Silver → %s", silver_path)

    log.info("YouTube done | downloaded=%d", count)
    return {"source": "youtube", "status": "ok", "count": count, "errors": []}

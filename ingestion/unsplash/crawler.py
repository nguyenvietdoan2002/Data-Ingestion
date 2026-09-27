"""Unsplash daily crawler.

Bronze → raw search API response JSON + image files
Silver → metadata Parquet (one row per downloaded image)
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import pandas as pd
import requests
import yaml

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


def _load_config() -> dict:
    with open(_DIR / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _api_headers(access_key: str) -> dict:
    return {"Authorization": f"Client-ID {access_key}", "Accept-Version": "v1"}


def _api_get(url: str, cfg: dict, access_key: str, **kwargs) -> requests.Response:
    api = cfg["api"]

    def _call():
        resp = requests.get(
            url,
            headers=_api_headers(access_key),
            timeout=api["download_timeout"],
            **kwargs,
        )
        resp.raise_for_status()
        return resp

    return retry_call(
        _call,
        max_retries=api["max_retries"],
        backoff=api["retry_backoff"],
        request_delay=api["request_delay"],
        label=url,
    )


def _append_client_id(url: str, access_key: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if "client_id" not in query:
        query["client_id"] = [access_key]
    return urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in query.items()})))


def _photo_date(photo: dict):
    created_at = photo.get("created_at")
    if not created_at:
        return None
    return datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone().date()


def _search_photos(
    cfg: dict,
    access_key: str,
    skip_ids: set[str],
    log,
) -> list[dict]:
    search = cfg["search"]
    api = cfg["api"]
    today = datetime.now().astimezone().date()
    photos: list[dict] = []
    page = 1

    log.info("Searching Unsplash: query=%r  today_only=%s", search["query"], search["today_only"])

    while len(photos) < search["max_results"]:
        params: dict = {
            "query": search["query"],
            "page": page,
            "per_page": search["per_page"],
            "order_by": search["order_by"],
        }
        if search.get("orientation"):
            params["orientation"] = search["orientation"]
        if search.get("color"):
            params["color"] = search["color"]

        resp = _api_get(f"{api['base_url']}/search/photos", cfg, access_key, params=params)
        payload = resp.json()
        results = payload.get("results", [])
        if not results:
            break

        all_older = True
        for photo in results:
            photo_id = photo["id"]
            created_date = _photo_date(photo)

            if created_date is None or created_date >= today:
                all_older = False

            if search["today_only"] and created_date != today:
                continue
            if photo_id in skip_ids or photo_id in {p["id"] for p in photos}:
                continue

            photos.append(photo)
            log.info("Found (%s): %s", created_date, photo.get("alt_description") or photo_id)

            if len(photos) >= search["max_results"]:
                break

        if search["today_only"] and all_older:
            log.info("No more photos from today in results.")
            break
        if page >= payload.get("total_pages", page):
            break
        page += 1

    return photos


def _download_image(
    photo: dict,
    save_dir: Path,
    cfg: dict,
    access_key: str,
    log,
) -> str | None:
    api = cfg["api"]
    image_quality = cfg["download"]["image_quality"]
    urls = photo.get("urls", {})
    image_url = urls.get(image_quality) or next(
        (urls[q] for q in ("regular", "full", "small", "thumb") if urls.get(q)), None
    )
    if not image_url:
        return None

    # Required by Unsplash ToS
    try:
        dl_loc = photo.get("links", {}).get("download_location")
        if dl_loc:
            _api_get(_append_client_id(dl_loc, access_key), cfg, access_key)
    except Exception as exc:
        log.warning("download_location trigger failed: %s", exc)

    photo_id = photo["id"]
    for attempt in range(1, api["max_retries"] + 1):
        try:
            resp = requests.get(image_url, timeout=api["download_timeout"], stream=True)
        except requests.RequestException as exc:
            if attempt == api["max_retries"]:
                raise
            log.warning("Download attempt %d failed: %s", attempt, exc)
            continue

        if resp.status_code == 200:
            ct = resp.headers.get("Content-Type", "")
            ext = (
                ".jpg" if ("jpeg" in ct or "jpg" in ct)
                else ".png" if "png" in ct
                else ".webp" if "webp" in ct
                else ".jpg"
            )
            save_path = save_dir / f"{photo_id}{ext}"
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return str(save_path)

        if resp.status_code in {429, 500, 502, 503, 504} and attempt < api["max_retries"]:
            continue
        resp.raise_for_status()

    return None


def _to_silver_df(records: list[dict]) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for r in records:
        photo = r["photo"]
        user = photo.get("user", {})
        rows.append({
            "photo_id": photo["id"],
            "description": photo.get("description"),
            "alt_description": photo.get("alt_description"),
            "width": photo.get("width"),
            "height": photo.get("height"),
            "color": photo.get("color"),
            "created_at": photo.get("created_at"),
            "photographer_name": user.get("name"),
            "photographer_username": user.get("username"),
            "unsplash_url": photo.get("links", {}).get("html"),
            "bronze_file_path": r["file_path"],
            "crawled_at": now,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
        df["width"] = pd.to_numeric(df["width"], errors="coerce").astype("Int64")
        df["height"] = pd.to_numeric(df["height"], errors="coerce").astype("Int64")
    return df


def main() -> dict:
    load_dotenv(_DIR / ".env")

    cfg = _load_config()
    log = get_logger("unsplash")

    access_key = os.getenv("ACCESS_KEY") or os.getenv("UNSPLASH_ACCESS_KEY")
    if not access_key:
        log.error("Missing Unsplash access key — set ACCESS_KEY in ingestion/unsplash/.env")
        return {"source": "unsplash", "status": "error", "count": 0, "errors": ["Missing access key"]}

    log.info("=" * 50)
    log.info("Unsplash crawler starting")

    dt = datetime.now(timezone.utc)
    seen_ids = load_ingested_ids("unsplash")
    log.info("Skip list: %d already ingested", len(seen_ids))

    photos = _search_photos(cfg, access_key, seen_ids, log)
    write_bronze_json("unsplash", photos, filename="search_results.json", dt=dt)

    bronze_dir = bronze_partition("unsplash", dt)
    image_dir = bronze_dir / "images"
    records: list[dict] = []
    errors: list[str] = []

    for photo in photos:
        photo_id = photo["id"]
        try:
            file_path = _download_image(photo, image_dir, cfg, access_key, log)
            if file_path:
                records.append({"photo": photo, "file_path": file_path})
                log.info("Saved: %s", Path(file_path).name)
        except Exception as exc:
            msg = f"{photo_id}: {exc}"
            log.error("Error: %s", msg)
            errors.append(msg)

    count = len(records)
    if records:
        silver_path = write_silver_parquet("unsplash", _to_silver_df(records), dt=dt)
        save_ingested_ids("unsplash", [r["photo"]["id"] for r in records])
        log.info("Silver → %s", silver_path)

    log.info("Unsplash done | downloaded=%d  errors=%d", count, len(errors))
    return {
        "source": "unsplash",
        "status": "ok" if not errors else "partial",
        "count": count,
        "errors": errors,
    }

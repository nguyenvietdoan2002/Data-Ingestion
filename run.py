#!/usr/bin/env python3
"""
Unified ingestion entrypoint.

Usage:
  python run.py --job all
  python run.py --job arxiv
  python run.py --job yt --job unsplash
"""
from __future__ import annotations

import argparse
import importlib
import json
from datetime import datetime, timezone
from pathlib import Path

_JOBS: dict[str, str] = {
    "arxiv":   "ingestion.arxiv.crawler",
    "yt":      "ingestion.youtube.crawler",
    "unsplash": "ingestion.unsplash.crawler",
}

_SUMMARY_DIR = Path(__file__).parent / "data-lake" / "gold" / "run_summary"


def _run(name: str) -> dict:
    mod = importlib.import_module(_JOBS[name])
    return mod.main()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AIO data ingestion")
    parser.add_argument(
        "--job", action="append", default=[],
        help="arxiv | yt | unsplash | all  (repeatable)",
    )
    args = parser.parse_args(argv)

    jobs = [j.strip().lower() for j in args.job if j.strip()]
    if not jobs or "all" in jobs:
        jobs = list(_JOBS)

    started_at = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []
    rc = 0

    for job in jobs:
        if job not in _JOBS:
            print(f"[run] Unknown job: {job!r} — skipping")
            continue
        print(f"\n[run] ── {job} ──────────────────────────────")
        try:
            result = _run(job)
        except Exception as exc:
            result = {"source": job, "status": "error", "count": 0, "errors": [str(exc)]}
        results.append(result)
        status = result.get("status", "error")
        count = result.get("count", 0)
        print(f"[run] {job}: status={status}  count={count}")
        if status == "error" and rc == 0:
            rc = 1

    run = {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "jobs": results,
        "total_ingested": sum(r.get("count", 0) for r in results),
    }
    _SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = _SUMMARY_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.json"
    if summary_path.exists():
        existing = json.loads(summary_path.read_text(encoding="utf-8"))
        existing["runs"].append(run)
        daily = existing
    else:
        daily = {"date": datetime.now().strftime("%Y-%m-%d"), "runs": [run]}
    summary_path.write_text(json.dumps(daily, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[run] Summary → {summary_path}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())

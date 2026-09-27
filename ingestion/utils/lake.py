"""
Data lake I/O — Bronze / Silver / _state.

Layout on disk:
  data-lake/
  ├── bronze/<source>/year=YYYY/month=MM/day=DD/   raw, immutable
  ├── silver/<source>/year=YYYY/month=MM/day=DD/   cleaned Parquet
  ├── gold/                                         aggregations
  └── _state/<source>_ids.parquet                  ingestion checkpoint
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

LAKE_ROOT = Path(__file__).resolve().parents[2] / "data-lake"


# ── internal helpers ──────────────────────────────────────────────────────────

def _partition(zone: str, source: str, dt: datetime) -> Path:
    return (
        LAKE_ROOT / zone / source
        / f"year={dt.year}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
    )


def _atomic_write(path: Path, write_fn) -> None:
    """Write to a .tmp file then atomically replace the target."""
    tmp = path.with_name(path.name + ".tmp")
    write_fn(tmp)
    tmp.replace(path)


def _mark_success(partition: Path) -> None:
    (partition / "_SUCCESS").touch()


# ── Bronze ────────────────────────────────────────────────────────────────────

def bronze_partition(source: str, dt: datetime | None = None) -> Path:
    """Return (and create) the Bronze partition directory for today."""
    dt = dt or datetime.now(timezone.utc)
    p = _partition("bronze", source, dt)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_bronze_json(
    source: str,
    data: list[Any] | dict[str, Any],
    *,
    filename: str = "raw.json",
    dt: datetime | None = None,
) -> Path:
    dt = dt or datetime.now(timezone.utc)
    p = bronze_partition(source, dt)
    out = p / filename
    _atomic_write(
        out,
        lambda tmp: tmp.write_text(
            json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8"
        ),
    )
    return out


# ── Silver ────────────────────────────────────────────────────────────────────

def write_silver_parquet(
    source: str,
    df: pd.DataFrame,
    dt: datetime | None = None,
) -> Path:
    dt = dt or datetime.now(timezone.utc)
    p = _partition("silver", source, dt)
    p.mkdir(parents=True, exist_ok=True)
    out = p / "data.parquet"
    _atomic_write(out, lambda tmp: df.to_parquet(tmp, index=False, engine="pyarrow"))
    _mark_success(p)
    return out


# ── State (ingestion checkpoint) ──────────────────────────────────────────────

def _state_path(source: str) -> Path:
    return LAKE_ROOT / "_state" / f"{source}_ids.parquet"


def load_ingested_ids(source: str) -> set[str]:
    """Return the set of item IDs already ingested for this source."""
    path = _state_path(source)
    if not path.exists():
        return set()
    return set(pd.read_parquet(path, columns=["item_id"])["item_id"].tolist())


def save_ingested_ids(
    source: str,
    new_ids: list[str],
    status: str = "ok",
) -> None:
    """Append new IDs to the source checkpoint file (atomic write)."""
    if not new_ids:
        return
    path = _state_path(source)
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    new_df = pd.DataFrame({"item_id": new_ids, "ingested_at": now, "status": status})
    if path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
            "item_id", keep="first"
        )
    else:
        df = new_df
    _atomic_write(path, lambda tmp: df.to_parquet(tmp, index=False, engine="pyarrow"))

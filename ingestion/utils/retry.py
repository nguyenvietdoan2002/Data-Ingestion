"""Generic HTTP retry helper — works with both googleapiclient and requests exceptions."""
from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")

_RETRYABLE = {429, 500, 502, 503, 504}


def _http_status(exc: Exception) -> int | None:
    # googleapiclient.errors.HttpError  →  exc.resp.status
    resp = getattr(exc, "resp", None)
    if resp is not None:
        return getattr(resp, "status", None)
    # requests.HTTPError  →  exc.response.status_code
    response = getattr(exc, "response", None)
    if response is not None:
        return getattr(response, "status_code", None)
    return None


def _retry_after(exc: Exception) -> float | None:
    resp = getattr(exc, "resp", None)
    if resp is not None:
        val = resp.get("retry-after")
        return float(val) if val else None
    response = getattr(exc, "response", None)
    if response is not None:
        val = response.headers.get("Retry-After")
        return float(val) if val else None
    return None


def retry_call(
    fn: Callable[[], T],
    *,
    max_retries: int = 6,
    backoff: float = 15.0,
    request_delay: float = 0.0,
    label: str = "",
) -> T:
    for attempt in range(1, max_retries + 1):
        if request_delay > 0:
            time.sleep(request_delay)
        try:
            return fn()
        except Exception as exc:
            status = _http_status(exc)
            if status not in _RETRYABLE or attempt == max_retries:
                raise
            wait = _retry_after(exc) or (
                max(60.0, backoff * attempt) if status == 429 else backoff * attempt
            )
            print(
                f"  {label}: HTTP {status} "
                f"(attempt {attempt}/{max_retries}), retry in {wait:.0f}s"
            )
            time.sleep(wait)
    raise RuntimeError(f"{label}: max retries exceeded")

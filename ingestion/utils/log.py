import logging
import sys
from datetime import datetime
from pathlib import Path

_LOGS_ROOT = Path(__file__).resolve().parents[2] / "logs"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    _LOGS_ROOT.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    fh = logging.FileHandler(
        _LOGS_ROOT / f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

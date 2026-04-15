"""
utils.py — Shared utilities for the MT5 bridge.

Provides logging setup, timezone helpers, and filesystem utilities
used across all bridge modules.
"""

import logging
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo


def ensure_parent_dir(file_path: str) -> None:
    """Create parent directories for a file path if they don't exist."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)


def init_logger(log_path: str) -> logging.Logger:
    """Initialize a file logger for the bridge.

    Creates parent directories if needed. Only adds a handler once
    to avoid duplicate log lines on repeated calls.

    Args:
        log_path: File path for the log output.

    Returns:
        Configured Logger instance.
    """
    ensure_parent_dir(log_path)

    logger = logging.getLogger("mt5_bridge")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers on re-init
    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Also log to console for dev convenience
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def now_iso(tz_name: str) -> str:
    """Get current time as ISO-8601 string in the specified timezone.

    Args:
        tz_name: IANA timezone name (e.g. 'Asia/Ho_Chi_Minh').

    Returns:
        ISO-8601 datetime string with timezone offset.
    """
    return datetime.now(ZoneInfo(tz_name)).isoformat()


def sleep_ms(ms: int) -> None:
    """Sleep for specified milliseconds."""
    time.sleep(ms / 1000.0)

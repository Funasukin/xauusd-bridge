"""
writer.py — Atomic JSON file writer.

Implements safe file writing pattern:
  1. Write to temp file (.tmp)
  2. Flush and fsync
  3. Atomic rename to target path

This prevents downstream consumers from reading partial/corrupt files.
"""

import json
import os
from pathlib import Path


def write_json_atomic(path: str, payload: dict) -> None:
    """Write payload as JSON using atomic write pattern.

    Writes to a temporary file first, then renames to the target path.
    This ensures the target file is never in a partially-written state
    even if the process crashes mid-write.

    Args:
        path: Target file path for the JSON output.
        payload: Dict to serialize as JSON.

    Raises:
        OSError: If file operations fail.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    temp_path = str(p) + ".tmp"

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    # Atomic replace: on Windows this is not truly atomic but
    # os.replace is the closest available primitive
    os.replace(temp_path, path)


def write_error_payload(path: str, payload: dict) -> None:
    """Write error payload to file.

    Unlike write_json_atomic, this does not use the temp-rename pattern
    since error files are informational and not consumed by real-time
    analysis pipelines.

    Args:
        path: Target file path for the error JSON.
        payload: Error payload dict to serialize.

    Raises:
        OSError: If file operations fail.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

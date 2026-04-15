"""
config.py — Load and validate bridge configuration.

Reads JSON config file and ensures all required fields are present
with valid values before the bridge run starts.
"""

import json
from pathlib import Path


REQUIRED_FIELDS = [
    "broker",
    "symbol",
    "timeframe",
    "bars",
    "timezone",
    "outputPath",
    "errorOutputPath",
    "logPath",
    "schemaPath",
]


def load_config(path: str) -> dict:
    """Load config from JSON file and validate required fields.

    Args:
        path: Path to the JSON config file.

    Returns:
        Validated config dictionary.

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If required fields are missing or invalid.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with p.open("r", encoding="utf-8") as f:
        config = json.load(f)

    validate_config(config)
    return config


def validate_config(config: dict) -> None:
    """Validate config dict against required fields and business constraints.

    Raises:
        ValueError: If any validation rule fails.
    """
    missing = [k for k in REQUIRED_FIELDS if k not in config]
    if missing:
        raise ValueError(f"Missing config fields: {missing}")

    # v1 only supports XAUUSD
    if config["symbol"] != "XAUUSD":
        raise ValueError("v1 only supports symbol=XAUUSD")

    # v1 only supports M15
    if config["timeframe"] != "M15":
        raise ValueError("v1 only supports timeframe=M15")

    # Need at least 100 bars for meaningful analysis
    if int(config["bars"]) < 100:
        raise ValueError("bars must be >= 100")

    # retryCount sanity check
    retry_count = int(config.get("retryCount", 0))
    if retry_count < 0:
        raise ValueError("retryCount must be >= 0")

    # retryDelayMs sanity check
    retry_delay = int(config.get("retryDelayMs", 1000))
    if retry_delay < 0:
        raise ValueError("retryDelayMs must be >= 0")

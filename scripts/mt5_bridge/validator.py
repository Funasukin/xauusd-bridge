"""
validator.py — Schema and business rule validation.

Two-layer validation:
  1. JSON Schema validation against the Data Contract v1 schema
  2. Business rule validation for data integrity and consistency
"""

import json
from pathlib import Path
from datetime import datetime

from jsonschema import validate, ValidationError


def load_schema(schema_path: str) -> dict:
    """Load JSON Schema from file.

    Args:
        schema_path: Path to the JSON schema file.

    Returns:
        Parsed JSON schema dict.

    Raises:
        FileNotFoundError: If schema file does not exist.
    """
    p = Path(schema_path)
    if not p.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_schema(payload: dict, schema: dict) -> list[str]:
    """Validate payload against JSON schema.

    Args:
        payload: The payload dict to validate.
        schema: The JSON schema dict.

    Returns:
        List of error messages. Empty list means valid.
    """
    errors = []
    try:
        validate(instance=payload, schema=schema)
    except ValidationError as e:
        errors.append(f"schema_error: {e.message}")
    return errors


# Known gap durations that are normal in forex markets
# Weekend: ~49h (Fri 23:45 → Mon 00:00 UTC)
# Daily session break: varies by broker, typically 1-5 hours
_MAX_NORMAL_GAP_SECONDS = 72 * 3600  # 72 hours covers long weekends/holidays


def validate_business_rules(payload: dict) -> list[str]:
    """Validate payload against business rules from Data Contract v1.

    Checks:
      - currentPrice consistency (ask >= bid, mid, spread)
      - sessionStats consistency (dayHigh >= dayLow)
      - Candle OHLC validity
      - Candle time sequence (ascending, no duplicates/backwards)
      - Closed bar requirement
      - Minimum candle count

    Args:
        payload: The payload dict to validate.

    Returns:
        List of error messages. Empty list means valid.
    """
    errors = []

    # ── Current Price checks ──
    cp = payload["market"]["currentPrice"]
    bid = cp["bid"]
    ask = cp["ask"]
    mid = cp["mid"]
    spread = cp["spread"]

    if ask < bid:
        errors.append("ask must be >= bid")

    expected_mid = round((bid + ask) / 2.0, 5)
    if abs(expected_mid - mid) > 0.05:
        errors.append(f"mid is inconsistent with bid/ask: expected ~{expected_mid}, got {mid}")

    expected_spread = round(ask - bid, 5)
    if abs(expected_spread - spread) > 0.05:
        errors.append(f"spread is inconsistent with ask-bid: expected ~{expected_spread}, got {spread}")

    if spread < 0:
        errors.append("spread must be >= 0")

    # ── Session Stats checks ──
    ss = payload["market"]["sessionStats"]
    if ss["dayHigh"] < ss["dayLow"]:
        errors.append("dayHigh must be >= dayLow")

    if "prevDayHigh" in ss and "prevDayLow" in ss:
        if ss["prevDayHigh"] < ss["prevDayLow"]:
            errors.append("prevDayHigh must be >= prevDayLow")

    # ── Candles checks ──
    candles = payload["candles"]

    if len(candles) < 100:
        errors.append(f"candles must have at least 100 bars, got {len(candles)}")

    prev_dt = None
    for i, c in enumerate(candles):
        o = c["open"]
        h = c["high"]
        l = c["low"]
        cl = c["close"]

        # OHLC validity: high must be the highest, low must be the lowest
        if not (h >= o and h >= cl and h >= l):
            errors.append(f"invalid high in candle #{i} at {c['time']}: high={h} but open={o} close={cl} low={l}")

        if not (l <= o and l <= cl and l <= h):
            errors.append(f"invalid low in candle #{i} at {c['time']}: low={l} but open={o} close={cl} high={h}")

        # Time sequence check — must be ascending and non-negative
        # Note: gaps > 900s are normal (weekends, session breaks)
        dt = datetime.fromisoformat(c["time"])
        if prev_dt is not None:
            delta = int((dt - prev_dt).total_seconds())

            if delta <= 0:
                # Backwards or duplicate timestamps are always an error
                errors.append(
                    f"candle time not ascending: {delta}s between "
                    f"candle #{i-1} and #{i} at {c['time']}"
                )
            elif delta % 900 != 0:
                # Interval should be a multiple of 15 min
                errors.append(
                    f"candle interval not a multiple of 900s: got {delta}s between "
                    f"candle #{i-1} and #{i} at {c['time']}"
                )
            elif delta > _MAX_NORMAL_GAP_SECONDS:
                # Extremely large gap — likely a data issue
                errors.append(
                    f"abnormal candle gap: {delta}s ({delta/3600:.1f}h) between "
                    f"candle #{i-1} and #{i} at {c['time']}"
                )

        prev_dt = dt

        # Closed bar check for v1
        if c["isClosed"] is not True:
            errors.append(f"candle must be closed in v1: #{i} at {c['time']}")

    return errors


def validate_staleness(payload: dict, max_age_seconds: int = 120) -> list[str]:
    """Validate that the payload is not stale.

    Compares generatedAt with the latest candle CLOSE time.
    Note: serverTime in the payload is the candle OPEN time, so we add
    M15_SECONDS (900s) to get the actual close time before comparing.

    During off-market hours (weekends, holidays), large gaps are expected
    and not flagged as errors.

    Args:
        payload: The payload dict.
        max_age_seconds: Maximum allowed difference in seconds
                         between generatedAt and the last candle close time.

    Returns:
        List of error messages. Empty list means valid.
    """
    errors = []
    M15_CLOSE_OFFSET = 900  # serverTime is candle open, add 15min for close

    try:
        generated_at = datetime.fromisoformat(payload["generatedAt"])
        server_time = datetime.fromisoformat(payload["serverTime"])

        # serverTime = last candle OPEN time
        # Actual close time = serverTime + 15 minutes
        from datetime import timedelta
        last_candle_close = server_time + timedelta(seconds=M15_CLOSE_OFFSET)

        diff = abs((generated_at - last_candle_close).total_seconds())

        # During off-market hours (weekends/holidays), the gap between
        # generatedAt and last candle close is legitimately large.
        # Only flag staleness if within a reasonable intraday window.
        off_market_threshold = 2 * 3600  # 2 hours

        if diff > max_age_seconds and diff < off_market_threshold:
            errors.append(
                f"payload may be stale: generatedAt is {diff:.0f}s after "
                f"last candle close (max allowed: {max_age_seconds}s)"
            )
        # If diff >= off_market_threshold, assume market is closed — not an error

    except (KeyError, ValueError) as e:
        errors.append(f"staleness check failed: {e}")

    return errors

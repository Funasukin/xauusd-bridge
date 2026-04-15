"""
transformer.py — Transform raw MT5 data into payload format.

Converts raw tick, rate, and terminal data from MT5 into the
standardized JSON payload structure defined by Data Contract v1.

Key improvements over skeleton:
  - Proper closed bar detection by comparing bar time with current server time
  - Session stats filtered to current day's candles only
  - Correct timezone conversion for all timestamps
"""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# M15 candle duration in seconds
M15_SECONDS = 15 * 60


def to_iso(ts: int, tz_name: str) -> str:
    """Convert Unix timestamp to ISO-8601 string in the given timezone.

    Args:
        ts: Unix timestamp (seconds since epoch, UTC).
        tz_name: IANA timezone name.

    Returns:
        ISO-8601 formatted datetime string with timezone offset.
    """
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .astimezone(ZoneInfo(tz_name))
        .isoformat()
    )


def build_current_price(tick) -> dict:
    """Build currentPrice block from MT5 tick data.

    Args:
        tick: MT5 tick object (has .bid, .ask attributes).

    Returns:
        Dict with bid, ask, mid, spread.
    """
    bid = float(tick.bid)
    ask = float(tick.ask)
    mid = (bid + ask) / 2.0
    spread = ask - bid

    return {
        "bid": bid,
        "ask": ask,
        "mid": round(mid, 5),
        "spread": round(spread, 5),
    }


def _is_bar_closed(bar_time_utc: int, now_utc: datetime) -> bool:
    """Determine if a candle bar has closed.

    A M15 bar is considered closed if the current time is past
    the bar's open time + 15 minutes.

    Args:
        bar_time_utc: Bar open time as Unix timestamp (UTC).
        now_utc: Current time in UTC.

    Returns:
        True if the bar has fully closed.
    """
    bar_close_time = datetime.fromtimestamp(bar_time_utc, tz=timezone.utc) + timedelta(seconds=M15_SECONDS)
    return now_utc >= bar_close_time


def build_candles(rates, tz_name: str, closed_only: bool = True) -> list[dict]:
    """Build candles array from MT5 rate data.

    Converts raw numpy structured array from MT5 into list of candle dicts.
    When closed_only is True, the last bar is dropped if it hasn't closed yet.

    Args:
        rates: Numpy structured array from mt5.copy_rates_from_pos.
        tz_name: IANA timezone for time display.
        closed_only: If True, exclude unclosed (forming) bars.

    Returns:
        List of candle dicts sorted ascending by time.
    """
    now_utc = datetime.now(timezone.utc)
    candles = []

    for r in rates:
        bar_time = int(r["time"])
        is_closed = _is_bar_closed(bar_time, now_utc)

        # Skip unclosed bars when closed_only mode is enabled
        if closed_only and not is_closed:
            continue

        candle = {
            "time": to_iso(bar_time, tz_name),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "tickVolume": int(r["tick_volume"]),
            "isClosed": is_closed,
        }

        # spread may or may not be present in the rate data
        if "spread" in r.dtype.names:
            candle["spread"] = float(r["spread"])

        candles.append(candle)

    return candles


def build_session_stats(m15_rates, d1_rates, tz_name: str) -> dict:
    """Build sessionStats block from rate data.

    Filters M15 rates to only include candles from the current trading day
    (based on the configured timezone), then computes day open/high/low.

    Args:
        m15_rates: Numpy structured array of M15 rates.
        d1_rates: Numpy structured array of D1 rates (for prev day stats).
        tz_name: IANA timezone name for day boundary calculation.

    Returns:
        Dict with dayOpen, dayHigh, dayLow and optionally prevDay stats.

    Raises:
        ValueError: If no M15 rates are available for today.
    """
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    today_date = now_local.date()

    # Filter M15 rates that belong to today (by local timezone)
    today_rates = []
    for r in m15_rates:
        bar_dt = datetime.fromtimestamp(int(r["time"]), tz=timezone.utc).astimezone(tz)
        if bar_dt.date() == today_date:
            today_rates.append(r)

    # If no rates for today, fall back to all rates for the most recent day
    if not today_rates:
        # Find the most recent date in the data and use those rates
        if len(m15_rates) > 0:
            latest_ts = int(m15_rates[-1]["time"])
            latest_date = datetime.fromtimestamp(latest_ts, tz=timezone.utc).astimezone(tz).date()
            for r in m15_rates:
                bar_dt = datetime.fromtimestamp(int(r["time"]), tz=timezone.utc).astimezone(tz)
                if bar_dt.date() == latest_date:
                    today_rates.append(r)

    if not today_rates:
        raise ValueError("No M15 rates available for session stats")

    day_open = float(today_rates[0]["open"])
    day_high = max(float(r["high"]) for r in today_rates)
    day_low = min(float(r["low"]) for r in today_rates)

    session = {
        "dayOpen": day_open,
        "dayHigh": day_high,
        "dayLow": day_low,
    }

    # Add previous day stats from D1 data
    # d1_rates[0] = current day (forming), d1_rates[1] = yesterday (closed)
    if hasattr(d1_rates, "__len__") and len(d1_rates) >= 2:
        # Index -2 is the previous completed day
        prev = d1_rates[-2]
        session["prevDayHigh"] = float(prev["high"])
        session["prevDayLow"] = float(prev["low"])
        session["prevDayClose"] = float(prev["close"])

    return session


def build_payload(
    config: dict,
    generated_at: str,
    server_time: str,
    terminal_info: dict,
    current_price: dict,
    session_stats: dict,
    candles: list[dict],
) -> dict:
    """Assemble the full payload conforming to Data Contract v1.

    Args:
        config: Bridge configuration dict.
        generated_at: ISO-8601 timestamp of when this run started.
        server_time: ISO-8601 timestamp of the latest candle.
        terminal_info: Terminal metadata dict.
        current_price: Current price dict (bid/ask/mid/spread).
        session_stats: Session stats dict (dayOpen/High/Low + prevDay).
        candles: List of candle dicts.

    Returns:
        Complete payload dict ready for validation and writing.
    """
    # Determine data status based on returned bars
    requested = int(config["bars"])
    returned = len(candles)

    if returned >= requested:
        data_status = "ok"
    elif returned >= 100:
        data_status = "partial"
    else:
        data_status = "error"

    return {
        "schemaVersion": "1.0.0",
        "source": "mt5",
        "broker": config["broker"],
        "symbol": config["symbol"],
        "timeframe": config["timeframe"],
        "generatedAt": generated_at,
        "serverTime": server_time,
        "timezone": config["timezone"],
        "terminal": terminal_info,
        "market": {
            "currentPrice": current_price,
            "sessionStats": session_stats,
        },
        "candles": candles,
        "meta": {
            "requestedBars": requested,
            "returnedBars": returned,
            "dataStatus": data_status,
        },
    }

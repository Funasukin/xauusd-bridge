"""Tests for transformer.py — data transformation logic."""

import numpy as np
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from mt5_bridge.transformer import (
    to_iso,
    build_current_price,
    build_candles,
    build_session_stats,
    build_payload,
)


# ── Helpers ──

def _make_rate(time_utc: int, o=100.0, h=110.0, l=90.0, c=105.0, tv=1000, sp=20):
    """Create a numpy structured array row mimicking MT5 rate data."""
    dtype = np.dtype([
        ("time", "i8"),
        ("open", "f8"),
        ("high", "f8"),
        ("low", "f8"),
        ("close", "f8"),
        ("tick_volume", "i8"),
        ("spread", "i4"),
        ("real_volume", "i8"),
    ])
    return np.array([(time_utc, o, h, l, c, tv, sp, 0)], dtype=dtype)[0]


def _make_rates(base_time_utc: int, count: int = 5, interval: int = 900):
    """Create an array of consecutive M15 rates."""
    dtype = np.dtype([
        ("time", "i8"),
        ("open", "f8"),
        ("high", "f8"),
        ("low", "f8"),
        ("close", "f8"),
        ("tick_volume", "i8"),
        ("spread", "i4"),
        ("real_volume", "i8"),
    ])
    rates = []
    for i in range(count):
        t = base_time_utc + i * interval
        rates.append((t, 100 + i, 110 + i, 90 + i, 105 + i, 1000 + i, 20, 0))
    return np.array(rates, dtype=dtype)


def _make_tick(bid=4772.10, ask=4772.45):
    """Create a mock tick object."""
    tick = MagicMock()
    tick.bid = bid
    tick.ask = ask
    return tick


# ── Tests: to_iso ──

class TestToIso:
    def test_converts_utc_timestamp(self):
        # 2026-01-01 00:00:00 UTC = 1767225600
        result = to_iso(1767225600, "UTC")
        assert "2026-01-01" in result

    def test_timezone_conversion(self):
        # UTC midnight should be +7 hours in Vietnam
        result = to_iso(1767225600, "Asia/Ho_Chi_Minh")
        assert "+07:00" in result
        assert "07:00:00" in result  # 00:00 UTC = 07:00 ICT


# ── Tests: build_current_price ──

class TestBuildCurrentPrice:
    def test_normal_price(self):
        tick = _make_tick(bid=4772.10, ask=4772.45)
        result = build_current_price(tick)

        assert result["bid"] == 4772.10
        assert result["ask"] == 4772.45
        assert result["mid"] == round((4772.10 + 4772.45) / 2, 5)
        assert result["spread"] == round(4772.45 - 4772.10, 5)

    def test_zero_spread(self):
        tick = _make_tick(bid=100.0, ask=100.0)
        result = build_current_price(tick)
        assert result["spread"] == 0.0
        assert result["mid"] == 100.0


# ── Tests: build_candles ──

class TestBuildCandles:
    def test_all_closed_bars(self):
        # Create rates from 2 hours ago (all should be closed)
        now_utc = datetime.now(timezone.utc)
        base = int((now_utc - timedelta(hours=2)).timestamp())
        rates = _make_rates(base, count=5)

        candles = build_candles(rates, "Asia/Ho_Chi_Minh", closed_only=True)
        assert len(candles) == 5
        assert all(c["isClosed"] for c in candles)

    def test_excludes_unclosed_bar(self):
        # Last bar is in the future (not closed)
        now_utc = datetime.now(timezone.utc)
        base = int((now_utc - timedelta(hours=1)).timestamp())
        rates = _make_rates(base, count=5)
        # Override last rate to be in the future
        future_time = int((now_utc + timedelta(minutes=5)).timestamp())
        rates[-1]["time"] = future_time

        candles = build_candles(rates, "Asia/Ho_Chi_Minh", closed_only=True)
        assert len(candles) == 4  # The future bar should be excluded

    def test_closed_only_false_includes_all(self):
        now_utc = datetime.now(timezone.utc)
        base = int((now_utc - timedelta(hours=1)).timestamp())
        rates = _make_rates(base, count=5)
        future_time = int((now_utc + timedelta(minutes=5)).timestamp())
        rates[-1]["time"] = future_time

        candles = build_candles(rates, "Asia/Ho_Chi_Minh", closed_only=False)
        assert len(candles) == 5  # All bars included

    def test_candle_fields(self):
        now_utc = datetime.now(timezone.utc)
        base = int((now_utc - timedelta(hours=2)).timestamp())
        rates = _make_rates(base, count=1)

        candles = build_candles(rates, "Asia/Ho_Chi_Minh")
        c = candles[0]
        assert "time" in c
        assert "open" in c
        assert "high" in c
        assert "low" in c
        assert "close" in c
        assert "tickVolume" in c
        assert "isClosed" in c
        assert "spread" in c


# ── Tests: build_session_stats ──

class TestBuildSessionStats:
    def test_today_rates_only(self):
        tz = ZoneInfo("Asia/Ho_Chi_Minh")
        now_local = datetime.now(tz)

        # Create rates for today
        today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        base_utc = int(today_start.astimezone(timezone.utc).timestamp())

        m15_rates = _make_rates(base_utc, count=10)
        d1_rates = _make_rates(base_utc - 86400, count=3, interval=86400)

        result = build_session_stats(m15_rates, d1_rates, "Asia/Ho_Chi_Minh")

        assert "dayOpen" in result
        assert "dayHigh" in result
        assert "dayLow" in result
        assert result["dayHigh"] >= result["dayLow"]

    def test_prev_day_stats_from_d1(self):
        tz = ZoneInfo("Asia/Ho_Chi_Minh")
        now_local = datetime.now(tz)

        today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        base_utc = int(today_start.astimezone(timezone.utc).timestamp())

        m15_rates = _make_rates(base_utc, count=10)
        # D1: 3 days, d1_rates[-2] = yesterday
        d1_rates = _make_rates(base_utc - 2 * 86400, count=3, interval=86400)

        result = build_session_stats(m15_rates, d1_rates, "Asia/Ho_Chi_Minh")

        assert "prevDayHigh" in result
        assert "prevDayLow" in result
        assert "prevDayClose" in result

    def test_no_d1_rates(self):
        tz = ZoneInfo("Asia/Ho_Chi_Minh")
        now_local = datetime.now(tz)

        today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        base_utc = int(today_start.astimezone(timezone.utc).timestamp())

        m15_rates = _make_rates(base_utc, count=10)

        result = build_session_stats(m15_rates, [], "Asia/Ho_Chi_Minh")

        assert "dayOpen" in result
        assert "prevDayHigh" not in result


# ── Tests: build_payload ──

class TestBuildPayload:
    def test_payload_structure(self):
        config = {
            "broker": "Exness",
            "symbol": "XAUUSD",
            "timeframe": "M15",
            "bars": 200,
            "timezone": "Asia/Ho_Chi_Minh",
        }
        candles = [{"time": "t", "isClosed": True}] * 200

        result = build_payload(
            config=config,
            generated_at="2026-04-15T01:00:00+07:00",
            server_time="2026-04-15T00:45:00+07:00",
            terminal_info={"platform": "MetaTrader5"},
            current_price={"bid": 100, "ask": 101, "mid": 100.5, "spread": 1},
            session_stats={"dayOpen": 99, "dayHigh": 105, "dayLow": 95},
            candles=candles,
        )

        assert result["schemaVersion"] == "1.0.0"
        assert result["source"] == "mt5"
        assert result["meta"]["returnedBars"] == 200
        assert result["meta"]["dataStatus"] == "ok"

    def test_partial_status_when_fewer_bars(self):
        config = {"broker": "Exness", "symbol": "XAUUSD", "timeframe": "M15",
                  "bars": 200, "timezone": "Asia/Ho_Chi_Minh"}
        candles = [{}] * 150

        result = build_payload(
            config=config, generated_at="t", server_time="t",
            terminal_info={}, current_price={}, session_stats={},
            candles=candles,
        )
        assert result["meta"]["dataStatus"] == "partial"

    def test_error_status_when_too_few_bars(self):
        config = {"broker": "Exness", "symbol": "XAUUSD", "timeframe": "M15",
                  "bars": 200, "timezone": "Asia/Ho_Chi_Minh"}
        candles = [{}] * 50

        result = build_payload(
            config=config, generated_at="t", server_time="t",
            terminal_info={}, current_price={}, session_stats={},
            candles=candles,
        )
        assert result["meta"]["dataStatus"] == "error"

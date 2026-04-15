"""Tests for validator.py — schema and business rule validation."""

import json
import os
import tempfile
import pytest

from mt5_bridge.validator import (
    load_schema,
    validate_schema,
    validate_business_rules,
    validate_staleness,
)


# ── Helpers ──

def _valid_payload() -> dict:
    """Create a minimal valid payload for testing."""
    candles = []
    for i in range(150):
        h = i  # hours offset
        candles.append({
            "time": f"2026-04-14T{(h % 24):02d}:{(i * 15 % 60):02d}:00+07:00",
            "open": 4770.0 + i,
            "high": 4780.0 + i,
            "low": 4760.0 + i,
            "close": 4775.0 + i,
            "tickVolume": 1000 + i,
            "spread": 0.3,
            "isClosed": True,
        })

    # Fix times to be properly sequential (15 min intervals)
    from datetime import datetime, timedelta
    base = datetime(2026, 4, 12, 0, 0)
    for i, c in enumerate(candles):
        t = base + timedelta(minutes=15 * i)
        c["time"] = t.strftime("%Y-%m-%dT%H:%M:%S+07:00")

    return {
        "schemaVersion": "1.0.0",
        "source": "mt5",
        "broker": "Exness",
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "generatedAt": "2026-04-14T21:45:08+07:00",
        "serverTime": "2026-04-14T21:30:00+07:00",
        "timezone": "Asia/Ho_Chi_Minh",
        "terminal": {
            "platform": "MetaTrader5",
            "accountServer": "Exness-MT5Real",
            "build": 4755,
        },
        "market": {
            "currentPrice": {
                "bid": 4772.10,
                "ask": 4772.45,
                "mid": 4772.275,
                "spread": 0.35,
            },
            "sessionStats": {
                "dayOpen": 4758.40,
                "dayHigh": 4788.60,
                "dayLow": 4749.20,
                "prevDayHigh": 4791.30,
                "prevDayLow": 4738.10,
                "prevDayClose": 4761.20,
            },
        },
        "candles": candles,
        "meta": {
            "requestedBars": 200,
            "returnedBars": 150,
            "dataStatus": "partial",
        },
    }


# ── Tests: validate_business_rules ──

class TestValidateBusinessRules:
    def test_valid_payload_passes(self):
        errors = validate_business_rules(_valid_payload())
        assert errors == [], f"Unexpected errors: {errors}"

    def test_ask_less_than_bid(self):
        payload = _valid_payload()
        payload["market"]["currentPrice"]["ask"] = 4770.0  # less than bid
        payload["market"]["currentPrice"]["bid"] = 4772.0
        errors = validate_business_rules(payload)
        assert any("ask must be >= bid" in e for e in errors)

    def test_inconsistent_mid(self):
        payload = _valid_payload()
        payload["market"]["currentPrice"]["mid"] = 9999.0  # way off
        errors = validate_business_rules(payload)
        assert any("mid is inconsistent" in e for e in errors)

    def test_inconsistent_spread(self):
        payload = _valid_payload()
        payload["market"]["currentPrice"]["spread"] = 999.0  # way off
        errors = validate_business_rules(payload)
        assert any("spread is inconsistent" in e for e in errors)

    def test_day_high_less_than_low(self):
        payload = _valid_payload()
        payload["market"]["sessionStats"]["dayHigh"] = 4700.0
        payload["market"]["sessionStats"]["dayLow"] = 4800.0
        errors = validate_business_rules(payload)
        assert any("dayHigh must be >= dayLow" in e for e in errors)

    def test_invalid_ohlc_high(self):
        payload = _valid_payload()
        payload["candles"][0]["high"] = 1.0  # lower than open/close/low
        errors = validate_business_rules(payload)
        assert any("invalid high" in e for e in errors)

    def test_invalid_ohlc_low(self):
        payload = _valid_payload()
        payload["candles"][0]["low"] = 99999.0  # higher than everything
        errors = validate_business_rules(payload)
        assert any("invalid low" in e for e in errors)

    def test_too_few_candles(self):
        payload = _valid_payload()
        payload["candles"] = payload["candles"][:50]
        errors = validate_business_rules(payload)
        assert any("at least 100 bars" in e for e in errors)

    def test_unclosed_candle_flagged(self):
        payload = _valid_payload()
        payload["candles"][0]["isClosed"] = False
        errors = validate_business_rules(payload)
        assert any("candle must be closed" in e for e in errors)

    def test_backwards_time_flagged(self):
        payload = _valid_payload()
        # Swap two candles to create backwards time
        payload["candles"][5]["time"] = payload["candles"][3]["time"]
        errors = validate_business_rules(payload)
        assert any("not ascending" in e or "not a multiple" in e for e in errors)

    def test_weekend_gap_is_ok(self):
        """Weekend gaps (>48h) should not trigger errors if < 72h."""
        payload = _valid_payload()
        # Simulate a weekend gap at candle index 50
        from datetime import datetime, timedelta
        base_dt = datetime.fromisoformat(payload["candles"][50]["time"])
        # Shift all candles after index 50 by +49 hours (weekend gap)
        for i in range(51, len(payload["candles"])):
            orig = datetime.fromisoformat(payload["candles"][i]["time"])
            new_time = orig + timedelta(hours=49)
            payload["candles"][i]["time"] = new_time.isoformat()

        errors = validate_business_rules(payload)
        # Should NOT have "abnormal candle gap" since 49h < 72h
        gap_errors = [e for e in errors if "abnormal candle gap" in e]
        assert len(gap_errors) == 0


# ── Tests: validate_staleness ──

class TestValidateStaleness:
    def test_fresh_payload_passes(self):
        payload = {
            "generatedAt": "2026-04-14T21:45:08+07:00",
            "serverTime": "2026-04-14T21:30:00+07:00",  # 15 min before → close = 21:45
        }
        errors = validate_staleness(payload, max_age_seconds=120)
        assert errors == []

    def test_stale_payload_within_market_hours(self):
        payload = {
            "generatedAt": "2026-04-14T22:15:00+07:00",  # 30 min after close
            "serverTime": "2026-04-14T21:30:00+07:00",   # close = 21:45
        }
        errors = validate_staleness(payload, max_age_seconds=120)
        assert any("stale" in e for e in errors)

    def test_off_market_hours_not_flagged(self):
        """Large gaps (> 2h) assume off-market and don't flag."""
        payload = {
            "generatedAt": "2026-04-14T10:00:00+07:00",  # way after
            "serverTime": "2026-04-12T23:45:00+07:00",   # Friday close
        }
        errors = validate_staleness(payload, max_age_seconds=120)
        assert errors == []


# ── Tests: load_schema ──

class TestLoadSchema:
    def test_load_valid_schema(self):
        schema = {"type": "object", "properties": {}}
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(schema, f)
        try:
            result = load_schema(path)
            assert result["type"] == "object"
        finally:
            os.unlink(path)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_schema("nonexistent_schema_12345.json")


# ── Tests: validate_schema ──

class TestValidateSchema:
    def test_valid_against_simple_schema(self):
        schema = {"type": "object", "required": ["name"]}
        payload = {"name": "test"}
        errors = validate_schema(payload, schema)
        assert errors == []

    def test_invalid_missing_required(self):
        schema = {"type": "object", "required": ["name"]}
        payload = {"other": "value"}
        errors = validate_schema(payload, schema)
        assert len(errors) > 0
        assert "schema_error" in errors[0]

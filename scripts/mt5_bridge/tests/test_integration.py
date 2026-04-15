"""Integration tests — requires MT5 terminal running with Exness account.

These tests connect to a real MT5 terminal and verify the full pipeline.
Skip if MT5 is not available.

Run with:  pytest tests/test_integration.py -v -s
"""

import json
import os
import tempfile
import pytest

# Try to import MT5 — skip all tests if not available
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not MT5_AVAILABLE,
    reason="MetaTrader5 package not installed",
)


from mt5_bridge.fetcher import (
    initialize_mt5,
    shutdown_mt5,
    ensure_symbol,
    fetch_tick,
    fetch_rates,
    fetch_daily_rates,
    get_terminal_info,
)
from mt5_bridge.transformer import (
    build_current_price,
    build_candles,
    build_session_stats,
    build_payload,
)
from mt5_bridge.validator import (
    load_schema,
    validate_schema,
    validate_business_rules,
)
from mt5_bridge.utils import now_iso


@pytest.fixture(scope="module")
def mt5_connection():
    """Initialize MT5 once for all integration tests."""
    try:
        initialize_mt5()
        yield True
    except Exception as e:
        pytest.skip(f"MT5 not available: {e}")
    finally:
        shutdown_mt5()


class TestMT5Connection:
    def test_symbol_exists(self, mt5_connection):
        ensure_symbol("XAUUSD")  # Should not raise

    def test_fetch_tick(self, mt5_connection):
        tick = fetch_tick("XAUUSD")
        assert tick.bid > 0
        assert tick.ask > 0
        assert tick.ask >= tick.bid

    def test_fetch_m15_rates(self, mt5_connection):
        rates = fetch_rates("XAUUSD", "M15", 200)
        assert len(rates) == 200
        # Verify rate structure
        assert "open" in rates.dtype.names
        assert "high" in rates.dtype.names
        assert "low" in rates.dtype.names
        assert "close" in rates.dtype.names

    def test_fetch_d1_rates(self, mt5_connection):
        rates = fetch_daily_rates("XAUUSD", count=3)
        assert len(rates) >= 1

    def test_terminal_info(self, mt5_connection):
        info, version = get_terminal_info()
        assert info is not None
        assert version is not None


class TestFullPipeline:
    """End-to-end test: fetch → transform → validate."""

    def test_full_pipeline(self, mt5_connection):
        config = {
            "broker": "Exness",
            "symbol": "XAUUSD",
            "timeframe": "M15",
            "bars": 200,
            "timezone": "Asia/Ho_Chi_Minh",
        }

        # Fetch
        tick = fetch_tick("XAUUSD")
        rates = fetch_rates("XAUUSD", "M15", 200)
        d1_rates = fetch_daily_rates("XAUUSD", count=3)
        info, version = get_terminal_info()

        # Transform
        current_price = build_current_price(tick)
        candles = build_candles(rates, "Asia/Ho_Chi_Minh", closed_only=True)
        session_stats = build_session_stats(rates, d1_rates, "Asia/Ho_Chi_Minh")
        generated_at = now_iso("Asia/Ho_Chi_Minh")
        server_time = candles[-1]["time"] if candles else generated_at

        terminal_info = {
            "platform": "MetaTrader5",
            "accountServer": getattr(info, "server", None),
            "build": version[1] if version and len(version) > 1 else None,
        }

        payload = build_payload(
            config=config,
            generated_at=generated_at,
            server_time=server_time,
            terminal_info=terminal_info,
            current_price=current_price,
            session_stats=session_stats,
            candles=candles,
        )

        # Validate structure
        assert payload["schemaVersion"] == "1.0.0"
        assert payload["source"] == "mt5"
        assert payload["symbol"] == "XAUUSD"
        assert len(payload["candles"]) >= 100

        # Validate business rules
        rule_errors = validate_business_rules(payload)
        assert rule_errors == [], f"Business rule errors: {rule_errors}"

        # Validate against schema
        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "schemas", "mt5-xauusd-m15-payload-v1.json"
        )
        if os.path.exists(schema_path):
            schema = load_schema(schema_path)
            schema_errors = validate_schema(payload, schema)
            assert schema_errors == [], f"Schema errors: {schema_errors}"

        print(f"\n✅ Pipeline OK: {len(candles)} candles, "
              f"bid={current_price['bid']}, ask={current_price['ask']}")

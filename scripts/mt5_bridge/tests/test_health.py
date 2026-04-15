"""Tests for health.py — error payload builder."""

from mt5_bridge.health import build_error_payload


class TestBuildErrorPayload:
    def test_basic_structure(self):
        result = build_error_payload(
            timezone="Asia/Ho_Chi_Minh",
            generated_at="2026-04-14T21:45:08+07:00",
            broker="Exness",
            symbol="XAUUSD",
            timeframe="M15",
            code="MT5_ERROR",
            message="init failed",
            retryable=True,
        )

        assert result["schemaVersion"] == "1.0.0"
        assert result["source"] == "mt5"
        assert result["broker"] == "Exness"
        assert result["symbol"] == "XAUUSD"
        assert result["error"]["code"] == "MT5_ERROR"
        assert result["error"]["message"] == "init failed"
        assert result["error"]["retryable"] is True

    def test_non_retryable(self):
        result = build_error_payload(
            timezone="UTC",
            generated_at="t",
            broker="B",
            symbol="XAUUSD",
            timeframe="M15",
            code="VALIDATION_FAILED",
            message="bad data",
            retryable=False,
        )
        assert result["error"]["retryable"] is False

    def test_all_error_codes(self):
        """Ensure all documented error codes can be built."""
        codes = [
            "MT5_INIT_ERROR",
            "MT5_ERROR",
            "SYMBOL_ERROR",
            "FETCH_ERROR",
            "VALIDATION_FAILED",
            "WRITE_ERROR",
            "UNEXPECTED_ERROR",
        ]
        for code in codes:
            result = build_error_payload(
                timezone="UTC", generated_at="t", broker="B",
                symbol="XAUUSD", timeframe="M15",
                code=code, message="test",
            )
            assert result["error"]["code"] == code

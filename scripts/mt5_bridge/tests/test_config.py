"""Tests for config.py — config loading and validation."""

import json
import os
import tempfile
import pytest

from mt5_bridge.config import load_config, validate_config


# ── Fixtures ──

def _write_temp_config(data: dict) -> str:
    """Write a temp config file and return its path."""
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return path


def _valid_config() -> dict:
    return {
        "broker": "Exness",
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "bars": 200,
        "timezone": "Asia/Ho_Chi_Minh",
        "outputPath": "data/xauusd/latest-m15.json",
        "errorOutputPath": "data/xauusd/latest-m15.error.json",
        "logPath": "logs/mt5-xauusd/bridge.log",
        "schemaPath": "schemas/mt5-xauusd-m15-payload-v1.json",
        "maxPayloadAgeSeconds": 1800,
        "retryCount": 2,
        "retryDelayMs": 1500,
        "requireClosedBarsOnly": True,
    }


# ── Tests: load_config ──

class TestLoadConfig:
    def test_load_valid_config(self):
        config = _valid_config()
        path = _write_temp_config(config)
        try:
            result = load_config(path)
            assert result["broker"] == "Exness"
            assert result["symbol"] == "XAUUSD"
            assert result["bars"] == 200
        finally:
            os.unlink(path)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent_path_12345.json")


# ── Tests: validate_config ──

class TestValidateConfig:
    def test_valid_config_passes(self):
        validate_config(_valid_config())  # Should not raise

    def test_missing_required_field(self):
        config = _valid_config()
        del config["broker"]
        with pytest.raises(ValueError, match="Missing config fields"):
            validate_config(config)

    def test_wrong_symbol(self):
        config = _valid_config()
        config["symbol"] = "EURUSD"
        with pytest.raises(ValueError, match="v1 only supports symbol=XAUUSD"):
            validate_config(config)

    def test_wrong_timeframe(self):
        config = _valid_config()
        config["timeframe"] = "H1"
        with pytest.raises(ValueError, match="v1 only supports timeframe=M15"):
            validate_config(config)

    def test_bars_too_low(self):
        config = _valid_config()
        config["bars"] = 50
        with pytest.raises(ValueError, match="bars must be >= 100"):
            validate_config(config)

    def test_negative_retry_count(self):
        config = _valid_config()
        config["retryCount"] = -1
        with pytest.raises(ValueError, match="retryCount must be >= 0"):
            validate_config(config)

    def test_bars_boundary_100(self):
        config = _valid_config()
        config["bars"] = 100
        validate_config(config)  # Should not raise

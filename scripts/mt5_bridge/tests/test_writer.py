"""Tests for writer.py — file writing logic."""

import json
import os
import tempfile
import pytest

from mt5_bridge.writer import write_json_atomic, write_error_payload


class TestWriteJsonAtomic:
    def test_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "output.json")
            payload = {"key": "value", "number": 42}

            write_json_atomic(path, payload)

            assert os.path.exists(path)
            with open(path, "r", encoding="utf-8") as f:
                result = json.load(f)
            assert result["key"] == "value"
            assert result["number"] == 42

    def test_no_temp_file_left(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "output.json")
            write_json_atomic(path, {"test": True})

            # .tmp file should not exist after successful write
            assert not os.path.exists(path + ".tmp")

    def test_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sub", "dir", "output.json")
            write_json_atomic(path, {"nested": True})
            assert os.path.exists(path)

    def test_overwrites_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "output.json")
            write_json_atomic(path, {"version": 1})
            write_json_atomic(path, {"version": 2})

            with open(path, "r", encoding="utf-8") as f:
                result = json.load(f)
            assert result["version"] == 2

    def test_utf8_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "output.json")
            write_json_atomic(path, {"name": "Việt Nam", "emoji": "🚀"})

            with open(path, "r", encoding="utf-8") as f:
                result = json.load(f)
            assert result["name"] == "Việt Nam"


class TestWriteErrorPayload:
    def test_creates_error_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "error.json")
            payload = {
                "error": {"code": "TEST_ERROR", "message": "test"},
            }

            write_error_payload(path, payload)

            assert os.path.exists(path)
            with open(path, "r", encoding="utf-8") as f:
                result = json.load(f)
            assert result["error"]["code"] == "TEST_ERROR"

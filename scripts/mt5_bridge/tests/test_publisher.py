"""Tests for publisher.py — HTTP publishing to VPS."""

import json
import pytest
from unittest.mock import patch, MagicMock

from mt5_bridge.publisher import publish_payload, PublishError


def _mock_response(status_code=200, json_data=None, text=""):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {"ok": True, "message": "payload accepted"}
    resp.text = text or json.dumps(json_data or {"ok": True})
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        from requests.exceptions import HTTPError
        resp.raise_for_status.side_effect = HTTPError(f"{status_code} Error")
    return resp


class TestPublishPayload:
    @patch("mt5_bridge.publisher.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = _mock_response(
            200,
            {"ok": True, "message": "payload accepted", "symbol": "XAUUSD", "timeframe": "M15"},
        )

        result = publish_payload(
            endpoint_url="https://vps.example.com/api/market-data/xauusd-m15",
            bearer_token="test-token",
            payload={"schemaVersion": "1.0.0"},
        )

        assert result["ok"] is True
        assert result["message"] == "payload accepted"

        # Verify request was made correctly
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["json"] == {"schemaVersion": "1.0.0"}
        assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer test-token"
        assert call_kwargs.kwargs["headers"]["X-Source"] == "mt5-bridge"
        assert call_kwargs.kwargs["headers"]["X-Schema-Version"] == "1.0.0"
        assert call_kwargs.kwargs["headers"]["Content-Type"] == "application/json"

    @patch("mt5_bridge.publisher.requests.post")
    def test_401_unauthorized(self, mock_post):
        mock_post.return_value = _mock_response(401, text="Unauthorized")

        with pytest.raises(PublishError, match="401 Unauthorized"):
            publish_payload(
                endpoint_url="https://vps.example.com/api/market-data/xauusd-m15",
                bearer_token="wrong-token",
                payload={},
            )

    @patch("mt5_bridge.publisher.requests.post")
    def test_400_bad_request(self, mock_post):
        mock_post.return_value = _mock_response(400, text="Bad Request: missing field")

        with pytest.raises(PublishError, match="400 Bad Request"):
            publish_payload(
                endpoint_url="https://vps.example.com/api/market-data/xauusd-m15",
                bearer_token="token",
                payload={},
            )

    @patch("mt5_bridge.publisher.requests.post")
    def test_500_server_error(self, mock_post):
        mock_post.return_value = _mock_response(500, text="Internal Server Error")

        with pytest.raises(PublishError, match="500 Server Error"):
            publish_payload(
                endpoint_url="https://vps.example.com/api/market-data/xauusd-m15",
                bearer_token="token",
                payload={},
            )

    @patch("mt5_bridge.publisher.requests.post")
    def test_timeout(self, mock_post):
        import requests as req
        mock_post.side_effect = req.exceptions.Timeout("timed out")

        with pytest.raises(PublishError, match="timed out"):
            publish_payload(
                endpoint_url="https://vps.example.com/api/market-data/xauusd-m15",
                bearer_token="token",
                payload={},
                timeout=5,
            )

    @patch("mt5_bridge.publisher.requests.post")
    def test_connection_error(self, mock_post):
        import requests as req
        mock_post.side_effect = req.exceptions.ConnectionError("refused")

        with pytest.raises(PublishError, match="Connection failed"):
            publish_payload(
                endpoint_url="https://unreachable.example.com/api",
                bearer_token="token",
                payload={},
            )

    @patch("mt5_bridge.publisher.requests.post")
    def test_custom_timeout(self, mock_post):
        mock_post.return_value = _mock_response(200)

        publish_payload(
            endpoint_url="https://vps.example.com/api",
            bearer_token="token",
            payload={},
            timeout=30,
        )

        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["timeout"] == 30

"""
publisher.py — HTTP publisher for sending payload to VPS.

Sends the validated JSON payload to the OpenClaw/Livermore VPS
via HTTP POST with bearer token authentication.

Endpoint: POST /api/market-data/xauusd-m15
Headers:
  - Content-Type: application/json
  - Authorization: Bearer <shared-secret>
  - X-Source: mt5-bridge
  - X-Schema-Version: 1.0.0
"""

import requests
import logging

logger = logging.getLogger("mt5_bridge")


class PublishError(Exception):
    """Raised when publishing payload to VPS fails."""
    pass


def publish_payload(
    endpoint_url: str,
    bearer_token: str,
    payload: dict,
    timeout: int = 15,
) -> dict:
    """Send payload to VPS via HTTP POST.

    Args:
        endpoint_url: Full URL of the VPS endpoint
                      (e.g. https://vps.example.com/api/market-data/xauusd-m15).
        bearer_token: Shared secret for Authorization header.
        payload: Validated JSON payload dict.
        timeout: Request timeout in seconds.

    Returns:
        Response body as dict on success.

    Raises:
        PublishError: If the request fails or returns non-2xx status.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "X-Source": "mt5-bridge",
        "X-Schema-Version": "1.0.0",
    }

    try:
        resp = requests.post(
            endpoint_url,
            json=payload,
            headers=headers,
            timeout=timeout,
        )

        # Log response details
        logger.info(
            "publish_response status=%d url=%s",
            resp.status_code,
            endpoint_url,
        )

        # Handle specific error codes as per handoff doc
        if resp.status_code == 401:
            raise PublishError(
                f"401 Unauthorized: token invalid or missing. "
                f"Response: {resp.text[:200]}"
            )

        if resp.status_code == 400:
            raise PublishError(
                f"400 Bad Request: payload format rejected by VPS. "
                f"Response: {resp.text[:500]}"
            )

        if resp.status_code >= 500:
            raise PublishError(
                f"{resp.status_code} Server Error: VPS internal error. "
                f"Response: {resp.text[:200]}"
            )

        # Raise for any other non-2xx status
        resp.raise_for_status()

        return resp.json()

    except requests.exceptions.Timeout:
        raise PublishError(
            f"Request timed out after {timeout}s to {endpoint_url}"
        )
    except requests.exceptions.ConnectionError as e:
        raise PublishError(
            f"Connection failed to {endpoint_url}: {e}"
        )
    except requests.exceptions.RequestException as e:
        if isinstance(e.__cause__, PublishError):
            raise
        raise PublishError(f"HTTP request failed: {e}")

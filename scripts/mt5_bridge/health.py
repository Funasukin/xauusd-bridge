"""
health.py — Error payload builder for the MT5 bridge.

Builds standardized error payloads that conform to the Data Contract v1
error format. These are written to the error output path when the bridge
encounters failures.
"""


def build_error_payload(
    timezone: str,
    generated_at: str,
    broker: str,
    symbol: str,
    timeframe: str,
    code: str,
    message: str,
    retryable: bool = True,
) -> dict:
    """Build a standardized error payload.

    Error codes follow Data Contract v1:
        - MT5_INIT_ERROR: MT5 terminal failed to initialize
        - MT5_ERROR: General MT5 connection/fetch error
        - SYMBOL_ERROR: Symbol not found or cannot be selected
        - FETCH_ERROR: Failed to fetch tick or rates data
        - VALIDATION_FAILED: Payload failed schema or business rules
        - WRITE_ERROR: Failed to write output file
        - UNEXPECTED_ERROR: Unhandled exception

    Args:
        timezone: IANA timezone name.
        generated_at: ISO-8601 timestamp when the run started.
        broker: Broker name from config.
        symbol: Trading symbol.
        timeframe: Candle timeframe.
        code: Error code string.
        message: Human-readable error description.
        retryable: Whether this error type can be retried.

    Returns:
        Error payload dict ready for JSON serialization.
    """
    return {
        "schemaVersion": "1.0.0",
        "source": "mt5",
        "broker": broker,
        "symbol": symbol,
        "timeframe": timeframe,
        "generatedAt": generated_at,
        "timezone": timezone,
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
        },
    }

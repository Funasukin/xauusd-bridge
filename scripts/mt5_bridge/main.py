"""
main.py — MT5 Bridge entrypoint.

Orchestrates the full bridge pipeline:
  1. Load config
  2. Initialize MT5
  3. Fetch market data (tick, M15 rates, D1 rates)
  4. Transform into Data Contract v1 payload
  5. Validate (schema + business rules + staleness)
  6. Write output (atomic JSON or error JSON)
  7. Publish payload to VPS via HTTP POST

Supports retry logic and CLI flags for dry-run / stdout / no-publish modes.

Usage:
    # Dry run with stdout output (no write, no publish)
    python -m mt5_bridge.main --config config/mt5-xauusd.json --dry-run --stdout

    # Production run (write local + publish to VPS)
    python -m mt5_bridge.main --config config/mt5-xauusd.json

    # Write local only, skip VPS publish
    python -m mt5_bridge.main --config config/mt5-xauusd.json --no-publish

Exit Codes:
    0 = success
    1 = config error
    2 = MT5 init/connect error
    3 = symbol error
    4 = fetch error
    5 = validation error
    6 = write error
    7 = publish error
    9 = unexpected error
"""

import argparse
import sys
import time
import json

from mt5_bridge.config import load_config
from mt5_bridge.utils import init_logger, now_iso, sleep_ms
from mt5_bridge.health import build_error_payload
from mt5_bridge.fetcher import (
    initialize_mt5,
    shutdown_mt5,
    ensure_symbol,
    fetch_tick,
    fetch_rates,
    fetch_daily_rates,
    get_terminal_info,
    MT5Error,
    SymbolError,
    FetchError,
)
from mt5_bridge.transformer import (
    to_iso,
    build_current_price,
    build_candles,
    build_session_stats,
    build_payload,
)
from mt5_bridge.validator import (
    load_schema,
    validate_schema,
    validate_business_rules,
    validate_staleness,
)
from mt5_bridge.writer import write_json_atomic, write_error_payload
from mt5_bridge.publisher import publish_payload, PublishError


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="MT5 Bridge — Fetch XAUUSD M15 data and produce standardized JSON payload"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the JSON config file (e.g. config/mt5-xauusd.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run full pipeline but do not write output file",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print payload JSON to stdout",
    )
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help="Skip publishing payload to VPS (local write only)",
    )
    return parser.parse_args()


def build_terminal_info(info, version) -> dict:
    """Build terminal info block from MT5 terminal data.

    Args:
        info: MT5 terminal_info object.
        version: MT5 version tuple.

    Returns:
        Dict with platform, accountServer, and build number.
    """
    build = version[1] if version and len(version) > 1 else None
    return {
        "platform": "MetaTrader5",
        "accountServer": getattr(info, "server", None) if info else None,
        "build": build,
    }


def _write_error_and_log(config, logger, generated_at, code, message, retryable):
    """Helper to build error payload, write it, and log the error.

    Args:
        config: Bridge config dict.
        logger: Logger instance.
        generated_at: ISO-8601 timestamp.
        code: Error code string.
        message: Error message.
        retryable: Whether this error is retryable.
    """
    error_payload = build_error_payload(
        timezone=config["timezone"],
        generated_at=generated_at,
        broker=config["broker"],
        symbol=config["symbol"],
        timeframe=config["timeframe"],
        code=code,
        message=message,
        retryable=retryable,
    )

    try:
        write_error_payload(config["errorOutputPath"], error_payload)
        logger.info(
            "error_written path=%s code=%s",
            config["errorOutputPath"],
            code,
        )
    except Exception as write_err:
        logger.error("failed_to_write_error_payload: %s", write_err)


def run_once(
    config: dict,
    logger,
    dry_run: bool = False,
    stdout: bool = False,
    no_publish: bool = False,
) -> int:
    """Execute a single bridge run.

    Args:
        config: Bridge configuration dict.
        logger: Logger instance.
        dry_run: If True, skip writing output file.
        stdout: If True, print payload to stdout.

    Returns:
        Exit code (0 = success, see module docstring for others).
    """
    start_time = time.monotonic()
    generated_at = now_iso(config["timezone"])
    schema = load_schema(config["schemaPath"])

    try:
        logger.info(
            "run_start symbol=%s timeframe=%s bars=%s",
            config["symbol"],
            config["timeframe"],
            config["bars"],
        )

        # ── Step 1: Initialize MT5 ──
        initialize_mt5()
        logger.info("mt5_initialized")

        # ── Step 2: Ensure symbol is visible ──
        ensure_symbol(config["symbol"])
        logger.info("symbol_ready symbol=%s", config["symbol"])

        # ── Step 3: Fetch data ──
        info, version = get_terminal_info()
        tick = fetch_tick(config["symbol"])
        rates = fetch_rates(
            config["symbol"],
            config["timeframe"],
            int(config["bars"]),
        )
        d1_rates = fetch_daily_rates(config["symbol"], count=3)
        logger.info("data_fetched rates=%d d1_rates=%d", len(rates), len(d1_rates) if hasattr(d1_rates, '__len__') else 0)

        # ── Step 4: Transform ──
        current_price = build_current_price(tick)
        logger.info(
            "current_price bid=%.2f ask=%.2f mid=%.2f spread=%.5f",
            current_price["bid"],
            current_price["ask"],
            current_price["mid"],
            current_price["spread"],
        )

        candles = build_candles(
            rates,
            tz_name=config["timezone"],
            closed_only=config.get("requireClosedBarsOnly", True),
        )
        logger.info("candles_built count=%d", len(candles))

        if not candles:
            _write_error_and_log(
                config, logger, generated_at,
                "FETCH_ERROR", "No closed candles available", True,
            )
            return 4

        server_time = candles[-1]["time"]
        session_stats = build_session_stats(rates, d1_rates, config["timezone"])
        logger.info(
            "session_stats dayOpen=%.2f dayHigh=%.2f dayLow=%.2f",
            session_stats["dayOpen"],
            session_stats["dayHigh"],
            session_stats["dayLow"],
        )

        terminal_info = build_terminal_info(info, version)

        payload = build_payload(
            config=config,
            generated_at=generated_at,
            server_time=server_time,
            terminal_info=terminal_info,
            current_price=current_price,
            session_stats=session_stats,
            candles=candles,
        )

        # ── Step 5: Validate ──
        schema_errors = validate_schema(payload, schema)
        rule_errors = validate_business_rules(payload)
        stale_errors = validate_staleness(
            payload,
            max_age_seconds=int(config.get("maxPayloadAgeSeconds", 120)),
        )

        all_errors = schema_errors + rule_errors + stale_errors

        if all_errors:
            message = "; ".join(all_errors)
            logger.error("validation_failed errors=%d detail=%s", len(all_errors), message)

            _write_error_and_log(
                config, logger, generated_at,
                "VALIDATION_FAILED", message, False,
            )
            return 5

        logger.info("payload_valid")

        # ── Step 6: Output (local file) ──
        if stdout:
            print(json.dumps(payload, ensure_ascii=False, indent=2))

        if not dry_run:
            write_json_atomic(config["outputPath"], payload)
            logger.info("write_success path=%s", config["outputPath"])
        else:
            logger.info("dry_run_skip_write path=%s", config["outputPath"])

        # ── Step 7: Publish to VPS ──
        endpoint_url = config.get("endpointUrl", "")
        bearer_token = config.get("bearerToken", "")
        should_publish = (
            not dry_run
            and not no_publish
            and endpoint_url
            and not endpoint_url.startswith("https://<")
            and bearer_token
            and not bearer_token.startswith("<")
        )

        if should_publish:
            timeout = int(config.get("publishTimeoutSeconds", 15))
            try:
                resp = publish_payload(
                    endpoint_url=endpoint_url,
                    bearer_token=bearer_token,
                    payload=payload,
                    timeout=timeout,
                )
                logger.info(
                    "publish_success url=%s response=%s",
                    endpoint_url,
                    resp,
                )
            except PublishError as e:
                logger.error("publish_failed: %s", e)
                _write_error_and_log(
                    config, logger, generated_at,
                    "PUBLISH_ERROR", str(e), True,
                )
                return 7
        else:
            if dry_run:
                logger.info("dry_run_skip_publish")
            elif no_publish:
                logger.info("publish_skipped_by_flag")
            else:
                logger.info("publish_skipped_no_endpoint_configured")

        elapsed = time.monotonic() - start_time
        logger.info(
            "run_complete returnedBars=%d duration_ms=%.0f",
            len(candles),
            elapsed * 1000,
        )
        return 0

    except SymbolError as e:
        logger.exception("symbol_error")
        _write_error_and_log(
            config, logger, generated_at,
            "SYMBOL_ERROR", str(e), False,
        )
        return 3

    except FetchError as e:
        logger.exception("fetch_error")
        _write_error_and_log(
            config, logger, generated_at,
            "FETCH_ERROR", str(e), True,
        )
        return 4

    except MT5Error as e:
        logger.exception("mt5_error")
        _write_error_and_log(
            config, logger, generated_at,
            "MT5_ERROR", str(e), True,
        )
        return 2

    except Exception as e:
        logger.exception("unexpected_error")
        _write_error_and_log(
            config, logger, generated_at,
            "UNEXPECTED_ERROR", str(e), False,
        )
        return 9

    finally:
        shutdown_mt5()


def main():
    """Main entrypoint with retry logic."""
    args = parse_args()

    # ── Load config ──
    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"CONFIG_ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # ── Init logger ──
    logger = init_logger(config["logPath"])

    # ── Retry loop ──
    retries = int(config.get("retryCount", 0))
    delay_ms = int(config.get("retryDelayMs", 1000))

    last_code = 9
    for attempt in range(retries + 1):
        if attempt > 0:
            logger.warning(
                "retrying attempt=%d/%d next_in_ms=%d",
                attempt, retries, delay_ms,
            )
            sleep_ms(delay_ms)

        last_code = run_once(
            config,
            logger,
            dry_run=args.dry_run,
            stdout=args.stdout,
            no_publish=args.no_publish,
        )

        if last_code == 0:
            sys.exit(0)

        # Non-retryable errors: config (1), validation (5)
        # Publish errors (7) are retryable
        if last_code in (1, 5):
            logger.error("non_retryable_error code=%d — aborting", last_code)
            break

    logger.error("all_attempts_failed last_code=%d", last_code)
    sys.exit(last_code)


if __name__ == "__main__":
    main()

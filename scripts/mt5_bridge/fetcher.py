"""
fetcher.py — MetaTrader 5 data fetcher.

Handles all direct interaction with the MT5 terminal:
  - Initialize / shutdown connection
  - Ensure symbol is visible and selectable
  - Fetch current tick (bid/ask)
  - Fetch M15 OHLCV rates
  - Fetch D1 rates for session stats
  - Get terminal metadata
"""

import MetaTrader5 as mt5


# Mapping of timeframe strings to MT5 constants
TIMEFRAME_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1,
}


class MT5Error(Exception):
    """Raised when MT5 operations fail (retryable)."""
    pass


class SymbolError(MT5Error):
    """Raised when symbol lookup or selection fails."""
    pass


class FetchError(MT5Error):
    """Raised when tick or rate data fetch fails."""
    pass


def initialize_mt5() -> None:
    """Initialize connection to MT5 terminal.

    Raises:
        MT5Error: If the terminal cannot be initialized.
    """
    if not mt5.initialize():
        raise MT5Error(f"MT5 initialize failed: {mt5.last_error()}")


def shutdown_mt5() -> None:
    """Shutdown connection to MT5 terminal."""
    mt5.shutdown()


def ensure_symbol(symbol: str) -> None:
    """Ensure the symbol exists and is visible in Market Watch.

    Args:
        symbol: Trading symbol (e.g. 'XAUUSD').

    Raises:
        SymbolError: If the symbol is not found or cannot be selected.
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        raise SymbolError(f"Symbol not found: {symbol}")

    if not info.visible:
        selected = mt5.symbol_select(symbol, True)
        if not selected:
            raise SymbolError(f"Failed to select symbol: {symbol}")


def fetch_tick(symbol: str):
    """Fetch the latest tick data (bid/ask) for a symbol.

    Args:
        symbol: Trading symbol.

    Returns:
        MT5 tick object with bid, ask, etc.

    Raises:
        FetchError: If tick data is unavailable.
    """
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        raise FetchError(f"Failed to fetch tick for symbol: {symbol}")
    return tick


def fetch_rates(symbol: str, timeframe: str, bars: int):
    """Fetch OHLCV candle data from MT5.

    Uses copy_rates_from_pos to get the most recent N bars.

    Args:
        symbol: Trading symbol.
        timeframe: Timeframe string (e.g. 'M15').
        bars: Number of bars to fetch.

    Returns:
        Numpy structured array of rate data.

    Raises:
        FetchError: If no rates can be fetched.
    """
    tf = TIMEFRAME_MAP.get(timeframe)
    if tf is None:
        raise FetchError(f"Unsupported timeframe: {timeframe}")

    rates = mt5.copy_rates_from_pos(symbol, tf, 0, bars)
    if rates is None or len(rates) == 0:
        raise FetchError(f"Failed to fetch rates for {symbol}/{timeframe}")
    return rates


def fetch_daily_rates(symbol: str, count: int = 3):
    """Fetch daily (D1) rates for session stats calculation.

    Args:
        symbol: Trading symbol.
        count: Number of D1 bars to fetch (default 3).

    Returns:
        Numpy structured array of D1 rates, or empty list on failure.
    """
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_D1, 0, count)
    if rates is None or len(rates) == 0:
        return []
    return rates


def get_terminal_info():
    """Get MT5 terminal info and version.

    Returns:
        Tuple of (terminal_info, version_tuple).
    """
    info = mt5.terminal_info()
    version = mt5.version()
    return info, version

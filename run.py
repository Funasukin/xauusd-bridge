"""
run.py — Root-level runner for MT5 Bridge.

Adds the scripts/ directory to sys.path so that mt5_bridge package
can be imported regardless of where this script is called from.

Usage (from project root):
    python run.py --config config/mt5-xauusd.json --dry-run --stdout
    python run.py --config config/mt5-xauusd.json
"""

import sys
from pathlib import Path

# Ensure scripts/ is on the import path
project_root = Path(__file__).resolve().parent
scripts_dir = project_root / "scripts"
sys.path.insert(0, str(scripts_dir))

from mt5_bridge.main import main

if __name__ == "__main__":
    main()

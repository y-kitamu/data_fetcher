"""constants.py"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).absolute().parent.parent

JP_TICKERS_PATH = PROJECT_ROOT / "data" / "jp_tickers.csv"
US_TICKERS_PATH = PROJECT_ROOT / "data" / "us_tickers.csv"

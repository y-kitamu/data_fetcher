"""E2E test: fetch_jp_news.py の小規模版テスト.

対象:
  - KabutanNewsFetcher: 直近 3 日 (日付ベース, データが存在する平日のみ)
  - GNewsFetcher       : 直近 3 日 (既存ファイルはスキップ)
  - YfinanceNewsFetcher: 2 銘柄 × 直近 3 日

実行方法:
    uv run python tests/test_fetch_jp_news_e2e.py
"""

import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
from loguru import logger

TEST_TICKERS = ["9984", "8035"]  # SoftBank, Tokyo Electron

TODAY = datetime.date.today()
KABUTAN_START = TODAY - datetime.timedelta(days=2)  # test last 3 days
GNEWS_START = TODAY - datetime.timedelta(days=3)

BASE_DATA = Path(__file__).parent.parent / "data" / "news"


def _check_csv(path: Path) -> tuple[int, int]:
    """Return (total_rows, rows_with_body)."""
    if not path.exists():
        return 0, 0
    df = pl.read_csv(path, infer_schema_length=0)
    with_body = df.filter(pl.col("body").str.len_chars() > 50)
    return len(df), len(with_body)


def test_kabutan() -> None:
    from data_fetcher.fetchers.stocks.kabutan_news import KabutanNewsFetcher

    logger.info(f"=== [1/3] Kabutan ({KABUTAN_START} → {TODAY}) ===")
    KabutanNewsFetcher().run(KABUTAN_START, TODAY)

    logger.info("--- Kabutan results ---")
    kabutan_dir = BASE_DATA / "kabutan"
    d = KABUTAN_START
    found_any = False
    while d <= TODAY:
        csv_path = kabutan_dir / f"{d.isoformat()}.csv"
        assert csv_path.exists(), f"Missing Kabutan file: {csv_path.name}"
        total, with_body = _check_csv(csv_path)
        logger.info(f"  {d}: {total} rows, {with_body} with body")
        if total > 0:
            found_any = True
        d += datetime.timedelta(days=1)
    if not found_any:
        logger.warning("Kabutan: all dates returned 0 rows (may be weekend/holiday)")
    logger.info("Kabutan: ✅ PASS")


def test_gnews() -> None:
    from data_fetcher.fetchers.stocks.gnews_fetcher import GNewsFetcher

    logger.info(f"=== [2/3] GNews ({GNEWS_START} → {TODAY}) ===")
    GNewsFetcher().run(GNEWS_START, TODAY)

    logger.info("--- GNews results ---")
    total_rows = 0
    d = GNEWS_START
    while d <= TODAY:
        total, with_body = _check_csv(BASE_DATA / "gnews" / f"{d.isoformat()}.csv")
        logger.info(f"  {d}: {total} rows, {with_body} with body")
        total_rows += total
        d += datetime.timedelta(days=1)
    logger.info(f"  total: {total_rows} rows across {(TODAY - GNEWS_START).days + 1} days")
    # GNews may have 0 results on weekends – only fail if files are missing entirely
    for i in range((TODAY - GNEWS_START).days + 1):
        d = GNEWS_START + datetime.timedelta(days=i)
        csv_path = BASE_DATA / "gnews" / f"{d.isoformat()}.csv"
        assert csv_path.exists(), f"Missing GNews file: {csv_path.name}"
    logger.info("GNews: ✅ PASS")


def test_yfinance() -> None:
    from data_fetcher.fetchers.stocks.yfinance_news import YfinanceNewsFetcher

    logger.info(f"=== [3/3] yfinance ({TEST_TICKERS}, {GNEWS_START} → {TODAY}) ===")
    with patch(
        "data_fetcher.fetchers.stocks.yfinance_news.get_jp_ticker_list",
        return_value=TEST_TICKERS,
    ):
        YfinanceNewsFetcher().run(GNEWS_START, TODAY)

    logger.info("--- yfinance results ---")
    yf_dir = BASE_DATA / "yfinance"
    assert yf_dir.exists(), f"yfinance data dir not found: {yf_dir}"
    # Each date in the range must have its own CSV file
    d = GNEWS_START
    total_rows = 0
    while d <= TODAY:
        csv_path = yf_dir / f"{d.isoformat()}.csv"
        assert csv_path.exists(), f"Missing yfinance file: {csv_path.name}"
        df = pl.read_csv(csv_path, infer_schema_length=0)
        logger.info(f"  {d}: {len(df)} rows")
        total_rows += len(df)
        for col in ["published_at", "source", "symbol", "title", "body", "url"]:
            assert col in df.columns, f"Missing column in {csv_path.name}: {col}"
        d += datetime.timedelta(days=1)
    logger.info(f"  total: {total_rows} rows across {(TODAY - GNEWS_START).days + 1} days")
    logger.info("yfinance: ✅ PASS")


if __name__ == "__main__":
    test_kabutan()
    test_gnews()
    test_yfinance()
    logger.info("\n=== ALL E2E TESTS PASSED ===")

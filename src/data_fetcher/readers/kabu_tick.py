"""Reader for raw kabu STATION tick logs written by stock's autotrade DataFeed."""

import datetime
import gzip
import json
import re
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT
from ..core.minutes_bar import convert_tick_to_ohlc

KABU_TICK_DATA_DIR = PROJECT_ROOT / "../stock/logs/ticks"

_FILENAME_RE = re.compile(r"^(?P<symbol>.+)_(?P<date>\d{4}-\d{2}-\d{2})\.jsonl(?:\.gz)?$")
_NOOP_STATUS = "0000"


class KabuTickReader(BaseReader):
    """Reader for kabu STATION push-payload tick logs (stock/logs/ticks/*.jsonl[.gz])."""

    def __init__(self, data_dir: Path = KABU_TICK_DATA_DIR):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

    def _iter_files(
        self, symbol: str | None = None
    ) -> list[tuple[Path, str, datetime.date]]:
        """Glob and parse tick files, optionally filtered to one symbol.

        Returns list of (path, symbol, date) tuples sorted by date.
        """
        if not self.data_dir.exists():
            return []
        pattern = f"{symbol}_*.jsonl*" if symbol else "*.jsonl*"
        results = []
        for path in self.data_dir.glob(pattern):
            m = _FILENAME_RE.match(path.name)
            if m is None:
                continue
            file_symbol = m.group("symbol")
            if symbol is not None and file_symbol != symbol:
                continue
            try:
                file_date = datetime.datetime.strptime(
                    m.group("date"), "%Y-%m-%d"
                ).date()
            except ValueError:
                continue
            results.append((path, file_symbol, file_date))
        results.sort(key=lambda t: t[2])
        return results

    @property
    def available_tickers(self) -> list[str]:
        """Get list of available ticker symbols.

        Returns:
            list[str]: List of available ticker symbols
        """
        if len(self._available_tickers) == 0:
            tickers = {sym for _, sym, _ in self._iter_files()}
            self._available_tickers = sorted(tickers)
        return self._available_tickers

    def _get_dates(self, symbol: str) -> list[datetime.date]:
        return sorted({d for _, _, d in self._iter_files(symbol)})

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        """Get the earliest available date for a symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            datetime.datetime: Earliest available date, or 1970-01-01 if none found
        """
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return datetime.datetime.combine(dates[0], datetime.time.min)

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        """Get the latest available date for a symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            datetime.datetime: Latest available date, or 1970-01-01 if none found
        """
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return datetime.datetime.combine(dates[-1], datetime.time.min)

    def _read_raw_lines(self, path: Path) -> list[dict]:
        """Read and json.loads every line of a tick file.

        Transparently gzip-decodes based on suffix. Tolerates a truncated
        trailing line since today's file may be actively appended to by the
        live DataFeed process.
        """
        opener = gzip.open if path.suffix == ".gz" else open
        rows = []
        try:
            with opener(path, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except (OSError, FileNotFoundError):
            # File may have been rotated (compressed + deleted) between glob and open.
            return []
        return rows

    def _rows_to_tick_df(
        self, rows: list[dict], timezone_delta: datetime.timedelta
    ) -> pl.DataFrame:
        if len(rows) == 0:
            return pl.DataFrame()

        raw = pl.DataFrame(
            {
                "_idx": list(range(len(rows))),
                "CurrentPrice": [r.get("CurrentPrice") for r in rows],
                "CurrentPriceTime": [r.get("CurrentPriceTime") for r in rows],
                "CurrentPriceChangeStatus": [
                    r.get("CurrentPriceChangeStatus") for r in rows
                ],
                "TradingVolume": [r.get("TradingVolume") for r in rows],
                "TradingValue": [r.get("TradingValue") for r in rows],
            },
            schema={
                "_idx": pl.Int64,
                "CurrentPrice": pl.Float64,
                "CurrentPriceTime": pl.Utf8,
                "CurrentPriceChangeStatus": pl.Utf8,
                "TradingVolume": pl.Float64,
                "TradingValue": pl.Float64,
            },
        )

        # TradingVolume/TradingValue are cumulative-for-the-day and may arrive on
        # lines without CurrentPrice, so forward-fill and diff across ALL rows
        # before filtering down to actual price ticks. The first row's diff has
        # no predecessor; fill it with the raw cumulative value itself (i.e. diff
        # against an implicit 0), which means a log that starts mid-session will
        # attribute all untracked prior-session volume to its first recorded tick.
        raw = raw.sort("_idx").with_columns(
            pl.col("TradingVolume").fill_null(strategy="forward"),
            pl.col("TradingValue").fill_null(strategy="forward"),
        )
        raw = raw.with_columns(
            pl.col("TradingVolume")
            .diff()
            .fill_null(pl.col("TradingVolume"))
            .alias("volume"),
            pl.col("TradingValue")
            .diff()
            .fill_null(pl.col("TradingValue"))
            .alias("amount"),
        )

        # A tick is a real price update: CurrentPrice/CurrentPriceTime present
        # (matches DataFeed._on_message's own "if not price or not time_str: return"
        # convention) and CurrentPriceChangeStatus is not the "0000" (no-op) code.
        ticks = raw.filter(
            pl.col("CurrentPrice").is_not_null()
            & (pl.col("CurrentPrice") != 0)
            & pl.col("CurrentPriceTime").is_not_null()
            & (pl.col("CurrentPriceTime") != "")
            & (
                pl.col("CurrentPriceChangeStatus").is_null()
                | (pl.col("CurrentPriceChangeStatus") != _NOOP_STATUS)
            )
        )
        if len(ticks) == 0:
            return pl.DataFrame()

        ticks = ticks.with_columns(
            (
                pl.col("CurrentPriceTime")
                .str.to_datetime(time_zone="UTC", strict=False)
                .dt.replace_time_zone(None)
                + timezone_delta
            ).alias("datetime"),
            pl.col("CurrentPrice").alias("price"),
        ).select(["datetime", "price", "volume", "amount"])

        return ticks

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        """Read tick data for a symbol.

        Args:
            symbol: Ticker symbol
            start_date: Start date (default: 1970-01-01)
            end_date: End date (default: now)
            timezone_delta: Timezone offset (default: 9 hours for JST)

        Returns:
            pl.DataFrame: Tick data with datetime, price, volume, amount columns
        """
        files = self._iter_files(symbol)
        files = [
            (path, sym, d)
            for path, sym, d in files
            if start_date.date() <= d <= end_date.date()
        ]
        if len(files) == 0:
            return pl.DataFrame()

        dfs = []
        for path, _sym, _file_date in files:
            rows = self._read_raw_lines(path)
            df = self._rows_to_tick_df(rows, timezone_delta)
            if len(df) > 0:
                dfs.append(df)

        if len(dfs) == 0:
            return pl.DataFrame()

        result = pl.concat(dfs).sort("datetime")
        result = result.filter(
            pl.col("datetime").is_between(start_date, end_date, closed="left")
        )
        return result

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> pl.DataFrame:
        """Read OHLC data for a symbol.

        Args:
            symbol: Ticker symbol
            interval: Time interval for OHLC bars
            start_date: Start date
            end_date: End date

        Returns:
            pl.DataFrame: OHLC data with datetime, open, high, low, close, volume columns
        """
        tick_df = self.read_ticker(symbol, start_date, end_date)
        if len(tick_df) == 0:
            return pl.DataFrame()

        ohlc_df = convert_tick_to_ohlc(
            tick_df,
            interval,
            date_key="datetime",
            price_key="price",
            volume_key="volume",
        )
        return ohlc_df.select(["datetime", "open", "high", "low", "close", "volume"])

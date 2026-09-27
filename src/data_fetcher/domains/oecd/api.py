"""api.py - OECD SDMX API（短期経済統計、Composite Leading Indicators）クライアント

認証不要。景気先行指数(CLI)データフローをCSV形式で直接取得できる。
旧`data.oecd.org`(OECD.Stat)は2024年に廃止されているため使わない
(実測確認: `sdmx.oecd.org`のREST APIが現行のエンドポイント)。
"""

import io

import polars as pl
import requests

from ...core.retry import retry_with_backoff

BASE_URL = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,"
TIMEOUT = 20.0


def build_cli_url(ref_area: str) -> str:
    """景気先行指数(CLI, 季節調整済・振幅調整済・月次)のデータ取得URLを組み立てる。

    `ref_area`はISO3166-1 alpha-3（例: "JPN", "USA", "CHN"）。
    """
    return f"{BASE_URL}/{ref_area}.M.LI...AA...H?format=csvfilewithlabels"


@retry_with_backoff(
    max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,)
)
def download_csv(session: requests.Session, url: str) -> bytes:
    res = session.get(url, timeout=TIMEOUT)
    res.raise_for_status()
    return res.content


def parse_cli_csv(raw: bytes) -> pl.DataFrame:
    """`format=csvfilewithlabels`のレスポンスから`date, value`だけを取り出す。

    `TIME_PERIOD`は"YYYY-MM"形式なので、月初日付("YYYY-MM-01")に変換する。
    """
    df = pl.read_csv(io.BytesIO(raw), infer_schema_length=0)
    return df.select(
        (pl.col("TIME_PERIOD") + "-01").alias("date"),
        pl.col("OBS_VALUE").alias("value"),
    )

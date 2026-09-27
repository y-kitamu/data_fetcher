"""fetch_data_from_boj_stats.py
日銀時系列統計データ検索サイトの一括ダウンロードZIP（企業物価指数・短観）を取得する。

いずれも認証不要で https://www.stat-search.boj.or.jp/info/dload.html からそのまま
ダウンロードできる。企業物価指数(cgpi_m_jp.zip)は3,042系列、短観(co.zip)は
48,430系列と非常に多いため、1系列=1ファイルではなく「1データセット=1ファイル」
（series_code列付きのlong形式）にまとめて保存する。

値の改定履歴を残すため、取得したデータにfetched_at列を付けた上で
`append_and_save_csv(..., dedup_subset=["series_code", "date", "value"])`で
「値が変わった時だけ追記する」（値が変わっていない再取得では行が増えない）。

動作確認済み: 2026-09-26に実行し、data/boj_stats/cgpi.csv(243,360行)・
tankan.csv(54,831行)が生成されることを確認。
"""

import argparse
import datetime

import polars as pl

import data_fetcher
from data_fetcher.domains.boj_stats import api as boj_api
from data_fetcher.domains.boj_stats import parser as boj_parser

OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/boj_stats"

ZIP_CATALOG = [
    {
        "zip_filename": "cgpi_m_jp.zip",
        "csv_filename": "cgpi_m_jp.csv",
        "format": "wide",
        "output": "cgpi.csv",
    },
    {
        "zip_filename": "co.zip",
        "csv_filename": "co.csv",
        "format": "long",
        "output": "tankan.csv",
    },
]


def update_boj_stats(zip_catalog: list[dict] = ZIP_CATALOG) -> None:
    session = data_fetcher.get_session(max_requests_per_second=1, cache_file=None)
    fetched_at = datetime.date.today().isoformat()

    for entry in zip_catalog:
        try:
            zip_bytes = boj_api.download_zip(session, entry["zip_filename"])
            raw = boj_api.extract_csv_from_zip(zip_bytes, entry["csv_filename"])
            text = boj_parser.decode_shift_jis(raw)
            if entry["format"] == "wide":
                df = boj_parser.parse_wide_csv(text)
            else:
                df = boj_parser.parse_tankan_csv(text)
        except Exception as e:
            data_fetcher.logger.error(f"Failed to fetch {entry['zip_filename']}: {e}")
            continue

        df = df.with_columns(pl.lit(fetched_at).alias("fetched_at"))
        output_path = OUTPUT_DIR / entry["output"]
        data_fetcher.append_and_save_csv(
            df,
            output_path,
            sort_col=["series_code", "date"],
            dedup_subset=["series_code", "date", "value"],
        )
        data_fetcher.logger.info(
            f"Fetched {entry['zip_filename']}: {df.height} rows, saved to {output_path}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    update_boj_stats()


if __name__ == "__main__":
    main()

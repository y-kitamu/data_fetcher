"""fetch_data_from_fred.py
FRED（セントルイス連銀）APIから米国マクロ経済指標を定期取得する。

無料のAPIキー登録のみで利用可能（cert/fred_api_key.txtに配置済み）。1回のAPI
呼び出しで指定シリーズの全履歴が返るため、値の改定履歴を残すためfetched_at列を
付けた上でappend_and_save_csv(..., dedup_subset=["date", "value"])で
「値が変わった時だけ追記する」（値が変わっていない再取得では行が増えない）。

FRED_SERIESの選定理由（住宅/労働/物価/センチメント/為替/金利/生産/自動車の
docs/20260926_indices.mdでカバーした領域を中心に拡張、いずれも実在確認済み):
  HOUST, PERMIT: 米住宅着工・着工許可
  MORTGAGE30US: 30年固定住宅ローン金利
  CSUSHPINSA: Case-Shiller住宅価格指数
  UNRATE, PAYEMS: 失業率・非農業部門雇用者数
  CPIAUCSL, PPIACO: 消費者物価指数・生産者物価指数
  UMCSENT: ミシガン大学消費者信頼感指数
  DEXJPUS, DEXCHUS: 円ドル・人民元ドル相場
  DGS10: 米10年国債利回り
  INDPRO: 鉱工業生産指数
  TOTALSA: 新車販売(SAAR)

除外理由: OECD景気先行指数(CLI)のFREDミラー(USALOLITONOSTSAM/JPNLOLITONOSTSAM)
は2024-01を最後に更新停止済み（実測確認）のため、OECD直接取得
(fetch_data_from_oecd.py)に譯る。ISM製造業PMI・中国official PMIはFRED未収録
（series/searchで該当なしを確認済み）のため対象外。

動作確認済み: 2026-09-26に実行し、data/fred/配下に14シリーズ分のCSVが
生成されることを確認。
"""

import argparse
import datetime

import polars as pl

import data_fetcher
from data_fetcher.domains.fred import api as fred_api

OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/fred"

FRED_SERIES = [
    "HOUST",
    "PERMIT",
    "MORTGAGE30US",
    "CSUSHPINSA",
    "UNRATE",
    "PAYEMS",
    "CPIAUCSL",
    "PPIACO",
    "UMCSENT",
    "DEXJPUS",
    "DEXCHUS",
    "DGS10",
    "INDPRO",
    "TOTALSA",
]


def update_fred(series_ids: list[str] = FRED_SERIES) -> None:
    session = data_fetcher.get_session(max_requests_per_second=2, cache_file=None)
    api_key = fred_api.load_api_key()
    fetched_at = datetime.date.today().isoformat()

    for series_id in series_ids:
        try:
            observations = fred_api.get_series_observations(session, api_key, series_id)
            observations = fred_api.clean_observations(observations)
        except Exception as e:
            data_fetcher.logger.error(f"Failed to fetch FRED series {series_id}: {e}")
            continue

        if not observations:
            data_fetcher.logger.warning(f"No observations for FRED series {series_id}.")
            continue

        df = pl.DataFrame(
            observations, schema={"date": pl.Utf8, "value": pl.Utf8}
        ).with_columns(pl.lit(fetched_at).alias("fetched_at"))
        output_path = OUTPUT_DIR / f"{series_id}.csv"
        data_fetcher.append_and_save_csv(
            df, output_path, sort_col="date", dedup_subset=["date", "value"]
        )
        data_fetcher.logger.info(
            f"Fetched {series_id}: {df.height} rows, saved to {output_path}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    update_fred()


if __name__ == "__main__":
    main()

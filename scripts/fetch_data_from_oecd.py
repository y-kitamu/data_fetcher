"""fetch_data_from_oecd.py
OECD SDMX APIから景気先行指数(CLI, Composite Leading Indicator)を定期取得する。

認証不要（https://sdmx.oecd.org/public/rest/data/... を直接叩ける）。旧
`data.oecd.org`(OECD.Stat)は2024年に廃止されているため使わない（実測確認済み）。
FREDに存在するOECD CLIのミラー系列(USALOLITONOSTSAM等)は2024-01を最後に
更新停止済みのため、こちらが唯一の最新データ取得手段になる。

値の改定履歴を残すためfetched_at列を付けた上で
append_and_save_csv(..., dedup_subset=["date", "value"])で「値が変わった時だけ
追記する」（値が変わっていない再取得では行が増えない）。

OECD_CLI_COUNTRIES: JPN/USA/CHNいずれも実データが返ることを確認済み
（日本は2025-06まで、FREDミラーより新しい）。消費者信頼感(CCI)・企業信頼感(BCI)
等の他データフローは、推測したdataflow ID(DF_CCI/DF_BCI)が誤りでヒットしなかった
ため今回のスコープ外（正しいIDが判明したら別途追加）。

動作確認済み: 2026-09-26に実行し、data/oecd/配下にCLI_JPN.csv/CLI_USA.csv/
CLI_CHN.csvが生成されることを確認。
"""

import argparse
import datetime

import polars as pl

import data_fetcher
from data_fetcher.domains.oecd import api as oecd_api

OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/oecd"

OECD_CLI_COUNTRIES = ["JPN", "USA", "CHN"]


def update_oecd_cli(countries: list[str] = OECD_CLI_COUNTRIES) -> None:
    session = data_fetcher.get_session(max_requests_per_second=2, cache_file=None)
    fetched_at = datetime.date.today().isoformat()

    for ref_area in countries:
        try:
            url = oecd_api.build_cli_url(ref_area)
            raw = oecd_api.download_csv(session, url)
            df = oecd_api.parse_cli_csv(raw)
        except Exception as e:
            data_fetcher.logger.error(f"Failed to fetch OECD CLI for {ref_area}: {e}")
            continue

        if df.height == 0:
            data_fetcher.logger.warning(f"No CLI observations for {ref_area}.")
            continue

        df = df.with_columns(pl.lit(fetched_at).alias("fetched_at"))
        output_path = OUTPUT_DIR / f"CLI_{ref_area}.csv"
        data_fetcher.append_and_save_csv(
            df, output_path, sort_col="date", dedup_subset=["date", "value"]
        )
        data_fetcher.logger.info(
            f"Fetched CLI_{ref_area}: {df.height} rows, saved to {output_path}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    update_oecd_cli()


if __name__ == "__main__":
    main()

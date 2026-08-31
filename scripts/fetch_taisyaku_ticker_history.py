"""fetch_taisyaku_ticker_history.py
日証金（taisyaku.jp）の銘柄詳細検索を使い、銘柄ごとに融資新規/返済・貸株新規/返済を
含む信用残高履歴を取得する。

/app/stock/detail/{code}/search は日付範囲に上限がなく、1銘柄につき1回のリクエストで
サイトが保有する全期間のデータを取得できる（データがない範囲は自動的に切り詰められる）。
そのため銘柄数分のリクエストが必要になる一方、日付を1日ずつ辿る必要はない。

既存の融資残高一覧（zandaka.csv、fetch_data_from_taisyaku.py）は新規/返済の内訳を含み
毎日1リクエストで全銘柄分を取得できるため、当日以降の分はそちらを日次cronで蓄積し、
本スクリプトは主に過去分のバックフィルおよび直近日の確報反映（--overlap-days）に使う想定。
"""

import argparse
import csv
import datetime
import io
import time

import tqdm

import data_fetcher
from data_fetcher.domains.taisyaku.data import TRJO_KBN_BY_MARKET

HISTORY_DIR = data_fetcher.constants.PROJECT_ROOT / "data/taisyaku/history_by_ticker"
DEFAULT_MIN_DATE = datetime.date(2015, 1, 1)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codes",
        nargs="*",
        default=None,
        help="対象の銘柄コード（省略時は取引可能な全銘柄）",
    )
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=10,
        help="既存データがある銘柄について再取得し上書きする直近日数（速報→確報の反映用）",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    return parser.parse_args()


def list_stock_market_combos(
    session, csrf_token: str
) -> tuple[list[tuple[str, str, str]], str]:
    """直近の営業日の全銘柄一覧から (銘柄コード, 市場区分, 銘柄名) の組を取得する。"""
    today = datetime.date.today()
    count = 0
    for offset in range(10):
        target_date = today - datetime.timedelta(days=offset)
        count, csrf_token = data_fetcher.domains.taisyaku.data.search_stocks_by_date(
            session, csrf_token, target_date
        )
        if count > 0:
            break
    if count == 0:
        raise RuntimeError("Could not find a recent business day with data.")

    csv_text = data_fetcher.domains.taisyaku.data.download_search_result_csv(session)
    reader = csv.DictReader(io.StringIO(csv_text))
    combos = [(row["銘柄コード"], row["市場区分"], row["銘柄名"]) for row in reader]
    return combos, csrf_token


def read_latest_date(output_path) -> str | None:
    if not output_path.exists():
        return None
    with open(output_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None
    return max(row["申込日"] for row in rows)


def merge_and_write(output_path, new_csv_text: str):
    # 検索範囲の終端が未確定日（当日など）の場合、値が空のプレースホルダー行が
    # 含まれることがあるため除外する。
    new_rows = [
        row
        for row in csv.DictReader(io.StringIO(new_csv_text))
        if row.get("融資残高（株）")
    ]
    if not new_rows:
        return
    fieldnames = list(new_rows[0].keys())

    existing_rows = []
    if output_path.exists():
        with open(output_path, encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))

    new_dates = {row["申込日"] for row in new_rows}
    merged = [row for row in existing_rows if row["申込日"] not in new_dates] + new_rows
    merged.sort(key=lambda row: row["申込日"])

    output_path.parent.mkdir(exist_ok=True, parents=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged)


def fetch_all(codes: list[str] | None, overlap_days: int, sleep_seconds: float):
    session = data_fetcher.get_session(max_requests_per_second=2, cache_file=None)
    csrf_token = data_fetcher.domains.taisyaku.data.get_stock_search_csrf_token(session)

    combos, csrf_token = list_stock_market_combos(session, csrf_token)
    if codes:
        codes_set = set(codes)
        combos = [c for c in combos if c[0] in codes_set]

    end_date = datetime.date.today()

    for code, market, name in tqdm.tqdm(combos):
        trjo_kbn = TRJO_KBN_BY_MARKET.get(market)
        if trjo_kbn is None:
            data_fetcher.logger.warning(f"Unknown market '{market}' for {code}. Skip.")
            continue

        output_path = HISTORY_DIR / f"{code}-{trjo_kbn}.csv"
        latest_date = read_latest_date(output_path)
        if latest_date is None:
            start_date = DEFAULT_MIN_DATE
        else:
            start_date = min(
                end_date,
                datetime.datetime.strptime(latest_date, "%Y%m%d").date()
                - datetime.timedelta(days=overlap_days),
            )

        csv_text, csrf_token = (
            data_fetcher.domains.taisyaku.data.fetch_ticker_history_csv_with_retry(
                session, csrf_token, code, trjo_kbn, start_date, end_date, name
            )
        )
        merge_and_write(output_path, csv_text)
        time.sleep(sleep_seconds)


def main():
    args = parse_args()
    fetch_all(args.codes, args.overlap_days, args.sleep_seconds)


if __name__ == "__main__":
    main()

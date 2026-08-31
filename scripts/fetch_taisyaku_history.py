"""fetch_taisyaku_history.py
日証金（taisyaku.jp）の銘柄検索機能を使い、全銘柄の過去の信用残高履歴を日別に取得する
"""

import argparse
import datetime
import time

import tqdm

import data_fetcher

HISTORY_DIR = data_fetcher.constants.PROJECT_ROOT / "data/taisyaku/history"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="取得開始日 (YYYY-MM-DD)。省略時はサイト側のデータ保有期間の開始日を自動検出する。",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d").date(),
        default=datetime.date.today(),
        help="取得終了日 (YYYY-MM-DD)。省略時は今日。",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="1日分取得するごとの待機秒数。",
    )
    return parser.parse_args()


def fetch_history(start_date: datetime.date | None, end_date: datetime.date, sleep_seconds: float):
    HISTORY_DIR.mkdir(exist_ok=True, parents=True)

    session = data_fetcher.get_session(max_requests_per_second=2, cache_file=None)
    csrf_token = data_fetcher.domains.taisyaku.data.get_stock_search_csrf_token(session)

    if start_date is None:
        data_fetcher.logger.info("Auto-detecting earliest available date...")
        start_date, csrf_token = data_fetcher.domains.taisyaku.data.find_earliest_available_date(
            session, csrf_token, end_date
        )
        data_fetcher.logger.info(f"Earliest available date detected: {start_date}")

    dates = [
        start_date + datetime.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]

    for target_date in tqdm.tqdm(dates):
        output_path = HISTORY_DIR / f"{target_date:%Y%m%d}.csv"
        if output_path.exists():
            continue

        csv_text, csrf_token = data_fetcher.domains.taisyaku.data.fetch_history_for_date(
            session, csrf_token, target_date
        )
        if csv_text is None:
            data_fetcher.logger.debug(f"No data for {target_date} (holiday/weekend).")
        else:
            output_path.write_text(csv_text, encoding="utf-8")

        time.sleep(sleep_seconds)


def main():
    args = parse_args()
    fetch_history(args.start_date, args.end_date, args.sleep_seconds)


if __name__ == "__main__":
    main()

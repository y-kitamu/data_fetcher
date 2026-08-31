"""fetch_data_from_taisyaku.py
日証金（taisyaku.jp）から最新の銘柄別信用残高一覧を取得する

同じ申込日のデータでも「速報値」（毎日18時半頃発表）と「確報値」（毎日11時頃発表）の
2種類が存在するため、実行時刻ではなく取得したCSV自体に含まれる「速報／確報」列から
状態を判定して別ファイルに保存する。これにより実行タイミングに関わらず、
また1日に複数回実行しても正しく両方を保存できる。
"""

import csv
import datetime
import io

import data_fetcher

MIN_EXPECTED_ROWS = 1000

STATUS_SUFFIX = {
    "速報": "sokuho",
    "確報": "kakuho",
}


def get_trade_date_and_status(csv_text: str) -> tuple[str, str]:
    row = next(csv.DictReader(io.StringIO(csv_text)), None)
    if row is None:
        raise ValueError("zandaka.csv has no data rows.")
    trade_date = datetime.datetime.strptime(row["申込日"], "%Y/%m/%d").strftime(
        "%Y%m%d"
    )
    return trade_date, row["速報／確報"]


def update_zandaka_csv():
    session = data_fetcher.get_session(cache_file=None)
    csv_text = data_fetcher.domains.taisyaku.data.fetch_zandaka_csv(session)

    row_count = csv_text.count("\n")
    if row_count < MIN_EXPECTED_ROWS:
        data_fetcher.logger.warning(
            f"Unexpected zandaka.csv row count ({row_count}). Skip saving."
        )
        return

    trade_date, status = get_trade_date_and_status(csv_text)
    suffix = STATUS_SUFFIX.get(status)
    if suffix is None:
        data_fetcher.logger.warning(f"Unknown status '{status}' in zandaka.csv.")
        suffix = status

    output_path = (
        data_fetcher.constants.PROJECT_ROOT
        / f"data/taisyaku/zandaka/{trade_date}_{suffix}.csv"
    )
    output_path.parent.mkdir(exist_ok=True, parents=True)
    output_path.write_text(csv_text, encoding="utf-8")
    data_fetcher.logger.info(
        f"Saved {output_path} (trade_date={trade_date}, status={status}, {row_count} rows)"
    )


def main():
    update_zandaka_csv()


if __name__ == "__main__":
    main()

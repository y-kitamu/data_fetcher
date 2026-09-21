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
from pathlib import Path

import data_fetcher
from data_fetcher.domains.taisyaku.data import TRJO_KBN_BY_MARKET

MIN_EXPECTED_ROWS = 1000

STATUS_SUFFIX = {
    "速報": "sokuho",
    "確報": "kakuho",
}

ZANDAKA_DIR = data_fetcher.constants.PROJECT_ROOT / "data/taisyaku/zandaka"
HISTORY_BY_TICKER_DIR = (
    data_fetcher.constants.PROJECT_ROOT / "data/taisyaku/history_by_ticker"
)

# zandaka.csv の「取引所区分名」から history_by_ticker 側の「市場区分」
# （＝TRJO_KBN_BY_MARKET のキー）への正規化。
EXCHANGE_NAME_TO_MARKET = {
    "東証およびＰＴＳ": "東証",
    "名証": "名証",
    "福証": "福証",
    "札証": "札証",
}

# history_by_ticker/*.csv の既存カラム順（fetch_taisyaku_ticker_history.py が
# 銘柄詳細検索CSVからそのまま書き出しているもの）。zandaka由来では埋められない
# カラムは空欄のまま追記する。
HISTORY_FIELDNAMES = [
    "銘柄コード",
    "銘柄名",
    "直後基準日",
    "直近制限措置",
    "直近臨時措置",
    "直近特別措置",
    "申込日",
    "市場区分",
    "貸借区分",
    "融資新規（株）",
    "融資返済（株）",
    "融資残高（株）",
    "貸株新規（株）",
    "貸株返済（株）",
    "貸株残高（株）",
    "差引残高（株）",
    "貸借値段（円）",
    "品貸料率（品貸日数分/円）",
    "品貸日数",
    "品貸料率（年率換算/％）",
    "最高料率（品貸日数分/円）",
    "最低料率（品貸日数分/円）",
    "応札ランク",
    "制限措置",
    "臨時措置",
    "特別措置",
    "新株引受・権利入札",
]


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

    output_path = ZANDAKA_DIR / f"{trade_date}_{suffix}.csv"
    output_path.parent.mkdir(exist_ok=True, parents=True)
    output_path.write_text(csv_text, encoding="utf-8")
    data_fetcher.logger.info(
        f"Saved {output_path} (trade_date={trade_date}, status={status}, {row_count} rows)"
    )


def _read_latest_date(path: Path) -> str:
    """history_by_ticker/*.csv に記録済みの最新の申込日（YYYYMMDD）を返す。未作成なら空文字。"""
    if not path.exists():
        return ""
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return ""
    return max(row["申込日"] for row in rows)


def _zandaka_row_to_history_row(
    zandaka_row: dict[str, str], market: str, trade_date: str
) -> dict[str, str]:
    history_row = dict.fromkeys(HISTORY_FIELDNAMES, "")
    history_row.update(
        {
            "銘柄コード": zandaka_row["銘柄コード"],
            "銘柄名": zandaka_row["銘柄名"],
            "申込日": trade_date,
            "市場区分": market,
            "融資新規（株）": zandaka_row["融資新規株数"],
            "融資返済（株）": zandaka_row["融資返済株数"],
            "融資残高（株）": zandaka_row["融資残高株数"],
            "貸株新規（株）": zandaka_row["貸株新規株数"],
            "貸株返済（株）": zandaka_row["貸株返済株数"],
            "貸株残高（株）": zandaka_row["貸株残高株数"],
            "差引残高（株）": zandaka_row["差引残高株数"],
        }
    )
    return history_row


def _append_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    rows = sorted(rows, key=lambda row: row["申込日"])
    write_header = not output_path.exists()
    output_path.parent.mkdir(exist_ok=True, parents=True)
    with open(output_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HISTORY_FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def update_history_by_ticker_from_zandaka() -> None:
    """data/taisyaku/zandaka/*_kakuho.csv を全件スキャンし、各銘柄の history_by_ticker
    CSVに記録済みの最新日より後の分を追記する（未反映日の取りこぼしがあってもバックフィル
    される）。"""
    kakuho_paths = sorted(ZANDAKA_DIR.glob("*_kakuho.csv"))
    if not kakuho_paths:
        return

    latest_dates: dict[tuple[str, str], str] = {}
    pending_rows: dict[tuple[str, str], list[dict[str, str]]] = {}

    for path in kakuho_paths:
        with open(path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        for row in rows:
            market = EXCHANGE_NAME_TO_MARKET[row["取引所区分名"]]
            trjo_kbn = TRJO_KBN_BY_MARKET[market] if market else None
            if trjo_kbn is None:
                data_fetcher.logger.warning(
                    f"Unknown exchange '{row['取引所区分名']}' for "
                    f"{row['銘柄コード']}. Skip."
                )
                continue

            key = (row["銘柄コード"], trjo_kbn)
            trade_date = row["申込日"].replace("/", "")

            if key not in latest_dates:
                output_path = HISTORY_BY_TICKER_DIR / f"{key[0]}-{key[1]}.csv"
                latest_dates[key] = _read_latest_date(output_path)

            if trade_date <= latest_dates[key]:
                continue

            pending_rows.setdefault(key, []).append(
                _zandaka_row_to_history_row(row, market, trade_date)
            )
            latest_dates[key] = trade_date

    for key, rows in pending_rows.items():
        _append_rows(HISTORY_BY_TICKER_DIR / f"{key[0]}-{key[1]}.csv", rows)

    data_fetcher.logger.info(
        f"Updated history_by_ticker for {len(pending_rows)} ticker(s) "
        f"from {len(kakuho_paths)} zandaka kakuho file(s)."
    )


def main():
    update_zandaka_csv()
    update_history_by_ticker_from_zandaka()


if __name__ == "__main__":
    main()

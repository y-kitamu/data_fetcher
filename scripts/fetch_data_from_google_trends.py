"""fetch_data_from_google_trends.py
Google Trends（pytrends、非公式）から銘柄の検索関心度の推移を取得する。

pytrendsは非公式ライブラリでリクエスト頻度が高いと一時的にブロックされるため、
まずTOPIX Core30程度の少数銘柄でプロトタイプとして実行し、安定性・実行時間を
確認してから対象銘柄を広げる（このスクリプトではCore30以上には広げない）。

銘柄コード単体では検索キーワードとして曖昧すぎるため、企業名で検索する。

`interest`（検索関心度）は取得時のリクエスト期間内での相対値であり、同じ日付でも
取得タイミングが変わると値が変わる。そのため保存は上書きではなく追記
（`core.append_and_save_csv`）で行い、`fetched_at`/`window_start`/`window_end` を
含めて全て蓄積する（過去に保存済みの値を書き換えない）。

`--backfill-daily` で過去10年分を日次粒度のまま遡って取得できる。Google Trendsの粒度は
「1回のクエリで指定する期間の長さ」で決まる（270日以内なら日次）ため、過去の期間でも
270日以内のウィンドウを明示的に指定すれば日次粒度のまま取得可能
（`domains.google_trends.api.generate_daily_windows` 参照）。ウィンドウ同士を1ヶ月重複させて
いるのは、後日リスケールして1本の連続系列に繋ぎ合わせる際のアンカーにするため
（リスケール処理自体は生データ収集フェーズのスコープ外）。
Core30 x 過去10年で約720リクエストになり数時間かかるため、`_backfill_log.csv` に
成功したウィンドウを記録し、中断・再開に対応している。
"""

import argparse
import csv
import datetime
import time

import polars as pl

import data_fetcher
from data_fetcher.core.retry import retry_with_backoff
from data_fetcher.domains.google_trends import api as trends_api

# TOPIX Core30相当の主要銘柄（プロトタイプ用。定期的な見直しが必要）
CORE30_TICKERS = [
    ("7203", "トヨタ自動車"),
    ("9984", "ソフトバンクグループ"),
    ("6758", "ソニーグループ"),
    ("8306", "三菱UFJフィナンシャル・グループ"),
    ("9432", "日本電信電話"),
    ("6861", "キーエンス"),
    ("4063", "信越化学工業"),
    ("6098", "リクルートホールディングス"),
    ("8058", "三菱商事"),
    ("9433", "KDDI"),
    ("6501", "日立製作所"),
    ("8035", "東京エレクトロン"),
    ("7267", "本田技研工業"),
    ("6367", "ダイキン工業"),
    ("8031", "三井物産"),
    ("4519", "中外製薬"),
    ("6902", "デンソー"),
    ("4568", "第一三共"),
    ("8801", "三井不動産"),
    ("9020", "東日本旅客鉄道"),
    ("8316", "三井住友フィナンシャルグループ"),
    ("6981", "村田製作所"),
    ("7741", "HOYA"),
    ("8411", "みずほフィナンシャルグループ"),
    ("9022", "東海旅客鉄道"),
    ("4661", "オリエンタルランド"),
    ("6273", "SMC"),
    ("8267", "イオン"),
    ("6702", "富士通"),
    ("7182", "ゆうちょ銀行"),
]

REQUEST_INTERVAL_SECONDS = 15.0
OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/google_trends"

BACKFILL_YEARS_BACK = 10
BACKFILL_WINDOW_MONTHS = 6
BACKFILL_OVERLAP_MONTHS = 1
BACKFILL_REQUEST_INTERVAL_SECONDS = 20.0
BACKFILL_LOG_PATH = OUTPUT_DIR / "_backfill_log.csv"


@retry_with_backoff(max_retries=3, base_delay=60.0, exceptions=(Exception,))
def _fetch_one(pytrends, company_name: str) -> pl.DataFrame:
    return trends_api.fetch_interest_over_time(pytrends, company_name)


@retry_with_backoff(max_retries=5, base_delay=60.0, exceptions=(Exception,))
def _fetch_range(
    pytrends, company_name: str, start_date: datetime.date, end_date: datetime.date
) -> pl.DataFrame:
    return trends_api.fetch_interest_over_time_for_range(
        pytrends, company_name, start_date, end_date
    )


def update_google_trends(tickers: list[tuple[str, str]] = CORE30_TICKERS) -> None:
    pytrends = trends_api.get_trend_request()

    for i, (code, company_name) in enumerate(tickers):
        if i > 0:
            time.sleep(REQUEST_INTERVAL_SECONDS)

        try:
            df = _fetch_one(pytrends, company_name)
        except Exception as e:
            data_fetcher.logger.error(f"Failed to fetch trends for {code} ({company_name}): {e}")
            continue

        if df.height == 0:
            data_fetcher.logger.warning(f"No trend data for {code} ({company_name}).")
            continue

        df = df.with_columns(pl.lit(code).alias("code"))
        output_path = OUTPUT_DIR / f"{code}.csv"
        data_fetcher.append_and_save_csv(df, output_path, sort_col="date")
        dates = df["date"]
        data_fetcher.logger.info(
            f"Fetched {code} ({company_name}): {df.height} new rows, "
            f"window={dates.min()}~{dates.max()}, saved to {output_path}"
        )


def _load_backfill_log() -> set[tuple[str, str, str]]:
    if not BACKFILL_LOG_PATH.exists():
        return set()
    with open(BACKFILL_LOG_PATH, encoding="utf-8") as f:
        return {(row[0], row[1], row[2]) for row in csv.reader(f) if len(row) >= 3}


def _mark_backfill_done(code: str, window_start: str, window_end: str) -> None:
    BACKFILL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BACKFILL_LOG_PATH, "a", encoding="utf-8") as f:
        csv.writer(f, lineterminator="\n").writerow([code, window_start, window_end])


def backfill_daily_history(
    tickers: list[tuple[str, str]] = CORE30_TICKERS,
    years_back: int = BACKFILL_YEARS_BACK,
) -> None:
    """過去 years_back 年分、重複ウィンドウで日次粒度の生データを蓄積する。

    中断・再開に対応するため、成功したウィンドウは _backfill_log.csv に記録し、
    再実行時は未処理分のみ処理する。
    """
    pytrends = trends_api.get_trend_request()
    done = _load_backfill_log()
    today = datetime.date.today()

    plan = [
        (code, company_name, window_start, window_end)
        for code, company_name in tickers
        for window_start, window_end in trends_api.generate_daily_windows(
            today, years_back, BACKFILL_WINDOW_MONTHS, BACKFILL_OVERLAP_MONTHS
        )
    ]
    remaining = [
        item
        for item in plan
        if (item[0], item[2].isoformat(), item[3].isoformat()) not in done
    ]
    data_fetcher.logger.info(
        f"Backfill plan: {len(plan)} window(s) total, "
        f"{len(plan) - len(remaining)} already done, {len(remaining)} to fetch."
    )

    for i, (code, company_name, window_start, window_end) in enumerate(remaining):
        if i > 0:
            time.sleep(BACKFILL_REQUEST_INTERVAL_SECONDS)

        try:
            df = _fetch_range(pytrends, company_name, window_start, window_end)
        except Exception as e:
            data_fetcher.logger.error(
                f"Failed {code} ({company_name}) {window_start}~{window_end}: {e}"
            )
            continue

        if df.height > 0:
            df = df.with_columns(pl.lit(code).alias("code"))
            output_path = OUTPUT_DIR / f"{code}.csv"
            data_fetcher.append_and_save_csv(df, output_path, sort_col="date")
        else:
            data_fetcher.logger.warning(
                f"No data for {code} ({company_name}) {window_start}~{window_end}."
            )

        _mark_backfill_done(code, window_start.isoformat(), window_end.isoformat())

        if (i + 1) % 20 == 0:
            data_fetcher.logger.info(f"Progress: {i + 1}/{len(remaining)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backfill-daily",
        action="store_true",
        help="過去10年分を日次粒度の重複ウィンドウで取得する（数時間かかる）",
    )
    args = parser.parse_args()

    if args.backfill_daily:
        backfill_daily_history()
        return

    update_google_trends()


if __name__ == "__main__":
    main()

"""fetch_data_from_jquants.py
J-Quants API（無料プラン）から日本株の銘柄マスタ・株価四本値・財務情報・決算発表予定日を取得する。
ただし、無料プランは5リクエスト/分の制約があるため、実質的に全データの収集は不可能。
（J-Quants以外のデータソースから代替取得可能なため問題ない）

無料プランの制約:
- 直近12週間分のデータは取得できない
- そこから遡って約2年分のみ取得可能
- => バックテスト用の過去データ収集専用。当日〜直近のライブ運用には使えない
- 投資部門別売買状況・信用取引週末残高・空売り関連データは無料プランに含まれないため未実装
  （有料プラン移行時は data_fetcher.domains.jquants.api の _request/_fetch_all_pages を再利用して追加する）

株価四本値 (daily_quotes) は全銘柄×2年分で数百万行規模になるため data/jquants/daily_quotes/ は
.gitignore 対象。銘柄マスタ・財務情報・決算発表予定日は比較的小さいためGit管理下に置く。
"""

import argparse
from pathlib import Path

import polars as pl
import tqdm

import data_fetcher
from data_fetcher.domains.jquants import api as jquants_api

JQUANTS_DATA_DIR = data_fetcher.constants.PROJECT_ROOT / "data/jquants"


def _save_rows(
    rows: list[dict], output_path: Path, sort_col: str | None = None
) -> None:
    if not rows:
        data_fetcher.logger.warning(f"No rows to save for {output_path}.")
        return

    rows = [{k: str(v) if v is not None else "" for k, v in r.items()} for r in rows]
    # infer_schema_length=0 は read_csv と異なり全列をUtf8に固定しないため指定しない（既に全値をstr化済み）
    df = pl.from_dicts(rows)
    if output_path.exists():
        old_df = pl.read_csv(output_path, infer_schema_length=0)
        df = pl.concat([old_df, df]).unique()
    if sort_col:
        df = df.sort(sort_col)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)


def update_listed_info(date: str | None = None) -> None:
    session = data_fetcher.get_session(max_requests_per_second=1)
    api_key = jquants_api.load_api_key()
    rows = jquants_api.get_listed_info(session, api_key, date=date)
    if not rows:
        data_fetcher.logger.warning("No listed_info rows returned.")
        return
    target_date = rows[0]["Date"].replace("-", "")
    output_path = JQUANTS_DATA_DIR / f"listed_info/{target_date}.csv"
    _save_rows(rows, output_path)
    data_fetcher.logger.info(
        f"Saved {output_path} ({len(rows)} rows, date={target_date})"
    )


def update_daily_quotes(codes: list[str]) -> None:
    session = data_fetcher.get_session(max_requests_per_second=1)
    api_key = jquants_api.load_api_key()
    for code in tqdm.tqdm(codes):
        rows = jquants_api.get_daily_quotes(session, api_key, code=code)
        if not rows:
            data_fetcher.logger.warning(f"No daily_quotes rows for code={code}.")
            continue
        output_path = JQUANTS_DATA_DIR / f"daily_quotes/{code}.csv"
        _save_rows(rows, output_path, sort_col="Date")
        dates = sorted(r["Date"] for r in rows)
        data_fetcher.logger.info(
            f"Saved {output_path} ({len(rows)} rows, {dates[0]}~{dates[-1]})"
        )


def update_statements(codes: list[str]) -> None:
    session = data_fetcher.get_session(max_requests_per_second=1)
    api_key = jquants_api.load_api_key()
    for code in tqdm.tqdm(codes):
        rows = jquants_api.get_statements(session, api_key, code=code)
        if not rows:
            data_fetcher.logger.warning(f"No statements rows for code={code}.")
            continue
        output_path = JQUANTS_DATA_DIR / f"statements/{code}.csv"
        _save_rows(rows, output_path)
        data_fetcher.logger.info(f"Saved {output_path} ({len(rows)} rows)")


def update_earnings_dates(codes: list[str]) -> None:
    session = data_fetcher.get_session(max_requests_per_second=1)
    api_key = jquants_api.load_api_key()
    for code in tqdm.tqdm(codes):
        rows = jquants_api.get_earnings_dates(session, api_key, code=code)
        if not rows:
            data_fetcher.logger.warning(f"No earnings_dates rows for code={code}.")
            continue
        output_path = JQUANTS_DATA_DIR / f"earnings_dates/{code}.csv"
        _save_rows(rows, output_path)
        data_fetcher.logger.info(f"Saved {output_path} ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codes",
        nargs="*",
        default=None,
        help="対象銘柄コード（指定しない場合は data/jp_tickers.csv の全内国株式）",
    )
    args = parser.parse_args()

    codes = args.codes or data_fetcher.core.get_jp_ticker_list()

    update_listed_info()
    update_daily_quotes(codes)
    update_statements(codes)
    update_earnings_dates(codes)


if __name__ == "__main__":
    main()

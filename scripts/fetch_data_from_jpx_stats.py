"""fetch_data_from_jpx_stats.py
JPX統計情報ページ（登録不要）から、投資部門別売買状況・個別銘柄信用取引残高表
（日々公表銘柄）を取得する。

- 投資部門別売買状況: 週次更新。市場区分(プライム/スタンダード/グロース/二市場)ごとに
  金額(value)・株数(volume)の2ファイルが公開される。2022年4月4日の東証市場区分再編前後で
  シート名（市場区分）が変わるが、パーサー側で自動判定する（domains.jpx_stats.parser 参照）。
  年別アーカイブページ（archives-00〜10）から過去10年分の取得も可能
  （`python scripts/fetch_data_from_jpx_stats.py --backfill-investor-type`）。
- 個別銘柄信用取引残高表（日々公表銘柄）: 日次更新。ただし信用規制等により日々の開示が
  義務付けられた銘柄のみのサブセットであり、全銘柄の週末残高ではない
  （全銘柄分は fetch_data_from_taisyaku.py が既に取得している）

一覧ページのダウンロードリンクはハッシュ付きディレクトリ配下にあり、URLを日付から
直接組み立てることができないため、毎回一覧ページをスクレイピングして最新リンクを特定する。
"""

import argparse

import data_fetcher
from data_fetcher.domains.jpx_stats import api as jpx_stats_api
from data_fetcher.domains.jpx_stats import parser as jpx_stats_parser

JPX_STATS_DATA_DIR = data_fetcher.constants.PROJECT_ROOT / "data/jpx_stats"
INVESTOR_TYPE_DOWNLOADED_LOG_PATH = (
    JPX_STATS_DATA_DIR / "investor_type/_downloaded_urls.txt"
)

MIN_EXPECTED_INVESTOR_TYPE_ROWS = 150  # 15カテゴリ x 3項目 x 4市場 = 180行が正常値
MIN_EXPECTED_MARGIN_ROWS = 50


def _load_downloaded_investor_type_urls() -> set[str]:
    if not INVESTOR_TYPE_DOWNLOADED_LOG_PATH.exists():
        return set()
    return set(INVESTOR_TYPE_DOWNLOADED_LOG_PATH.read_text().splitlines())


def _mark_investor_type_url_downloaded(value_url: str) -> None:
    INVESTOR_TYPE_DOWNLOADED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(INVESTOR_TYPE_DOWNLOADED_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(value_url + "\n")


def _save_investor_type(val_content: bytes, vol_content: bytes) -> bool:
    """金額・株数ファイルをパースして保存する。保存できた場合 True を返す。"""
    val_df = jpx_stats_parser.parse_investor_type_workbook(val_content, metric="value")
    vol_df = jpx_stats_parser.parse_investor_type_workbook(vol_content, metric="volume")
    df = val_df.vstack(vol_df)

    if df.height < MIN_EXPECTED_INVESTOR_TYPE_ROWS:
        data_fetcher.logger.warning(
            f"Unexpected investor_type row count ({df.height}). Skip saving."
        )
        return False

    week_start = df["week_start"][0]
    week_end = df["week_end"][0]
    output_path = (
        JPX_STATS_DATA_DIR
        / f"investor_type/{week_start.replace('-', '')}_{week_end.replace('-', '')}.csv"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)
    data_fetcher.logger.info(
        f"Saved {output_path} ({df.height} rows, week={week_start}~{week_end})"
    )
    return True


def update_investor_type() -> None:
    session = data_fetcher.get_session(max_requests_per_second=1)
    urls = jpx_stats_api.get_latest_investor_type_urls(session)
    val_content = jpx_stats_api.download(session, urls["value_url"])
    vol_content = jpx_stats_api.download(session, urls["volume_url"])
    _save_investor_type(val_content, vol_content)


def backfill_investor_type_history() -> None:
    """投資部門別売買状況の年別アーカイブページ（当年+過去10年分）から全週分を取得する。

    処理済みのvalue_urlを `_downloaded_urls.txt` に記録し、再実行時は未取得分のみ処理する
    （長時間かかる処理のため、中断・再開が安全にできるようにしている）。
    """
    session = data_fetcher.get_session(max_requests_per_second=1)
    downloaded = _load_downloaded_investor_type_urls()

    all_pairs: list[dict[str, str]] = []
    for page_url in jpx_stats_api.get_investor_type_archive_page_urls():
        pairs = jpx_stats_api.get_investor_type_urls_from_page(session, page_url)
        data_fetcher.logger.info(f"{page_url}: {len(pairs)} week(s) found.")
        all_pairs.extend(pairs)

    new_pairs = [p for p in all_pairs if p["value_url"] not in downloaded]
    data_fetcher.logger.info(
        f"Total {len(all_pairs)} week(s) found across all archive pages, "
        f"{len(all_pairs) - len(new_pairs)} already downloaded, {len(new_pairs)} to fetch."
    )

    for i, pair in enumerate(new_pairs, start=1):
        try:
            val_content = jpx_stats_api.download(session, pair["value_url"])
            vol_content = jpx_stats_api.download(session, pair["volume_url"])
            _save_investor_type(val_content, vol_content)
        except Exception as e:
            data_fetcher.logger.error(f"Failed to fetch/parse {pair['value_url']}: {e}")
            continue
        _mark_investor_type_url_downloaded(pair["value_url"])
        if i % 20 == 0:
            data_fetcher.logger.info(f"Progress: {i}/{len(new_pairs)}")


def update_margin() -> None:
    session = data_fetcher.get_session(max_requests_per_second=1)
    url = jpx_stats_api.get_latest_margin_url(session)
    content = jpx_stats_api.download(session, url)
    df = jpx_stats_parser.parse_margin_workbook(content)

    if df.height < MIN_EXPECTED_MARGIN_ROWS:
        data_fetcher.logger.warning(
            f"Unexpected margin row count ({df.height}). Skip saving."
        )
        return

    report_date = df["report_date"][0].replace("-", "")
    output_path = JPX_STATS_DATA_DIR / f"margin_daily_disclosure/{report_date}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)
    data_fetcher.logger.info(
        f"Saved {output_path} ({df.height} rows, report_date={report_date})"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backfill-investor-type",
        action="store_true",
        help="投資部門別売買状況の過去10年分（年別アーカイブページ）を取得する",
    )
    args = parser.parse_args()

    if args.backfill_investor_type:
        backfill_investor_type_history()
        return

    update_investor_type()
    update_margin()


if __name__ == "__main__":
    main()

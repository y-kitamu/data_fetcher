"""fetch_data_from_estat.py
e-Stat（政府統計の総合窓口）APIから日本のマクロ経済指標を定期取得する。

無料のappId登録のみで利用可能（cert/estat_appid.txtに配置済み）。1テーブルが
数千〜数十万行になりうるため、SERIES_CATALOGの`cd_filters`で単一系列まで
絞り込んでから取得し、`assert_single_series`で絞り込み漏れ（複数系列混在）を
検知する。時間軸は`CLASS_INF`の`time`次元の`@code -> @name`マップ経由で
`time_label_to_date`によりISO日付に変換する。

値の改定履歴を残すためfetched_at列を付けた上で
append_and_save_csv(..., dedup_subset=["date", "value"])で「値が変わった時だけ
追記する」（値が変わっていない再取得では行が増えない）。

SERIES_CATALOGは実際にgetStatsDataを叩いて「絞り込み後のTOTAL_NUMBERが
時間軸の総数と一致する（＝単一系列になっている）」ことを確認済みの7系列:
  machinery_orders_total: 機械受注統計調査(月次・合計)
  cli_ci_leading/coincident/lagging: 景気動向指数 先行/一致/遅行指数(CI)
  cpi_national_all_items: 消費者物価指数(全国・総合、2020年基準)
  new_housing_starts_total: 新設住宅着工戸数(総数、年次)
  job_openings_new: 新規求人数(除学卒、景気動向指数の個別系列より)

未特定（実装時に別名称で再探索し、見つかればカタログに追加する予定）:
  鉱工業生産指数の最新基準年版テーブルID

動作確認済み: 2026-09-26に実行し、data/estat/配下に7系列分のCSVが
生成されることを確認。
"""

import argparse
import datetime

import polars as pl

import data_fetcher
from data_fetcher.domains.estat import api as estat_api

OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/estat"

SERIES_CATALOG = [
    {
        "series_id": "machinery_orders_total",
        "stats_data_id": "0003355268",
        "cd_filters": {"cdTab": "100", "cdCat01": "100", "cdCat02": "100"},
        "dimension_keys": ["@tab", "@cat01", "@cat02"],
    },
    {
        "series_id": "cli_ci_leading",
        "stats_data_id": "0003446461",
        "cd_filters": {"cdTab": "100", "cdCat01": "100"},
        "dimension_keys": ["@tab", "@cat01"],
    },
    {
        "series_id": "cli_ci_coincident",
        "stats_data_id": "0003446461",
        "cd_filters": {"cdTab": "100", "cdCat01": "110"},
        "dimension_keys": ["@tab", "@cat01"],
    },
    {
        "series_id": "cli_ci_lagging",
        "stats_data_id": "0003446461",
        "cd_filters": {"cdTab": "100", "cdCat01": "120"},
        "dimension_keys": ["@tab", "@cat01"],
    },
    {
        "series_id": "cpi_national_all_items",
        "stats_data_id": "0003427113",
        "cd_filters": {"cdTab": "1", "cdCat01": "0001", "cdArea": "00000"},
        "dimension_keys": ["@tab", "@cat01", "@area"],
    },
    {
        "series_id": "new_housing_starts_total",
        "stats_data_id": "0003114502",
        "cd_filters": {"cdTab": "18", "cdCat01": "11", "cdCat02": "11", "cdCat03": "12"},
        "dimension_keys": ["@tab", "@cat01", "@cat02", "@cat03"],
    },
    {
        "series_id": "job_openings_new",
        "stats_data_id": "0003446462",
        "cd_filters": {"cdTab": "200", "cdCat01": "1030"},
        "dimension_keys": ["@tab", "@cat01"],
    },
]


def _build_dataframe(values: list[dict], time_label_map: dict[str, str]) -> pl.DataFrame:
    """`values`を`date, value`のDataFrameに変換する（1日付=1行に正規化する）。

    一部のテーブル（例: 消費者物価指数）は時間軸に月次と会計年度("2025年度")の
    集計行が混在しており、`time_label_to_date`でパースできない（実測で確認済み）。
    月次系列として一貫させるため、パースできない行（月次以外の粒度）はスキップする。

    さらに消費者物価指数の古い年代（実測で1970年代〜、52件）では、同じ月ラベルを
    持つ`@time`コードが2つ存在し、異なる値を返すことを確認済み（旧基準年からの
    リンク処理由来と推測されるが詳細な原因は未特定）。API呼び出しごとに順序が
    入れ替わりうるため、そのまま先勝ちで採用すると再取得のたびに値が変わって見え、
    改定履歴が意図せず増殖してしまう。順序に依存しない決定的な選択として、
    同一日付内では数値が大きい方を採用する（経済的な正しさの保証ではなく、
    再取得での再現性を優先した実務上の割り切り）。
    """
    by_date: dict[str, list[str]] = {}
    skipped_unparseable = 0
    for v in values:
        label = time_label_map.get(v["@time"], "")
        try:
            date = estat_api.time_label_to_date(label)
        except ValueError:
            skipped_unparseable += 1
            continue
        by_date.setdefault(date, []).append(v.get("$"))

    if skipped_unparseable:
        data_fetcher.logger.debug(
            f"Skipped {skipped_unparseable} row(s) with unparseable time label "
            "(likely a non-monthly aggregate such as a fiscal year)."
        )

    duplicate_dates = [d for d, vs in by_date.items() if len(vs) > 1]
    if duplicate_dates:
        data_fetcher.logger.debug(
            f"{len(duplicate_dates)} date(s) had multiple raw values under the same "
            "time label; deterministically keeping the larger value."
        )

    dates = sorted(by_date.keys())
    series_values = [
        max(
            by_date[d],
            key=lambda s: float(s) if s not in (None, "") else float("-inf"),
        )
        for d in dates
    ]
    return pl.DataFrame({"date": dates, "value": series_values})


def update_estat(catalog: list[dict] = SERIES_CATALOG) -> None:
    session = data_fetcher.get_session(max_requests_per_second=2, cache_file=None)
    app_id = estat_api.load_app_id()
    fetched_at = datetime.date.today().isoformat()

    for entry in catalog:
        try:
            result = estat_api.get_stats_data(
                session, app_id, entry["stats_data_id"], **entry["cd_filters"]
            )
            estat_api.assert_single_series(result["values"], entry["dimension_keys"])
            time_label_map = estat_api.build_time_label_map(result["class_inf"])
            df = _build_dataframe(result["values"], time_label_map)
        except Exception as e:
            data_fetcher.logger.error(
                f"Failed to fetch e-Stat series {entry['series_id']}: {e}"
            )
            continue

        if df.height == 0:
            data_fetcher.logger.warning(f"No observations for {entry['series_id']}.")
            continue

        df = df.with_columns(pl.lit(fetched_at).alias("fetched_at"))
        output_path = OUTPUT_DIR / f"{entry['series_id']}.csv"
        data_fetcher.append_and_save_csv(
            df, output_path, sort_col="date", dedup_subset=["date", "value"]
        )
        data_fetcher.logger.info(
            f"Fetched {entry['series_id']}: {df.height} rows, saved to {output_path}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    update_estat()


if __name__ == "__main__":
    main()

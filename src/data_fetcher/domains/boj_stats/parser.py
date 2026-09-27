"""parser.py - 日銀時系列統計データ検索サイトのbulk CSVのパース処理

企業物価指数(cgpi_m_jp.csv)と短観(co.csv)は形式が異なる（実データを取得して確認済み）:
- cgpi_m_jp.csv: Shift_JIS。1行目がヘッダー行で、先頭3列は空、4列目以降が年月ラベル
  (`202001`,`202002`,...)。2行目以降が`系列コード,系列名,系列ラベル,値...`のwide形式。
- co.csv（短観）: Shift_JIS。ヘッダー行なし。`系列コード,頻度(Q),年月(YYYYMM),値`の
  4列で最初から縦持ち(long形式)。

どちらも様式を決め打ちしてパースし、様式変更時は呼び出し側の件数チェックで検知する
（`domains.jpx_stats.parser`と同じ方針）。
"""

import io

import polars as pl


def decode_shift_jis(raw: bytes) -> str:
    return raw.decode("shift_jis")


def _period_to_date(period_col: str) -> pl.Expr:
    """`"202001"`のような6桁の年月文字列を`"2020-01-01"`に変換する式を返す。"""
    return (
        pl.col(period_col).str.slice(0, 4)
        + "-"
        + pl.col(period_col).str.slice(4, 2)
        + "-01"
    )


def parse_wide_csv(text: str) -> pl.DataFrame:
    """企業物価指数のようなwide形式CSVを`series_code, date, value`のlong形式に変換する。

    1行目（先頭3列が空、4列目以降が年月ラベル）をヘッダーとして使い、
    2行目以降の`系列コード,系列名,系列ラベル`をメタ列、それ以降を年月ごとの値として
    unpivotする。系列名・系列ラベルは絞り込みに使わず、全系列をそのまま対象にする。
    """
    raw = pl.read_csv(io.StringIO(text), has_header=False, infer_schema_length=0)
    columns = raw.columns  # ["column_1", "column_2", ...] (has_header=False の既定名)
    period_labels = raw.row(0)[3:]

    data = raw.slice(1)  # 2行目以降が実データ
    rename_map = {
        columns[0]: "series_code",
        columns[1]: "series_name",
        columns[2]: "series_label",
    }
    rename_map.update(dict(zip(columns[3:], period_labels)))
    data = data.rename(rename_map)

    long_df = data.unpivot(
        index=["series_code", "series_name", "series_label"],
        variable_name="period",
        value_name="value",
    )
    long_df = long_df.with_columns(_period_to_date("period").alias("date")).with_columns(
        pl.when(pl.col("value") == "").then(None).otherwise(pl.col("value")).alias("value")
    )
    return long_df.select(["series_code", "date", "value"])


def parse_tankan_csv(text: str) -> pl.DataFrame:
    """短観のヘッダーなしlong形式CSVを`series_code, date, value`に整形する。

    3列目の年月(YYYYMM)を`date`に変換する。2列目の頻度（全行"Q"=四半期）は
    情報量が無いため保持しない。
    """
    raw = pl.read_csv(
        io.StringIO(text),
        has_header=False,
        infer_schema_length=0,
        new_columns=["series_code", "frequency", "period", "value"],
    )
    return raw.with_columns(_period_to_date("period").alias("date")).select(
        ["series_code", "date", "value"]
    )

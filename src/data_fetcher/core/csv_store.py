"""csv_store.py - 既存CSVへの追記保存を共通化するヘルパー

複数の取得元（J-Quants, Google Trends 等）で「既存CSVを読み込み、新規データと結合し、
完全一致する行だけ重複排除して書き戻す」という蓄積パターンが必要になるため、ここに集約する。
"""

from pathlib import Path

import polars as pl
from loguru import logger


def append_and_save_csv(
    df: pl.DataFrame, output_path: Path, sort_col: str | None = None
) -> None:
    """新規データを既存CSVに追記して保存する（完全一致する行のみ重複排除）。

    CSV往復で型情報が失われるため全列をUtf8にキャストしてから結合する
    （既存データと新規データで推論された型が食い違うことによるエラーを避けるため）。
    値そのものが異なる行（例: 取得タイミングにより値が変わり得るGoogle Trendsの
    `interest`）は重複排除されず、別行として残る。
    """
    if df.height == 0:
        logger.warning(f"No rows to save for {output_path}.")
        return

    df = df.select([pl.col(c).cast(pl.Utf8) for c in df.columns])
    if output_path.exists():
        old_df = pl.read_csv(output_path, infer_schema_length=0)
        df = pl.concat([old_df, df]).unique()
    if sort_col:
        df = df.sort(sort_col)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)

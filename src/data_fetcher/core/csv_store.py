"""csv_store.py - 既存CSVへの追記保存を共通化するヘルパー

複数の取得元（J-Quants, Google Trends 等）で「既存CSVを読み込み、新規データと結合し、
完全一致する行だけ重複排除して書き戻す」という蓄積パターンが必要になるため、ここに集約する。
"""

from pathlib import Path

import polars as pl
from loguru import logger


def append_and_save_csv(
    df: pl.DataFrame,
    output_path: Path,
    sort_col: str | list[str] | None = None,
    dedup_subset: list[str] | None = None,
) -> None:
    """新規データを既存CSVに追記して保存する（重複排除ルールは`dedup_subset`で決まる）。

    `dedup_subset`を省略した場合は全列が完全一致する行のみ重複排除する（既存の挙動）。
    値そのものが異なる行（例: 取得タイミングにより値が変わり得るGoogle Trendsの
    `interest`）は重複排除されず、別行として残る。

    `fetched_at`のような取得時刻列を含むデータでは、値そのものが変わっていない限り
    再取得の記録を増やしたくないことが多い。その場合は`dedup_subset=["date", "value"]`
    のように値そのものの列だけを指定する（`fetched_at`は比較対象から除外され、値が同じ
    行は古い方=最初に記録されたfetched_atが残る。値が変わった行だけ、新しいfetched_at
    付きで別行として残る）。

    CSV往復で型情報が失われるため全列をUtf8にキャストしてから結合する
    （既存データと新規データで推論された型が食い違うことによるエラーを避けるため）。
    """
    if df.height == 0:
        logger.warning(f"No rows to save for {output_path}.")
        return

    df = df.select([pl.col(c).cast(pl.Utf8) for c in df.columns])
    if output_path.exists():
        old_df = pl.read_csv(output_path, infer_schema_length=0)
        df = pl.concat([old_df, df]).unique(subset=dedup_subset, keep="first")
    if sort_col:
        df = df.sort(sort_col)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)

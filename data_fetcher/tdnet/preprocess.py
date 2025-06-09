"""preprocess.py
CSVデータを加工して利用可能なDataFrameに変換する
"""

from pathlib import Path

import polars as pl

target_cols = [
    "filing_date",
    "code",
    "period_start",
    "period_end",
    "quarterly_period",
    "period_pos",
    "net_sales",
    "operating_profit",
    "ordinary_profit",
    "net_income",
    "net_income_of_parent",
    "eps",
    "diluted_eps",
    "is_forecast",
    "is_consolidated",
    "number_of_shares",
    "number_of_treqsury_shares",
]

merge_keys = [
    "period_start",
    "period_end",
    "quarterly_period",
    "period_pos",
    "net_sales",
    "operating_profit",
    "ordinary_profit",
    "net_income",
    # "eps",
    # "diluted_eps",
]
diff_keys = [
    "net_sales",
    "operating_profit",
    "ordinary_profit",
    "net_income",
    # "eps",
    # "diluted_eps",
]


def preprocess_csv(
    file_path: Path,
) -> tuple[
    pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame
]:
    """
    Args:
        file_path (Path): xbrlから抜き出したデータが格納されているCSVファイルのパス

    Returns:
        tuple: 連結年間業績, 1Q業績, 2Q業績, 3Q業績, 4Q業績, 年間業績予想のDataFrame
    """
    df = pl.read_csv(file_path, ignore_errors=True)
    # 必要なcolumnの型変換
    df = (
        df.filter(pl.col("filing_date").is_not_null())
        .select(target_cols)
        .with_columns(
            pl.col("filing_date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("period_start").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("period_end").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("period_pos").cast(pl.Int64),
            pl.col("quarterly_period").cast(pl.Int64),
            pl.col("net_sales").cast(pl.Int64),
            pl.col("operating_profit").cast(pl.Int64),
            pl.col("ordinary_profit").cast(pl.Int64),
            pl.col("net_income").cast(pl.Int64),
            pl.col("net_income_of_parent").cast(pl.Int64),
            pl.col("eps").cast(pl.Float64),
            pl.col("diluted_eps").cast(pl.Float64),
            pl.col("is_forecast").cast(pl.Boolean),
            pl.col("is_consolidated").cast(pl.Boolean),
            pl.col("number_of_shares").cast(pl.Int64),
            pl.col("number_of_treqsury_shares").cast(pl.Int64),
        )
        .filter(pl.col("period_pos") >= 0)
        .unique()
    )

    # 連結と非連結のデータを分ける
    cdf = df.filter(pl.col("is_consolidated")).with_columns(
        pl.col("net_income").fill_null(pl.col("net_income_of_parent")),
    )
    ncdf = df.filter(pl.col("is_consolidated") == False)

    # 連結と非連結のデータを結合してnull値を埋める
    if len(ncdf) > 0:
        cdf = cdf.join(
            ncdf.filter(
                (pl.col("is_forecast") == False)
                & (
                    pl.col("number_of_shares").is_not_null()
                    | pl.col("number_of_treqsury_shares").is_not_null()
                )
            ).select(
                pl.col("number_of_shares"),
                pl.col("number_of_treqsury_shares"),
                pl.col("filing_date"),
            ),
            on="filing_date",
            how="left",
        ).with_columns(
            pl.col("number_of_shares").fill_null(pl.col("number_of_shares_right")),
            pl.col("number_of_treqsury_shares").fill_null(
                pl.col("number_of_treqsury_shares_right")
            ),
        )

    # epsを現在の発行済株式総数で計算し直す
    cdf = cdf.sort(pl.col("filing_date")).with_columns(
        pl.col("eps").alias("raw_eps"),
        (pl.col("net_income") / pl.col("number_of_shares").last()).alias("eps"),
    )

    # null値を持つ行を削除
    cdf = cdf.filter(
        pl.col("net_sales").is_not_null()
        | pl.col("operating_profit").is_not_null()
        | pl.col("ordinary_profit").is_not_null()
        | pl.col("net_income").is_not_null()
    )
    # 実績・予想データを期間ごとに分離して抽出
    rdf = cdf.filter(pl.col("is_forecast") == False)  # 実績データ
    racc4df = rdf.filter(
        pl.col("period_end") - pl.col("period_start") > pl.duration(days=360)
    )  # 年間業績の実績
    racc1df = rdf.filter(pl.col("quarterly_period") == 1)  # 1Q業績
    racc2df = rdf.filter(pl.col("quarterly_period") == 2)  # 2Q業績
    racc3df = rdf.filter(pl.col("quarterly_period") == 3)  # 3Q業績
    fdf = cdf.filter(
        pl.col("is_forecast")
        & (pl.col("period_end") - pl.col("period_start") > pl.duration(days=360))
    )  # 年間業績の予想

    # 累積データを変換して四半期ごとのデータを作成
    tcols = target_cols + ["raw_eps"]
    rq1df = racc1df.select(*tcols)
    rq2df = create_quarter_df(racc2df, racc1df, tcols, merge_keys, "prev_")
    rq3df = create_quarter_df(racc3df, racc2df, tcols, merge_keys, "prev_")
    rq4df = create_quarter_df(racc4df, racc3df, tcols, merge_keys, "prev_")

    rydf = racc4df.select(*tcols)
    fdf = fdf.select(*tcols)

    return rydf, rq1df, rq2df, rq3df, rq4df, fdf


def create_quarter_df(
    target_df: pl.DataFrame,
    ref_df: pl.DataFrame,
    tcols: list[str],
    merge_keys: list[str] = merge_keys,
    prefix: str = "merged_",
) -> pl.DataFrame:
    ref_df = ref_df.select(*[pl.col(key).alias(f"{prefix}{key}") for key in merge_keys])
    merged_df = target_df.join(
        ref_df, how="full", left_on="period_start", right_on=f"{prefix}period_start"
    )
    merged_df = (
        merged_df.select(
            pl.col("period_end")
            .dt.offset_by("-2mo")
            .dt.month_start()
            .alias("period_start"),
            *list(set(tcols) - set(diff_keys + ["period_start"])),
            *[(pl.col(key) - pl.col(f"{prefix}{key}")).alias(key) for key in diff_keys],
        )
        .with_columns(
            (pl.col("net_income") / pl.col("number_of_shares").last()).alias("eps")
        )
        .select(tcols)
    )
    return merged_df

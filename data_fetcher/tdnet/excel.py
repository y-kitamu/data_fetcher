"""excel.py"""

from pathlib import Path

import polars as pl

from .constants import excel_path, forecast_labels, target_english_labels

target_sheets_jp = [
    "日本基準（通期・連結）",
    "日本基準（通期・非連結）",
    "日本基準（四半期・連結）",
    "日本基準（四半期・非連結）",
    "日本基準（一般２Ｑ・連結）",
    "日本基準（一般２Ｑ・非連結）",
    "日本基準（特定２Ｑ・連結）",
    "日本基準（特定２Ｑ・非連結）",
]
target_sheets_us = [
    "米国基準（通期・連結）",
    "米国基準（四半期・連結）",
    "米国基準（２Ｑ・連結）",
]
target_sheets_ifrs = [
    "IFRS（通期・連結）",
    "IFRS（四半期・連結）",
    "IFRS（一般２Ｑ・連結）",
    "IFRS（特定２Ｑ・連結）",
]
target_sheets_rvfc = [
    "業績予想の修正",
]
target_sheets_rvdf = [
    "配当予想の修正",
]


def _read_excel_sheets(xlsx_path: Path, target_sheets: list[str]) -> list[pl.DataFrame]:
    """Excelファイルの指定したシートを読み込んでpl.DataFrame形式で返す"""
    dfs = [
        pl.read_excel(
            xlsx_path, sheet_name=sheet, columns=[0, 1, 2, 3, 4, 15, 16, 17, 18, 19, 21]
        )
        for sheet in target_sheets
    ]
    dfs = [
        df.rename(
            {
                df.columns[0]: "legend",
                df.columns[1]: "label_jp",
                df.columns[2]: "label_jp_long",
                df.columns[3]: "label_en",
                df.columns[4]: "label_en_long",
                df.columns[5]: "namespace",
                df.columns[6]: "qname",
                df.columns[7]: "type",
                df.columns[8]: "substitutionGroup",
                df.columns[9]: "periodType",
                df.columns[10]: "abstract",
            }
        )
        .filter(pl.col("legend") == "●")
        .filter(pl.col("abstract") == "false")
        .with_columns((pl.col("namespace") + ":" + pl.col("qname")).alias("tag"))
        for df in dfs
    ]
    return dfs


def _get_target_labels(report_type) -> dict[str, str]:
    """報告書種別ごとに応じた対象の英語ラベルを取得する
    Return:
        dict: key = 項目名, value = 英語ラベル(英語の長いラベル)
    """
    labels = {key: value for key, value in target_english_labels.items()}
    if report_type == "edjp" or report_type == "edus" or report_type == "edif":
        pass
    elif report_type == "rvdf":
        for key, value in forecast_labels.items():
            labels[key] = value
    elif report_type == "rvfc":
        for key, value in forecast_labels.items():
            labels[key] = value
    else:
        raise ValueError(f"Unsupported report type: {report_type}")
    return labels


def get_target_taxonomy(
    report_type: str,
    xlsx_path: Path = excel_path,
) -> dict[str, list[str]]:
    """報告書種別に応じた収集対象のタグを取得する"""
    # excelシートの読み込み
    if report_type == "edjp":
        dfs = _read_excel_sheets(xlsx_path, target_sheets_jp)
    elif report_type == "edus":
        dfs = _read_excel_sheets(xlsx_path, target_sheets_us)
    elif report_type == "edif":
        dfs = _read_excel_sheets(xlsx_path, target_sheets_ifrs)
    elif report_type == "rvdf":
        dfs = _read_excel_sheets(xlsx_path, target_sheets_rvdf)
    elif report_type == "rvfc":
        dfs = _read_excel_sheets(xlsx_path, target_sheets_rvfc)
    else:
        raise ValueError(f"Unsupported report type: {report_type}")

    # タグの抽出
    target_labels = _get_target_labels(report_type)
    target_taxonomy = {}
    for key, label in target_labels.items():
        filtered = [
            df.filter(pl.col("label_en_long").str.split("-").list.first() == label)
            for df in dfs
        ]
        tags = sorted(
            set(sum([sorted(df["tag"].unique().to_list()) for df in filtered], []))
        )

        if len(tags) == 0:
            print(f"Not found: {label}")
        else:
            target_taxonomy[key] = tags

    return target_taxonomy

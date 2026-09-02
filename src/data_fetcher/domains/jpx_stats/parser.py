"""parser.py - JPXが配信するExcel（.xls, CFBF形式）のパース処理

「投資部門別売買状況」「個別銘柄信用取引残高表（日々公表銘柄）」はいずれも
複数行にまたがるマージセル形式のヘッダーを持つ。行位置は様式が固定されている前提で
決め打ちする（様式変更時はここが壊れるため、実行結果の件数チェックで検知する）。
"""

import io
import re

import fastexcel
import polars as pl

# 2022年4月4日の東証市場区分再編（東証一部/二部/マザーズ/JASDAQ → プライム/スタンダード/グロース）
# 前後でシート名が変わる。ワークブックに実際に含まれるシート名から自動判定する。
NEW_MARKET_SHEET_NAMES = ["TSE Prime", "TSE Standard", "TSE Growth", "Tokyo & Nagoya"]
OLD_MARKET_SHEET_NAMES = [
    "TSE 1st",
    "TSE 2nd",
    "TSE Mothers",
    "TSE JASDAQ",
    "Tokyo & Nagoya",
]
MARKET_SHEET_NAMES = NEW_MARKET_SHEET_NAMES  # 後方互換のため残す（新形式を指す）


def detect_market_sheet_names(available_sheet_names: list[str]) -> list[str]:
    available = set(available_sheet_names)
    if set(NEW_MARKET_SHEET_NAMES).issubset(available):
        return NEW_MARKET_SHEET_NAMES
    if set(OLD_MARKET_SHEET_NAMES).issubset(available):
        return OLD_MARKET_SHEET_NAMES
    raise ValueError(
        f"投資部門別売買状況ワークブックのシート名を認識できません: {available_sheet_names}"
    )

# 投資部門別売買状況シートの (データ開始行, 投資部門名) 一覧。
# 各カテゴリは3行（売り／買い／合計）で構成される。
_INVESTOR_TYPE_CATEGORY_ROWS = [
    (10, "自己計"),
    (13, "委託計"),
    (16, "総計"),
    (20, "法人"),
    (23, "個人"),
    (26, "海外投資家"),
    (29, "証券会社"),
    (33, "投資信託"),
    (36, "事業法人"),
    (39, "その他法人等"),
    (42, "金融機関"),
    (46, "生保・損保"),
    (49, "都銀・地銀等"),
    (52, "信託銀行"),
    (55, "その他金融機関"),
]

_WEEK_LABEL_RE = re.compile(r"(\d{4})年.*?\(\s*(\d{1,2})/(\d{1,2})\s*[-〜]\s*(\d{1,2})/(\d{1,2})\s*\)")


def _to_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "").strip()
    if text in ("", "-", "－"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_week_label(label: str) -> tuple[str, str]:
    """シート先頭の週ラベル（例: '2026年8月第3週 2026/8 week3  ( 8/17 - 8/21 )'）から
    当該週の開始日・終了日 (YYYY-MM-DD文字列) を抽出する。"""
    match = _WEEK_LABEL_RE.search(label)
    if match is None:
        raise ValueError(f"Could not parse week label: {label!r}")
    year, start_month, start_day, end_month, end_day = match.groups()
    week_start = f"{year}-{int(start_month):02d}-{int(start_day):02d}"
    # 週をまたいで月が変わる場合（例: 1/29-2/2）は終了日の月を使う
    week_end = f"{year}-{int(end_month):02d}-{int(end_day):02d}"
    return week_start, week_end


def parse_investor_type_sheet(
    raw: pl.DataFrame, market: str, metric: str
) -> pl.DataFrame:
    """投資部門別売買状況の1シート（1市場区分）を長形式に変換する。

    Args:
        raw: `pl.read_excel(..., has_header=False)` で読み込んだ生シート
        market: シート名（例: "TSE Prime"）
        metric: "value"（金額）または "volume"（株数）
    """
    week_label = raw.row(2)[0]
    week_start, week_end = parse_week_label(str(week_label))

    records = []
    for start_row, category in _INVESTOR_TYPE_CATEGORY_ROWS:
        for offset, item in ((0, "sell"), (1, "buy"), (2, "total")):
            row = raw.row(start_row + offset)
            records.append(
                {
                    "market": market,
                    "metric": metric,
                    "week_start": week_start,
                    "week_end": week_end,
                    "category": category,
                    "item": item,
                    "value": _to_float(row[8]),
                    "ratio_pct": _to_float(row[9]),
                }
            )
    return pl.from_dicts(records)


def parse_investor_type_workbook(content: bytes, metric: str) -> pl.DataFrame:
    """投資部門別売買状況ワークブック（全市場区分シート）を長形式に変換する。

    2022年4月4日の東証市場区分再編前後でシート名（市場区分）が異なるため、
    ワークブックに含まれるシート名から自動判定する。
    """
    available_sheet_names = fastexcel.read_excel(content).sheet_names
    market_sheet_names = detect_market_sheet_names(available_sheet_names)

    sheets = []
    for market in market_sheet_names:
        raw = pl.read_excel(io.BytesIO(content), sheet_name=market, has_header=False)
        sheets.append(parse_investor_type_sheet(raw, market, metric))
    return pl.concat(sheets)


# (列インデックス, カラム名, 数値変換するか) の一覧。
# 規制フラグ（規/日/監/株/喚/○）は列1・列2のどちらに入るか銘柄によって異なる
# （複数フラグを持つ銘柄用に2枠用意されている）ため、両方とも別カラムとして残す。
_MARGIN_COLUMN_MAP: list[tuple[int, str, bool]] = [
    (0, "unit_flag", False),
    (1, "regulation_flag_1", False),
    (2, "regulation_flag_2", False),
    (3, "issue_name", False),
    (4, "market", False),
    (5, "loan_margin_type", False),
    (6, "code", False),
    (7, "new_sec_code", False),
    (8, "sell_outstanding", True),
    (9, "sell_daily_change", True),
    (10, "sell_ratio_to_listed", True),
    (11, "buy_outstanding", True),
    (12, "buy_daily_change", True),
    (13, "buy_ratio_to_listed", True),
    (14, "sale_purchase_ratio", True),
    (15, "general_margin_sell", True),
    (16, "general_margin_sell_daily_change", True),
    (17, "standardized_margin_sell", True),
    (18, "standardized_margin_sell_daily_change", True),
    (19, "general_margin_buy", True),
    (20, "general_margin_buy_daily_change", True),
    (21, "standardized_margin_buy", True),
    (22, "standardized_margin_buy_daily_change", True),
]


def parse_margin_workbook(content: bytes) -> pl.DataFrame:
    """個別銘柄信用取引残高表（日々公表銘柄）を長形式に変換する。

    注意: このデータは信用規制等により日々の開示が義務付けられた銘柄のみを含む
    サブセットであり、全銘柄の週末残高ではない（全銘柄分は domains.taisyaku を使う）。
    """
    raw = pl.read_excel(io.BytesIO(content), has_header=False)
    report_date = str(raw.row(2)[1]).split(" ")[0]

    records = []
    for i in range(7, raw.height):
        row = raw.row(i)
        if row[6] is None:  # コード列が空 = データ行でない
            continue
        record = {"report_date": report_date}
        for col_idx, col_name, is_numeric in _MARGIN_COLUMN_MAP:
            value = row[col_idx]
            record[col_name] = _to_float(value) if is_numeric else value
        records.append(record)
    return pl.from_dicts(records)

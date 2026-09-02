"""large_shareholding.py - EDINET大量保有報告書（5%ルール）のCSV(XBRL)パース処理

docTypeCode "350"(大量保有報告書・変更報告書) / "360"(訂正報告書) を対象とする。
既存の `domains.edinet.api` の書類一覧・書類取得ロジックをそのまま再利用し、
本モジュールでは取得済みCSV行（get_document() の戻り値）から必要な要素のみを
抽出する。

制約: 共同保有者が複数いる書類では要素が保有者ごとに別コンテキストで重複するが、
本パーサーは提出者（筆頭の保有者）の値のみを抽出する（各要素IDの最初の出現を採用）。
"""

TARGET_DOC_TYPE_CODES = ("350", "360")

_TARGET_ELEMENTS = {
    "jplvh_cor:DocumentTitleCoverPage": "doc_title",
    "jplvh_cor:NameOfIssuer": "issuer_name",
    "jplvh_cor:SecurityCodeOfIssuer": "issuer_sec_code",
    "jplvh_cor:BaseDate": "base_date",
    "jplvh_cor:FilingDateCoverPage": "filing_date",
    "jplvh_cor:DateWhenFilingRequirementAroseCoverPage": "filing_requirement_date",
    "jplvh_cor:HoldingRatioOfShareCertificatesEtc": "holding_ratio",
    "jplvh_cor:HoldingRatioOfShareCertificatesEtcPerLastReport": "holding_ratio_prev",
    "jplvh_cor:TotalNumberOfStocksEtcHeld": "shares_held",
    "jplvh_cor:TotalNumberOfOutstandingStocksEtc": "shares_outstanding",
    "jpdei_cor:FilerNameInJapaneseDEI": "holder_name",
    "jpdei_cor:EDINETCodeDEI": "holder_edinet_code",
    "jplvh_cor:IndividualOrCorporation": "holder_type",
    "jplvh_cor:PurposeOfHolding": "purpose_of_holding",
}

_EMPTY_VALUES = ("－", "-", "")


def parse_large_shareholding_document(
    doc_id: str, rows: list[list[str]]
) -> dict[str, str | None] | None:
    """get_document() が返すCSV行から、大量保有報告書の主要項目を抽出する。

    Returns:
        抽出結果の辞書。ヘッダー行のみでデータが無い場合は None を返す。
    """
    if len(rows) < 2:
        return None
    header = rows[0]
    try:
        element_id_idx = header.index("要素ID")
        value_idx = header.index("値")
    except ValueError:
        return None

    found: dict[str, str] = {}
    for row in rows[1:]:
        if len(row) <= max(element_id_idx, value_idx):
            continue
        element_id = row[element_id_idx]
        field = _TARGET_ELEMENTS.get(element_id)
        if field is None or field in found:
            continue
        value = row[value_idx]
        found[field] = None if value in _EMPTY_VALUES else value

    record: dict[str, str | None] = {"doc_id": doc_id}
    for field in _TARGET_ELEMENTS.values():
        record[field] = found.get(field)
    return record

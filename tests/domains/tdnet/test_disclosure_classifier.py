from data_fetcher.domains.tdnet.disclosure_classifier import classify

_TITLE_TO_CODE = [
    ("2027年３月期 決算短信〔日本基準〕（連結）", "T1"),
    ("2027年４月期 第１四半期決算短信[日本基準](連結)", "T2"),
    ("2027年3月期第２四半期（中間期）及び通期業績予想の修正に関するお知らせ", "T4"),
    ("2027年３月期の期末配当予想の修正（無配）に関するお知らせ", "T5"),
    ("自己株式の取得に係る事項の決定に関するお知らせ", "T9"),
    ("自己株式の取得状況に関するお知らせ", "T10"),
    ("自己株式の消却に関するお知らせ", "T11"),
    ("株式分割及び定款の一部変更に関するお知らせ", "T17"),
    ("株式併合に関するお知らせ", "T17"),
    ("「中期経営計画2029」策定に関するお知らせ", "T27"),
    ("2026年8月　月次売上高前年比（速報）に関するお知らせ", "T28"),
    ("代表取締役の異動に関するお知らせ", "T31"),
    (
        "ラックスシェア・プレシジョン・ケイマン・リミテッドによる"
        "当社株式に対する公開買付けに関する賛同の意見表明及び応募推奨に関するお知らせ",
        "T30",
    ),
]


def test_classify_representative_titles_returns_expected_code():
    for title, expected_code in _TITLE_TO_CODE:
        result = classify(title)
        assert result.category_code == expected_code, title


def test_classify_quarterly_tanshin_is_not_misclassified_as_annual():
    result = classify("2027年４月期 第１四半期決算短信[日本基準](連結)")
    assert result.category_code == "T2"


def test_classify_correction_of_annual_tanshin_is_t8():
    title = "（訂正・数値データ訂正）「2026年６月期 決算短信〔日本基準〕（連結）」の一部訂正について"
    result = classify(title)
    assert result.category_code == "T8"
    assert result.is_correction is True


def test_classify_correction_of_non_correctable_category_keeps_original_code():
    title = "（訂正）「自己株式取得に係る事項の決定に関するお知らせ」の一部訂正について"
    result = classify(title)
    assert result.category_code == "T9"
    assert result.is_correction is True


def test_classify_unmatched_title_returns_none():
    result = classify("株主優待制度の新設に関するお知らせ")
    assert result.category_code is None
    assert result.is_correction is False

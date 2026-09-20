from data_fetcher.domains.tdnet.filename_metadata import parse_filename


def test_report_style_filename():
    metadata = parse_filename(
        "0101010-acbs01-tse-acedjpfr-13010-2024-03-31-01-2024-05-10-ixbrl.htm"
    )
    assert metadata is not None
    assert metadata.period == "a"
    assert metadata.consolidated == "c"
    assert metadata.style == "edjp"
    assert any(dt.name == "貸借対照表" for dt in metadata.doc_types)


def test_summary_style_filename():
    metadata = parse_filename(
        "tse-acedjpfr-13010-2024-03-31-01-2024-05-10-ixbrl.htm"
    )
    assert metadata is not None
    assert metadata.period == "a"
    assert metadata.consolidated == "c"
    assert metadata.style == "edjp"
    assert any(dt.name == "決算短信サマリー" for dt in metadata.doc_types)


def test_unmatched_filename_returns_none():
    assert parse_filename("not-a-report-file.htm") is None

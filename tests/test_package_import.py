"""Regression tests for top-level package imports."""


def test_import_data_fetcher_exposes_edinet_api():
    """Importing data_fetcher should not fail because of stale EDINET exports."""
    import data_fetcher

    assert data_fetcher.domains.edinet.api is not None

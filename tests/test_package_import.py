"""Regression tests for package imports."""


def test_import_data_fetcher():
    """Importing the top-level package should not fail."""
    import data_fetcher

    assert data_fetcher is not None
    assert data_fetcher.domains.edinet.api is not None

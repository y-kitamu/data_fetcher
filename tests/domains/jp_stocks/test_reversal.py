from data_fetcher.domains.jp_stocks.reversal import (
    industry_capex_to_depreciation,
    loss_narrowing,
)


def test_loss_narrowing_true_when_deficit_shrinks():
    assert loss_narrowing(current=-5.0, prior=-10.0) is True


def test_loss_narrowing_false_when_deficit_widens():
    assert loss_narrowing(current=-15.0, prior=-10.0) is False


def test_loss_narrowing_null_when_currently_profitable():
    assert loss_narrowing(current=5.0, prior=-10.0) is None


def test_loss_narrowing_null_when_missing_data():
    assert loss_narrowing(current=None, prior=-10.0) is None


def test_industry_capex_to_depreciation_sums_across_companies():
    capex = [100.0, None, 50.0]
    depreciation = [40.0, 20.0, 10.0]
    assert industry_capex_to_depreciation(capex, depreciation) == (150.0 / 70.0)


def test_industry_capex_to_depreciation_null_when_no_capex_data():
    assert industry_capex_to_depreciation([None, None], [10.0, 20.0]) is None

from data_fetcher.domains.jp_stocks.liquidation import compute_liquidation_value

_BS = {
    "cash": 100.0,
    "receivables": 200.0,
    "securities_current": 50.0,
    "inventory": 80.0,
    "current_assets_other": 20.0,
    "ppe": 300.0,
    "intangibles": 10.0,
    "investment_securities": 40.0,
    "investments_other": 0.0,
    "total_liabilities": 400.0,
}


def test_compute_liquidation_value_matches_hand_calculation():
    result = compute_liquidation_value(_BS, shares_outstanding=1_000_000)

    # 100*1.0 + 200*0.85 + 50*1.0 + 80*0.5 + 20*0.0 + 300*0.5 + 10*0.0 + 40*0.5
    # = 100 + 170 + 50 + 40 + 0 + 150 + 0 + 20 = 530
    assert result.adjusted_assets == 530.0
    assert result.value_total == 130.0  # 530 - 400
    assert result.value_per_share == 130.0 * 1e6 / 1_000_000
    assert result.components["receivables"].adjusted == 170.0
    assert result.components["investments"].book == 40.0


def test_compute_liquidation_value_missing_total_liabilities_is_null():
    bs = dict(_BS)
    del bs["total_liabilities"]
    result = compute_liquidation_value(bs, shares_outstanding=1_000_000)
    assert result.value_total is None
    assert result.value_per_share is None


def test_compute_liquidation_value_no_asset_data_is_null():
    result = compute_liquidation_value({"total_liabilities": 100.0}, shares_outstanding=1_000_000)
    assert result.adjusted_assets is None
    assert result.value_total is None

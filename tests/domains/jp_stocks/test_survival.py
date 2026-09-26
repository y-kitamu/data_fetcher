from data_fetcher.domains.jp_stocks.survival import compute_survival


def test_compute_survival_negative_ocf_computes_runway_months():
    result = compute_survival(
        operating_cf=-1200.0, cash_and_securities=2400.0, debt_due_within_1y=500.0, cash=1000.0
    )
    assert result.ocf_positive is False
    assert result.runway_months == 24.0  # 2400 / (1200/12)
    assert result.debt_due_to_cash == 0.5


def test_compute_survival_positive_ocf_has_no_runway_and_is_flagged():
    result = compute_survival(
        operating_cf=500.0, cash_and_securities=2400.0, debt_due_within_1y=500.0, cash=1000.0
    )
    assert result.ocf_positive is True
    assert result.runway_months is None


def test_compute_survival_missing_ocf_leaves_flags_null():
    result = compute_survival(
        operating_cf=None, cash_and_securities=2400.0, debt_due_within_1y=None, cash=1000.0
    )
    assert result.ocf_positive is None
    assert result.runway_months is None
    assert result.debt_due_to_cash is None

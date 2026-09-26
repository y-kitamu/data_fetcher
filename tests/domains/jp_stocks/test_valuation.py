from data_fetcher.domains.jp_stocks.valuation import (
    ActualForecast,
    accruals_to_assets,
    compute_valuation_ratios,
    earnings_yield,
    ev,
    ev_ebitda,
    market_cap,
    pbr,
    pcfr,
    per,
    per_forecast,
    per_normalized,
    per_trailing,
    per_x_pbr,
    psr,
    roa,
    roe,
)


def test_compute_valuation_ratios_computes_per_pbr_roe_roa() -> None:
    ratios = compute_valuation_ratios(
        price=100.0,
        eps=ActualForecast(actual=10.0, forecast=20.0),
        bps=ActualForecast(actual=50.0),
        net_income=ActualForecast(actual=100.0, forecast=200.0),
        net_assets_actual=1000.0,
        total_assets_actual=2000.0,
    )
    assert ratios.per.actual == 10.0  # 100 / 10
    assert ratios.per.forecast == 5.0  # 100 / 20
    assert ratios.pbr.actual == 2.0  # 100 / 50
    assert ratios.roe.actual == 10.0  # 100 / 1000 * 100
    assert ratios.roe.forecast == 20.0  # 200 / 1000 * 100
    assert ratios.roa.actual == 5.0  # 100 / 2000 * 100
    assert ratios.roa.forecast == 10.0  # 200 / 2000 * 100


def test_compute_valuation_ratios_without_price_leaves_ratios_none() -> None:
    ratios = compute_valuation_ratios(
        price=None,
        eps=ActualForecast(actual=10.0),
        bps=ActualForecast(actual=50.0),
        net_income=ActualForecast(actual=100.0),
        net_assets_actual=None,
        total_assets_actual=None,
    )
    assert ratios.per.actual is None
    assert ratios.pbr.actual is None


def test_per_none_when_price_or_eps_missing() -> None:
    assert per(None, 10.0) is None
    assert per(100.0, None) is None
    assert per(100.0, 0.0) is None
    assert per(100.0, 10.0) == 10.0


def test_pbr_none_when_price_or_bps_missing() -> None:
    assert pbr(None, 50.0) is None
    assert pbr(100.0, None) is None
    assert pbr(100.0, 50.0) == 2.0


def test_roe_none_when_net_assets_missing_but_zero_income_computes() -> None:
    assert roe(100.0, None) is None
    assert roe(100.0, 0.0) is None
    # Existing behaviour: only net_assets_actual is truthy-guarded; net_income
    # of exactly 0 still yields a computed (zero) ratio, since the guard is
    # `net_income is None`, not a truthiness check.
    assert roe(0.0, 1000.0) == 0.0
    assert roe(None, 1000.0) is None


def test_roa_none_when_total_assets_missing_but_zero_income_computes() -> None:
    assert roa(100.0, None) is None
    assert roa(100.0, 0.0) is None
    assert roa(0.0, 2000.0) == 0.0
    assert roa(None, 2000.0) is None


def test_market_cap_none_when_price_or_shares_missing() -> None:
    assert market_cap(None, 1000.0) is None
    assert market_cap(100.0, None) is None
    assert market_cap(100.0, 1000.0) == 100_000.0


def test_per_forecast_and_per_trailing_and_per_normalized() -> None:
    assert per_forecast(1000.0, 100.0) == 10.0
    assert per_forecast(1000.0, None) is None
    assert per_trailing(1000.0, 200.0) == 5.0
    assert per_normalized(1000.0, 50.0) == 20.0


def test_pcfr_adds_depreciation_to_net_income() -> None:
    assert pcfr(1000.0, 80.0, 20.0) == 10.0  # 1000 / (80+20)
    assert pcfr(1000.0, None, 20.0) is None
    assert pcfr(1000.0, -20.0, 20.0) is None  # denom == 0


def test_psr() -> None:
    assert psr(1000.0, 2000.0) == 0.5
    assert psr(1000.0, None) is None


def test_earnings_yield_is_inverse_of_per_forecast() -> None:
    assert earnings_yield(10.0) == 0.1
    assert earnings_yield(None) is None
    assert earnings_yield(0.0) is None


def test_per_x_pbr() -> None:
    assert per_x_pbr(10.0, 0.5) == 5.0
    assert per_x_pbr(None, 0.5) is None


def test_ev_formula() -> None:
    assert ev(1000.0, 300.0, 100.0, 50.0) == 1150.0  # 1000+300-100-50
    assert ev(1000.0, 300.0, None, 50.0) is None
    assert ev(1000.0, 300.0, 100.0, None) == 1200.0  # securities_current defaults to 0


def test_ev_ebitda() -> None:
    assert ev_ebitda(1200.0, 400.0) == 3.0
    assert ev_ebitda(1200.0, 0.0) is None
    assert ev_ebitda(1200.0, None) is None


def test_accruals_to_assets() -> None:
    assert accruals_to_assets(100.0, 60.0, 2000.0) == 0.02  # (100-60)/2000
    assert accruals_to_assets(100.0, None, 2000.0) is None
    assert accruals_to_assets(100.0, 60.0, None) is None

from data_fetcher.domains.reports.kill_criteria import (
    evaluate_kill_criteria,
    evaluate_kill_criterion,
    resolve_metric_path,
)

_SNAPSHOT = {
    "survival": {"runway_months": 8},
    "ratios": {"cycle_stats": {"operating_margin": {"latest": -0.02}}},
}


def test_resolve_metric_path_navigates_nested_dict():
    assert resolve_metric_path(_SNAPSHOT, "survival.runway_months") == 8
    assert resolve_metric_path(_SNAPSHOT, "ratios.cycle_stats.operating_margin.latest") == -0.02


def test_resolve_metric_path_missing_path_returns_none():
    assert resolve_metric_path(_SNAPSHOT, "survival.missing_key") is None
    assert resolve_metric_path(_SNAPSHOT, "not.a.real.path") is None


def test_evaluate_kill_criterion_all_six_operators():
    for op, actual_hits in [
        ("<", True), ("<=", True), (">", False), (">=", False), ("==", False), ("!=", True),
    ]:
        criterion = {"text": "t", "metric": "survival.runway_months", "op": op, "value": 12}
        result = evaluate_kill_criterion(criterion, _SNAPSHOT)
        assert result.hit is actual_hits, op
        assert result.actual == 8


def test_evaluate_kill_criterion_null_actual_yields_null_hit():
    criterion = {"text": "t", "metric": "survival.missing", "op": "<", "value": 12}
    result = evaluate_kill_criterion(criterion, _SNAPSHOT)
    assert result.actual is None
    assert result.hit is None


def test_evaluate_kill_criteria_skips_plain_string_conditions():
    criteria = [
        "撤退条件を文章で書いただけのもの",
        {"text": "runway<12", "metric": "survival.runway_months", "op": "<", "value": 12},
    ]
    results = evaluate_kill_criteria(criteria, _SNAPSHOT)
    assert len(results) == 1
    assert results[0].hit is True

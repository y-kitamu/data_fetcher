import pytest

from data_fetcher.domains.reports.rubric import (
    ConditionSyntaxError,
    compute_auto_ratings,
    evaluate_bins,
    evaluate_components,
    evaluate_condition,
    evaluate_rules,
    evaluate_score_items,
    grade_from_buckets,
    parse_condition,
)

_RUBRIC = {
    "rubric_version": 1,
    "asset_value": {"metric": "market_cap_to_liquidation", "bins": [0.7, 1.0, 1.5, 2.5], "order": "asc"},
    "profitability": {"metric": "median_operating_margin", "bins": [0.12, 0.08, 0.05, 0.02], "order": "desc"},
    "financial_health": {
        "components": {
            "equity_ratio": {"points": [[0.60, 2], [0.40, 1]], "else": 0},
            "runway_months": {"points": [[36, 2], [18, 1]], "else": 0, "null_if_ocf_positive": 2},
        },
        "grade_by_total": {"A": [4], "B": [3], "C": [1, 2], "D": [0]},
    },
    "survival": {
        "rules": [
            {"if": "going_concern_note or covenant_breach", "grade": "E"},
            {"if": "runway_months >= 36", "grade": "A"},
            {"if": "runway_months >= 12", "grade": "C"},
            {"else": "E"},
        ]
    },
    "reversal": {
        "score_items": ["loss_narrowing_qoq", "loss_narrowing_yoy", "industry_capex_to_depreciation < 1.0"],
        "grade_by_count": {"A": [3], "B": [2], "C": [1], "D": [0]},
    },
}


def test_evaluate_bins_asc_and_desc_boundaries():
    assert evaluate_bins(0.7, [0.7, 1.0, 1.5, 2.5], "asc") == "A"
    assert evaluate_bins(0.71, [0.7, 1.0, 1.5, 2.5], "asc") == "B"
    assert evaluate_bins(3.0, [0.7, 1.0, 1.5, 2.5], "asc") == "E"
    assert evaluate_bins(None, [0.7, 1.0], "asc") is None

    assert evaluate_bins(0.12, [0.12, 0.08, 0.05, 0.02], "desc") == "A"
    assert evaluate_bins(0.01, [0.12, 0.08, 0.05, 0.02], "desc") == "E"


def test_evaluate_condition_and_or_kleene_logic():
    cond = parse_condition("a >= 0.6 and b <= 3")
    assert evaluate_condition(cond, {"a": 0.7, "b": 2}) is True
    assert evaluate_condition(cond, {"a": 0.5, "b": 2}) is False
    assert evaluate_condition(cond, {"a": None, "b": 2}) is None  # unknown, not false
    assert evaluate_condition(cond, {"a": 0.4, "b": None}) is False  # a already fails -> False wins

    cond_or = parse_condition("x or y")
    assert evaluate_condition(cond_or, {"x": True, "y": None}) is True
    assert evaluate_condition(cond_or, {"x": False, "y": None}) is None
    assert evaluate_condition(cond_or, {"x": False, "y": False}) is False


def test_condition_parser_rejects_unsafe_expressions():
    for unsafe in ["__import__('os')", "a; b", "a == 1 == 2", "1 +", "a >"]:
        with pytest.raises(ConditionSyntaxError):
            evaluate_condition(parse_condition(unsafe), {"a": 1})


def test_evaluate_components_scores_and_sums():
    total, scores = evaluate_components(
        _RUBRIC["financial_health"]["components"],
        {"equity_ratio": 0.65, "runway_months": 20, "ocf_positive": False},
    )
    assert scores == {"equity_ratio": 2, "runway_months": 1}
    assert total == 3


def test_evaluate_components_null_if_ocf_positive_overrides_runway():
    total, scores = evaluate_components(
        _RUBRIC["financial_health"]["components"],
        {"equity_ratio": 0.3, "runway_months": None, "ocf_positive": True},
    )
    assert scores["runway_months"] == 2


def test_grade_from_buckets():
    assert grade_from_buckets(4, _RUBRIC["financial_health"]["grade_by_total"]) == "A"
    assert grade_from_buckets(2, _RUBRIC["financial_health"]["grade_by_total"]) == "C"
    assert grade_from_buckets(99, _RUBRIC["financial_health"]["grade_by_total"]) is None


def test_evaluate_rules_first_match_wins_and_else_is_fallback():
    rules = _RUBRIC["survival"]["rules"]
    assert evaluate_rules(rules, {"going_concern_note": True}) == "E"
    assert evaluate_rules(rules, {"going_concern_note": False, "covenant_breach": False, "runway_months": 40}) == "A"
    assert evaluate_rules(rules, {"going_concern_note": False, "covenant_breach": False, "runway_months": 15}) == "C"
    assert evaluate_rules(rules, {"going_concern_note": False, "covenant_breach": False, "runway_months": 1}) == "E"


def test_evaluate_score_items_counts_true_conditions():
    count = evaluate_score_items(
        _RUBRIC["reversal"]["score_items"],
        {"loss_narrowing_qoq": True, "loss_narrowing_yoy": False, "industry_capex_to_depreciation": 0.5},
    )
    assert count == 2


def test_compute_auto_ratings_end_to_end():
    metrics = {
        "market_cap_to_liquidation": 0.9,
        "median_operating_margin": 0.09,
        "equity_ratio": 0.65,
        "runway_months": 40,
        "ocf_positive": True,
        "going_concern_note": False,
        "covenant_breach": False,
        "loss_narrowing_qoq": True,
        "loss_narrowing_yoy": True,
        "industry_capex_to_depreciation": 0.8,
    }
    result = compute_auto_ratings(_RUBRIC, metrics)
    assert result.ratings["asset_value"] == "B"
    assert result.ratings["profitability"] == "B"
    assert result.ratings["financial_health"] == "A"
    assert result.ratings["survival"] == "A"
    assert result.ratings["reversal"] == "A"
    assert result.ratings["business_quality"] is None
    assert result.ratings["shareholder_policy"] is None
    assert result.details["asset_value"].metric == 0.9

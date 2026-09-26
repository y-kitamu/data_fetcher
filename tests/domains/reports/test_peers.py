from data_fetcher.domains.reports.peers import select_peers_by_market_cap


def test_select_peers_by_market_cap_picks_nearest_n():
    market_caps = {
        "1000": 1000.0,  # self
        "1001": 1100.0,  # diff 100
        "1002": 500.0,  # diff 500
        "1003": 950.0,  # diff 50
        "1004": 2000.0,  # diff 1000
        "1005": 900.0,  # diff 100
        "1006": 1050.0,  # diff 50
    }
    result = select_peers_by_market_cap("1000", market_caps, n=5)
    assert "1000" not in result
    assert len(result) == 5
    # Closest by |diff|: 1003(50), 1006(50), 1001(100), 1005(100), then either 1002 or 1004(500/1000)
    assert set(result[:2]) == {"1003", "1006"}
    assert "1002" in result or "1004" in result


def test_select_peers_by_market_cap_skips_unknown_caps():
    market_caps = {"1000": 1000.0, "1001": None, "1002": 900.0}
    result = select_peers_by_market_cap("1000", market_caps)
    assert result == ["1002"]


def test_select_peers_by_market_cap_returns_empty_when_own_cap_unknown():
    market_caps = {"1000": None, "1001": 900.0}
    assert select_peers_by_market_cap("1000", market_caps) == []

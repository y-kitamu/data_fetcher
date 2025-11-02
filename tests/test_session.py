"""Tests for session module to validate logic before refactoring"""


def test_get_session_with_cache():
    """Test get_session with cache enabled"""
    from data_fetcher.session import get_session

    # Create a session with cache
    session = get_session(max_requests_per_second=10)

    assert session is not None
    # Check if it's the expected session type
    from data_fetcher.session import CachedLimiterSession

    assert isinstance(session, CachedLimiterSession)


def test_get_session_without_cache():
    """Test get_session without cache"""
    from data_fetcher.session import get_session

    # Create a session without cache
    session = get_session(max_requests_per_second=5, cache_file=None)

    assert session is not None
    # Check if it's the expected session type
    from data_fetcher.session import LimiterSession

    assert isinstance(session, LimiterSession)


def test_session_class_naming():
    """Test that session classes are named correctly"""
    from data_fetcher.session import CachedLimiterSession, LimiterSession

    # Verify class names (this will help catch the typo fix)
    assert LimiterSession.__name__ == "LimiterSession"
    assert CachedLimiterSession.__name__ == "CachedLimiterSession"

"""session.py
"""

from pathlib import Path

from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

from .constants import PROJECT_ROOT


class LimitterSession(LimiterMixin, Session):
    pass


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


def get_session(
    max_requsets_per_second: int = 10,
    cache_file: Path | None = PROJECT_ROOT / "cache/requests.cache",
) -> Session:
    if cache_file is None:
        return LimitterSession(
            limiter=Limiter(RequestRate(max_requsets_per_second, Duration.SECOND))
        )

    return CachedLimiterSession(
        limiter=Limiter(RequestRate(max_requsets_per_second, Duration.SECOND)),
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache(cache_file),
    )

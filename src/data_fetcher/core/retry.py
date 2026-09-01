"""retry.py"""

import time
from functools import wraps
from typing import Callable, TypeVar

from loguru import logger

T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """指数バックオフでリトライするデコレータ。

    Args:
        max_retries: 最大試行回数（初回呼び出しを含む）
        base_delay: 初回リトライまでの待機秒数。以降 2 のべき乗で増加する（例: 1s, 2s, 4s）
        exceptions: リトライ対象とする例外の型
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt >= max_retries - 1:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} attempts: {e}"
                        )
                        raise
                    wait_time = base_delay * (2**attempt)
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): "
                        f"{e}. Retrying after {wait_time}s..."
                    )
                    time.sleep(wait_time)
            raise RuntimeError("unreachable")  # pragma: no cover

        return wrapper

    return decorator

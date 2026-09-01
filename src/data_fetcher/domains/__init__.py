"""Domains module for specialized business logic.

Contains complex, domain-specific modules that don't fit neatly into
fetchers/readers/processors categories. These modules typically have
specialized data processing pipelines and extensive internal dependencies.
"""

from . import (
    edinet,
    google_trends,
    jp_stocks,
    jpx_stats,
    jquants,
    kabutan,
    taisyaku,
    tdnet,
)

__all__ = [
    "edinet",
    "google_trends",
    "jp_stocks",
    "jpx_stats",
    "jquants",
    "kabutan",
    "taisyaku",
    "tdnet",
]

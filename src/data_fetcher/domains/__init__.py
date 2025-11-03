"""Domains module for specialized business logic.

Contains complex, domain-specific modules that don't fit neatly into
fetchers/readers/processors categories. These modules typically have
specialized data processing pipelines and extensive internal dependencies.
"""

from . import edinet, jp_stocks, kabutan, tdnet

__all__ = ["edinet", "jp_stocks", "kabutan", "tdnet"]

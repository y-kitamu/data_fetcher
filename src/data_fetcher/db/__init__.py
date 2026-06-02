"""DuckDB storage layer for data_fetcher.

Usage:
    from data_fetcher.db import get_connection, upsert_news, load_news_df
"""

from .connection import DEFAULT_DB_PATH, SCHEMA_VERSION, get_connection
from .financial import (
    get_processed_filing_ids,
    load_all_facts_df,
    load_facts_by_concept_df,
    load_facts_df,
    upsert_from_zip,
)
from .news import get_existing_news_urls, load_news_df, upsert_news
from .taxonomy import (
    is_concepts_empty,
    is_taxonomy_empty,
    load_concepts,
    load_taxonomy,
    upsert_concepts,
    upsert_taxonomy,
)

__all__ = [
    "DEFAULT_DB_PATH",
    "SCHEMA_VERSION",
    "get_connection",
    "upsert_taxonomy",
    "load_taxonomy",
    "is_taxonomy_empty",
    "upsert_concepts",
    "load_concepts",
    "is_concepts_empty",
    "get_processed_filing_ids",
    "upsert_from_zip",
    "load_facts_df",
    "load_all_facts_df",
    "load_facts_by_concept_df",
    "upsert_news",
    "get_existing_news_urls",
    "load_news_df",
]

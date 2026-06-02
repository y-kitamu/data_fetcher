"""DuckDB connection management and schema migrations."""

from __future__ import annotations

from pathlib import Path

import duckdb
from loguru import logger

from ..core.constants import DEFAULT_DB_PATH

SCHEMA_VERSION = 2

_MIGRATIONS: dict[int, str] = {
    1: "initial schema: filings, financial_facts, taxonomy_labels, concepts, concept_mappings",
    2: "add news and news_symbols tables",
}

# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #

_DDL_SCHEMA_VERSIONS = """
CREATE TABLE IF NOT EXISTS schema_versions (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMP DEFAULT current_timestamp,
    description VARCHAR
)
"""

_DDL_FILINGS = """
CREATE TABLE IF NOT EXISTS filings (
    filing_id       VARCHAR PRIMARY KEY,
    source          VARCHAR NOT NULL,
    company_code    VARCHAR NOT NULL,
    edinet_code     VARCHAR,
    filing_datetime TIMESTAMP NOT NULL,
    fiscal_year_end DATE
)
"""

_DDL_FINANCIAL_FACTS = """
CREATE TABLE IF NOT EXISTS financial_facts (
    filing_id    VARCHAR NOT NULL REFERENCES filings(filing_id),
    element_id   VARCHAR NOT NULL REFERENCES taxonomy_labels(element_id),
    context_id   VARCHAR,
    start_date   DATE,
    end_date     DATE,
    instant_date DATE,
    segments     VARCHAR,
    period       VARCHAR,
    quarter      VARCHAR,
    consolidated VARCHAR,
    forecast     VARCHAR,
    value        DOUBLE,
    text         VARCHAR,
    PRIMARY KEY (filing_id, element_id, context_id)
)
"""

# element_id = f"{namespace}:{element_name}"
_DDL_TAXONOMY_LABELS = """
CREATE TABLE IF NOT EXISTS taxonomy_labels (
    element_id            VARCHAR NOT NULL PRIMARY KEY,
    japanese_label        VARCHAR,
    english_label         VARCHAR,
    period_type           VARCHAR,
    balance               VARCHAR,
    abstract              BOOLEAN,
)
"""

_DDL_CONCEPTS = """
CREATE TABLE IF NOT EXISTS concepts (
    concept_name VARCHAR PRIMARY KEY,
    description  VARCHAR
)
"""

_DDL_CONCEPT_MAPPINGS = """
CREATE TABLE IF NOT EXISTS concept_mappings (
    namespace    VARCHAR NOT NULL,
    element_id   VARCHAR NOT NULL,
    concept_name VARCHAR NOT NULL REFERENCES concepts(concept_name),
    PRIMARY KEY (namespace, element_id)
)
"""

_DDL_CONCEPT_MAPPINGS_IDX = """
CREATE INDEX IF NOT EXISTS idx_concept_mappings_name
ON concept_mappings(concept_name)
"""

_DDL_NEWS = """
CREATE TABLE IF NOT EXISTS news (
    news_id      VARCHAR PRIMARY KEY,
    published_at TIMESTAMP NOT NULL,
    source       VARCHAR NOT NULL,
    title        VARCHAR,
    body         VARCHAR,
    url          VARCHAR,
    category     VARCHAR
)
"""

_DDL_NEWS_IDX_PUBLISHED = """
CREATE INDEX IF NOT EXISTS idx_news_published
ON news(published_at)
"""

_DDL_NEWS_SYMBOLS = """
CREATE TABLE IF NOT EXISTS news_symbols (
    news_id VARCHAR NOT NULL REFERENCES news(news_id),
    symbol  VARCHAR NOT NULL,
    PRIMARY KEY (news_id, symbol)
)
"""

_DDL_NEWS_SYMBOLS_IDX = """
CREATE INDEX IF NOT EXISTS idx_news_symbols_symbol
ON news_symbols(symbol)
"""


# --------------------------------------------------------------------------- #
# 接続管理
# --------------------------------------------------------------------------- #


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """DuckDB 接続を取得し、スキーマが存在しない場合は初期化して返す。

    Args:
        db_path: DuckDB ファイルのパス。デフォルトは data/database.duckdb。

    Returns:
        初期化済みの DuckDB 接続。
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    _ensure_schema(con)
    return con


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """テーブルが存在しない場合に作成し、スキーマバージョンを記録する。"""
    con.execute(_DDL_SCHEMA_VERSIONS)
    con.execute(_DDL_FILINGS)
    con.execute(_DDL_TAXONOMY_LABELS)
    con.execute(_DDL_FINANCIAL_FACTS)
    con.execute(_DDL_CONCEPTS)
    con.execute(_DDL_CONCEPT_MAPPINGS)
    con.execute(_DDL_CONCEPT_MAPPINGS_IDX)
    con.execute(_DDL_NEWS)
    con.execute(_DDL_NEWS_IDX_PUBLISHED)
    con.execute(_DDL_NEWS_SYMBOLS)
    con.execute(_DDL_NEWS_SYMBOLS_IDX)

    current_version: int = con.execute(
        "SELECT COALESCE(MAX(version), 0) FROM schema_versions"
    ).fetchone()[0]  # type: ignore[index]

    for v in range(current_version + 1, SCHEMA_VERSION + 1):
        con.execute(
            "INSERT OR IGNORE INTO schema_versions VALUES (?, current_timestamp, ?)",
            [v, _MIGRATIONS.get(v, f"schema version {v}")],
        )
        logger.info(f"DuckDB schema migrated to version={v}: {_MIGRATIONS.get(v, '')}")

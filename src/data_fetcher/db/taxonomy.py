"""DuckDB CRUD for taxonomy_labels, concepts, concept_mappings."""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
from loguru import logger

if TYPE_CHECKING:
    from ..domains.tdnet.constants.schema import TaxonomyElement


def upsert_taxonomy(
    con: duckdb.DuckDBPyConnection,
    elements: list[TaxonomyElement],
) -> None:
    """タクソノミ要素を taxonomy_labels テーブルに upsert する。

    同じ element_id が存在する場合は上書きする。

    Args:
        con: DuckDB 接続。
        elements: タクソノミ要素のリスト。
    """
    rows = [
        (
            e.element_id,
            e.japanese_label,
            e.english_label,
            e.period_type,
            e.balance,
            e.abstract,
        )
        for e in elements
    ]
    if not rows:
        return
    con.executemany(
        """
        INSERT OR REPLACE INTO taxonomy_labels
            (element_id, japanese_label, english_label, period_type, balance, abstract)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    logger.info(f"Upserted {len(rows)} taxonomy elements")


def load_taxonomy(
    con: duckdb.DuckDBPyConnection,
) -> list[TaxonomyElement]:
    """taxonomy_labels テーブルからタクソノミを読み込む。

    Returns:
        list of TaxonomyElement. テーブルが空の場合は空リスト。
    """
    from ..domains.tdnet.constants.schema import TaxonomyElement  # lazy import

    rows = con.execute("""
        SELECT element_id, japanese_label, english_label, period_type, balance, abstract
        FROM taxonomy_labels
    """).fetchall()

    results: list[TaxonomyElement] = []
    for (
        element_id,
        japanese_label,
        english_label,
        period_type,
        balance,
        abstract,
    ) in rows:
        results.append(
            TaxonomyElement(
                element_id=element_id,
                japanese_label=japanese_label,
                english_label=english_label or "",
                period_type=period_type or "",
                balance=balance or "",
                abstract=abstract,
            )
        )
    return results


def is_taxonomy_empty(con: duckdb.DuckDBPyConnection) -> bool:
    """taxonomy_labels テーブルが空かどうかを返す。"""
    count: int = con.execute("SELECT COUNT(*) FROM taxonomy_labels").fetchone()[0]  # type: ignore[index]
    return count == 0


def upsert_concepts(
    con: duckdb.DuckDBPyConnection,
    groups: dict[str, list[TaxonomyElement]] | None = None,
) -> None:
    """concepts / concept_mappings テーブルを upsert する。

    同じ会計基準の異なる (namespace, element_id) を同一の concept_name にマッピングする。

    Args:
        con: DuckDB 接続。
        groups: concept_name → TaxonomyElement リストのマッピング。
                None の場合は taxonomy_groups (デフォルト定義) を使用する。
    """
    if groups is None:
        from ..domains.tdnet.constants.taxonomy_group import (  # lazy import
            taxonomy_groups as _default_taxonomy_groups,
        )

        groups = _default_taxonomy_groups

    con.executemany(
        "INSERT OR IGNORE INTO concepts (concept_name) VALUES (?)",
        [(name,) for name in groups],
    )

    rows = [
        (e.namespace, e.element_id, name)
        for name, elems in groups.items()
        for e in elems
    ]
    if rows:
        con.executemany(
            "INSERT OR REPLACE INTO concept_mappings (namespace, element_id, concept_name) VALUES (?, ?, ?)",
            rows,
        )
        logger.info(f"Upserted {len(groups)} concepts, {len(rows)} concept mappings")


def load_concepts(con: duckdb.DuckDBPyConnection) -> list[str]:
    """登録済みの concept_name 一覧を返す。"""
    rows = con.execute(
        "SELECT concept_name FROM concepts ORDER BY concept_name"
    ).fetchall()
    return [row[0] for row in rows]


def is_concepts_empty(con: duckdb.DuckDBPyConnection) -> bool:
    """concept_mappings テーブルが空かどうかを返す。"""
    count: int = con.execute("SELECT COUNT(*) FROM concept_mappings").fetchone()[0]  # type: ignore[index]
    return count == 0

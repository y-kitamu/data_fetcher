"""DuckDB CRUD for filings and financial_facts."""

from __future__ import annotations

import duckdb
import polars as pl
from loguru import logger

from ..domains.tdnet.constants.schema import Document, NonNumericData, NumericData


def get_processed_filing_ids(
    con: duckdb.DuckDBPyConnection,
    source: str,
    company_code: str,
) -> set[str]:
    """処理済みの filing_id セットを返す。

    Args:
        con: DuckDB 接続。
        source: データソース ('tdnet' or 'edinet')。
        company_code: 証券コード (5桁)。
    """
    rows = con.execute(
        "SELECT filing_id FROM filings WHERE source=? AND company_code=?",
        [source, company_code],
    ).fetchall()
    return {row[0] for row in rows}


def upsert_from_zip(
    con: duckdb.DuckDBPyConnection,
    filing_id: str,
    documents: list[Document],
    numerics: list[NumericData],
    nonnumerics: list[NonNumericData],
    source: str = "tdnet",
) -> None:
    """ZIP 1件分の NumericData を filings + financial_facts テーブルに upsert する。

    既に同じ PRIMARY KEY が存在する場合は IGNORE (スキップ) する。

    Args:
        con: DuckDB 接続。
        filing_id: ZIP ファイル名 (stem) など、一意な書類識別子。
        numerics: collect_numeric_data() の戻り値。
        nonnumerics: collect_non_numeric_data() の戻り値。
        source: データソース ('tdnet' or 'edinet')。
    """
    if not numerics and not nonnumerics:
        logger.debug(f"No data to upsert for filing_id={filing_id}")
        return

    doc = documents[0]
    con.execute(
        """
        INSERT OR IGNORE INTO filings
            (filing_id, source, company_code, edinet_code,
             filing_datetime, fiscal_year_end)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            filing_id,
            source,
            doc.security_code,
            None,
            doc.filing_date,
            doc.fiscal_year_end,
        ),
    )

    fact_rows = [
        (
            filing_id,
            d.element.element_id,
            d.context_id,
            d.start_date,
            d.end_date,
            d.instant_date,
            ",".join(d.segments),
            d.period,
            d.quarter,
            d.consolidated,
            d.forecast,
            d.value,
            None,  # text
        )
        for d in numerics
    ] + [
        (
            filing_id,
            d.element.element_id,
            d.context_id,
            d.start_date,
            d.end_date,
            d.instant_date,
            ",".join(d.segments),
            d.period,
            d.quarter,
            d.consolidated,
            d.forecast,
            None,  # value
            str(d.value),
        )
        for d in nonnumerics
    ]
    con.executemany(
        """
        INSERT OR IGNORE INTO financial_facts
            (filing_id, element_id, context_id,
            start_date, end_date, instant_date,
            segments, period, quarter, consolidated, 
            forecast, value, text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        fact_rows,
    )
    logger.debug(f"Upserted {len(fact_rows)} facts for filing_id={filing_id}")


def load_facts_df(
    con: duckdb.DuckDBPyConnection,
    company_code: str,
) -> pl.DataFrame:
    """指定会社の財務データを Polars DataFrame で返す。

    taxonomy_labels と JOIN して日本語ラベルも付与する。

    Args:
        con: DuckDB 接続。
        company_code: 証券コード (5桁)。

    Returns:
        カラム: company_code, filing_date, namespace, element_id,
                period_ctx, consolidated, forecast, value, unit,
                japanese_label, english_label
    """
    return con.execute(
        """
        SELECT
            f.company_code,
            f.filing_date,
            f.namespace,
            f.element_id,
            f.period_ctx,
            f.consolidated,
            f.forecast,
            f.value,
            f.unit,
            t.japanese_label,
            t.english_label
        FROM financial_facts f
        LEFT JOIN taxonomy_labels t
            ON f.namespace = t.namespace
            AND f.element_id = t.element_id
        WHERE f.company_code = ?
        ORDER BY f.filing_date, f.namespace, f.element_id
        """,
        [company_code],
    ).pl()


def load_all_facts_df(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """全社の財務データを Polars DataFrame で返す。"""
    return con.execute("""
        SELECT
            f.company_code,
            f.filing_date,
            f.namespace,
            f.element_id,
            f.period_ctx,
            f.consolidated,
            f.forecast,
            f.value,
            f.unit,
            t.japanese_label,
            t.english_label
        FROM financial_facts f
        LEFT JOIN taxonomy_labels t
            ON f.namespace = t.namespace
            AND f.element_id = t.element_id
        ORDER BY f.company_code, f.filing_date
    """).pl()


def load_facts_by_concept_df(
    con: duckdb.DuckDBPyConnection,
    concept_name: str,
    company_code: str | None = None,
) -> pl.DataFrame:
    """指定した正規化概念名で財務データを取得する。

    会計基準（日本基準 / IFRS / 米国基準）の違いを吸収して横断検索できる。

    Args:
        con: DuckDB 接続。
        concept_name: 正規化された概念名 ('net_sales', 'operating_income' など)。
        company_code: 絞り込む証券コード。None の場合は全社。

    Returns:
        カラム: company_code, filing_date, namespace, element_id,
                concept_name, period_ctx, consolidated, forecast, value, unit
    """
    query = """
        SELECT
            f.company_code,
            f.filing_date,
            f.namespace,
            f.element_id,
            cm.concept_name,
            f.period_ctx,
            f.consolidated,
            f.forecast,
            f.value,
            f.unit
        FROM financial_facts f
        JOIN concept_mappings cm
            ON f.namespace = cm.namespace AND f.element_id = cm.element_id
        WHERE cm.concept_name = ?
    """
    params: list = [concept_name]
    if company_code is not None:
        query += " AND f.company_code = ?"
        params.append(company_code)
    query += " ORDER BY f.company_code, f.filing_date"
    return con.execute(query, params).pl()

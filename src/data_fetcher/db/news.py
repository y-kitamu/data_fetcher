"""DuckDB CRUD for news and news_symbols."""

from __future__ import annotations

import datetime
import hashlib

import duckdb
import polars as pl
from loguru import logger


def upsert_news(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
) -> None:
    """ニュースデータを news / news_symbols テーブルに upsert する。

    同じ URL のニュースが既に存在する場合は IGNORE (スキップ) する。
    1 記事に複数の銘柄が紐づく場合は news_symbols に複数行挿入する。

    Args:
        con: DuckDB 接続。
        df: ニュースデータ。カラム: published_at, source, symbol, title, body, url, category。
            symbol が空・null の行は news_symbols に追加されない。
    """
    if df.is_empty():
        return

    df = df.with_columns(
        pl.col("url")
        .map_elements(
            lambda u: hashlib.md5(u.encode()).hexdigest() if u else "",
            return_dtype=pl.Utf8,
        )
        .alias("news_id")
    ).filter(pl.col("news_id") != "")

    news_rows = (
        df.select(
            ["news_id", "published_at", "source", "title", "body", "url", "category"]
        )
        .unique(subset=["news_id"], keep="first")
        .to_dicts()
    )
    con.executemany(
        """
        INSERT OR IGNORE INTO news
            (news_id, published_at, source, title, body, url, category)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["news_id"],
                r["published_at"],
                r["source"],
                r["title"],
                r["body"],
                r["url"],
                r["category"],
            )
            for r in news_rows
        ],
    )

    symbol_rows = (
        df.select(["news_id", "symbol"])
        .filter(pl.col("symbol").is_not_null() & (pl.col("symbol") != ""))
        .unique()
        .to_dicts()
    )
    if symbol_rows:
        con.executemany(
            "INSERT OR IGNORE INTO news_symbols (news_id, symbol) VALUES (?, ?)",
            [(r["news_id"], r["symbol"]) for r in symbol_rows],
        )

    logger.info(
        f"Upserted {len(news_rows)} news articles, {len(symbol_rows)} symbol links"
    )


def get_existing_news_urls(
    con: duckdb.DuckDBPyConnection,
    source: str | None = None,
) -> set[str]:
    """登録済みニュース URL のセットを返す。重複取り込みのスキップ用。

    Args:
        con: DuckDB 接続。
        source: データソースで絞り込む ('kabutan', 'gnews' など)。None の場合は全件。
    """
    if source is not None:
        rows = con.execute("SELECT url FROM news WHERE source=?", [source]).fetchall()
    else:
        rows = con.execute("SELECT url FROM news").fetchall()
    return {row[0] for row in rows if row[0]}


def load_news_df(
    con: duckdb.DuckDBPyConnection,
    symbols: list[str] | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> pl.DataFrame:
    """ニュースデータを Polars DataFrame で返す。

    symbols を指定した場合は news_symbols 経由で JOIN するため、
    銘柄紐付けのないニュースは除外される。

    Args:
        con: DuckDB 接続。
        symbols: 絞り込む証券コードリスト。None の場合は全銘柄（銘柄未紐付け含む）。
        start_date: 絞り込む開始日（published_at >= start_date の 00:00:00）。
        end_date: 絞り込む終了日（published_at <= end_date の 23:59:59）。

    Returns:
        カラム: news_id, published_at, source, title, body, url, category, symbol
    """
    if symbols is not None:
        join_clause = "JOIN news_symbols ns ON n.news_id = ns.news_id"
    else:
        join_clause = "LEFT JOIN news_symbols ns ON n.news_id = ns.news_id"

    query = f"""
        SELECT
            n.news_id,
            n.published_at,
            n.source,
            n.title,
            n.body,
            n.url,
            n.category,
            ns.symbol
        FROM news n
        {join_clause}
        WHERE 1=1
    """
    params: list = []

    if symbols is not None:
        placeholders = ", ".join("?" * len(symbols))
        query += f" AND ns.symbol IN ({placeholders})"
        params.extend(symbols)
    if start_date is not None:
        query += " AND n.published_at >= ?"
        params.append(datetime.datetime.combine(start_date, datetime.time.min))
    if end_date is not None:
        query += " AND n.published_at <= ?"
        params.append(datetime.datetime.combine(end_date, datetime.time.max))

    query += " ORDER BY n.published_at DESC"
    return con.execute(query, params).pl()

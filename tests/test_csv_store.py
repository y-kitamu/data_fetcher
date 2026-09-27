"""Tests for append_and_save_csv (src/data_fetcher/core/csv_store.py)."""

import polars as pl

from data_fetcher.core.csv_store import append_and_save_csv


def test_default_dedup_subset_none_keeps_existing_full_row_dedup(tmp_path):
    """dedup_subset未指定時は既存の「完全一致行のみ重複排除」の挙動が変わらない
    （Google Trends等の既存呼び出し元に対する回帰テスト）。"""
    path = tmp_path / "out.csv"

    df1 = pl.DataFrame({"date": ["2024-01-01"], "keyword": ["foo"], "interest": [10]})
    append_and_save_csv(df1, path, sort_col="date")

    # 全く同じ行を再度追記しても増えない
    append_and_save_csv(df1, path, sort_col="date")
    result = pl.read_csv(path)
    assert result.height == 1

    # interestの値が異なる行は別行として残る（完全一致ではないため重複排除されない）
    df2 = pl.DataFrame({"date": ["2024-01-01"], "keyword": ["foo"], "interest": [20]})
    append_and_save_csv(df2, path, sort_col="date")
    result = pl.read_csv(path)
    assert result.height == 2
    assert sorted(result["interest"].to_list()) == [10, 20]


def test_dedup_subset_skips_unchanged_value_and_keeps_old_fetched_at(tmp_path):
    """dedup_subset=["date","value"]指定時、値が変わっていない再取得は追記されず、
    古い方のfetched_atが残る。"""
    path = tmp_path / "out.csv"

    df_day1 = pl.DataFrame(
        {"date": ["2024-01-01"], "value": [100.0], "fetched_at": ["2024-01-02"]}
    )
    append_and_save_csv(df_day1, path, sort_col="date", dedup_subset=["date", "value"])

    df_day2 = pl.DataFrame(
        {"date": ["2024-01-01"], "value": [100.0], "fetched_at": ["2024-01-03"]}
    )
    append_and_save_csv(df_day2, path, sort_col="date", dedup_subset=["date", "value"])

    result = pl.read_csv(path)
    assert result.height == 1
    assert result["fetched_at"].to_list() == ["2024-01-02"]


def test_dedup_subset_appends_new_row_when_value_changes(tmp_path):
    """値が変わった行は新しいfetched_at付きで追記され、旧行は残る（改定履歴）。"""
    path = tmp_path / "out.csv"

    df_day1 = pl.DataFrame(
        {"date": ["2024-01-01"], "value": [100.0], "fetched_at": ["2024-01-02"]}
    )
    append_and_save_csv(df_day1, path, sort_col="date", dedup_subset=["date", "value"])

    df_revision = pl.DataFrame(
        {"date": ["2024-01-01"], "value": [105.0], "fetched_at": ["2024-02-01"]}
    )
    append_and_save_csv(
        df_revision, path, sort_col="date", dedup_subset=["date", "value"]
    )

    result = pl.read_csv(path).sort("fetched_at")
    assert result.height == 2
    assert result["value"].to_list() == [100.0, 105.0]
    assert result["fetched_at"].to_list() == ["2024-01-02", "2024-02-01"]


def test_dedup_subset_always_appends_new_key(tmp_path):
    """新規keyの行は既存データに関係なく常に追記される。"""
    path = tmp_path / "out.csv"

    df_day1 = pl.DataFrame(
        {"date": ["2024-01-01"], "value": [100.0], "fetched_at": ["2024-01-02"]}
    )
    append_and_save_csv(df_day1, path, sort_col="date", dedup_subset=["date", "value"])

    df_day2 = pl.DataFrame(
        {"date": ["2024-02-01"], "value": [110.0], "fetched_at": ["2024-02-02"]}
    )
    append_and_save_csv(df_day2, path, sort_col="date", dedup_subset=["date", "value"])

    result = pl.read_csv(path).sort("date")
    assert result.height == 2
    assert result["date"].to_list() == ["2024-01-01", "2024-02-01"]

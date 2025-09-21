"""__init__.py"""

import polars as pl

from ..constants import PROJECT_ROOT

# from . import convert, excel, preprocess
from .document import collect_documents
from .numeric_data import collect_numeric_data
from .taxonomy_element import collect_all_taxonomies


def read_csv(ticker: str):
    """指定した銘柄のCSVデータを読み込む"""
    csv_path = PROJECT_ROOT / "data/tdnet/csv" / f"{ticker}.csv"
    return pl.read_csv(csv_path, infer_schema_length=None)

"""__init__.py"""

import shutil
from pathlib import Path

import polars as pl
import requests

# from . import convert, excel, preprocess
from . import fetcher
from .constants import zip_root_dir
from .document import collect_documents
from .numeric_data import collect_numeric_datas
from .taxonomy_element import collect_all_taxonomies

__all__ = [
    "fetcher",
    "zip_root_dir",
    "collect_documents",
    "collect_numeric_datas",
    "collect_all_taxonomies",
    "get_all_data",
]


def get_all_data(code: str, work_dir: Path, session: requests.Session) -> list:
    zip_dir = zip_root_dir / code
    zip_files = sorted(zip_dir.glob("*.zip"))

    all_data = []
    taxonomy_elems = collect_all_taxonomies()
    for zip_file in zip_files:
        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(exist_ok=True)
        shutil.unpack_archive(zip_file, extract_dir=work_dir)

        documents = collect_documents(work_dir, zip_file, session)
        all_data += collect_numeric_datas(documents, taxonomy_elems)

    if work_dir.exists():
        shutil.rmtree(work_dir)
    return all_data

"""__init__.py"""

import shutil
from pathlib import Path

import polars as pl

from ..constants import PROJECT_ROOT

# from . import convert, excel, preprocess
from .constants import zip_root_dir
from .document import collect_documents
from .numeric_data import collect_numeric_data
from .taxonomy_element import collect_all_taxonomies


def get_all_data(code: str, work_dir: Path):
    zip_dir = zip_root_dir / code
    zip_files = sorted(zip_dir.glob("*.zip"))

    all_data = []
    taxonomy_elems = collect_all_taxonomies()
    for zip_file in zip_files:
        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(exist_ok=True)
        shutil.unpack_archive(zip_file, extract_dir=work_dir)

        documents = collect_documents(work_dir, zip_file)
        all_data += collect_numeric_data(documents, taxonomy_elems)

    if work_dir.exists():
        shutil.rmtree(work_dir)
    return all_data

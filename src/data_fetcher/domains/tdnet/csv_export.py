"""XBRLファクト(NumericData/NonNumericData)を1事実=1行のロング形式CSVに変換・追記する。

決算短信1件の中には同じ財務項目でも「連結/単体」「当期/前期」「実績/予想」といった
複数のcontextの値が存在するため、集計・選択はせず全context・数値/非数値をそのまま
保持する（db/financial.py::upsert_from_zip がDuckDB向けに行っている分解と同じ粒度）。

zip一括バックフィル用の scripts/convert_tdnet_to_csv.py と、取得直後に変換する
scripts/fetch_data_from_tdnet.py の両方から使われる共有ロジック。
"""

import shutil
from pathlib import Path

import polars as pl
from requests import Session

from ...core.constants import PROJECT_ROOT
from ...core.csv_store import append_and_save_csv
from .constants.schema import NonNumericData, NumericData
from .document import collect_documents
from .numeric_data import collect_data_from_document
from .taxonomy_index import TaxonomyIndex

OUTPUT_DIR = PROJECT_ROOT / "data/tdnet/csv"

# 型混在によるpolarsの推論エラーを避けるため、DataFrame構築時に明示する。
ROW_SCHEMA = {
    "code": pl.Utf8,
    "filing_date": pl.Utf8,
    "filing_datetime": pl.Utf8,
    "fiscal_year_end": pl.Utf8,
    "doc_period": pl.Utf8,
    "doc_consolidated": pl.Utf8,
    "doc_style": pl.Utf8,
    "source_file": pl.Utf8,
    "element_id": pl.Utf8,
    "japanese_label": pl.Utf8,
    "english_label": pl.Utf8,
    "context_id": pl.Utf8,
    "start_date": pl.Utf8,
    "end_date": pl.Utf8,
    "instant_date": pl.Utf8,
    "segments": pl.Utf8,
    "period": pl.Utf8,
    "quarter": pl.Utf8,
    "consolidated": pl.Utf8,
    "previous_current": pl.Utf8,
    "forecast": pl.Utf8,
    "is_nil": pl.Boolean,
    "value": pl.Float64,
    "text": pl.Utf8,
}


def _fact_row(fact: NumericData | NonNumericData, source_file: str) -> dict:
    doc = fact.document
    is_numeric = isinstance(fact, NumericData)
    return {
        "code": doc.security_code,
        "filing_date": doc.filing_date.date().isoformat(),
        "filing_datetime": doc.filing_date.isoformat(),
        "fiscal_year_end": doc.fiscal_year_end.isoformat() if doc.fiscal_year_end else None,
        "doc_period": doc.period,
        "doc_consolidated": doc.consolidated,
        "doc_style": doc.style,
        "source_file": source_file,
        "element_id": fact.element.element_id,
        "japanese_label": fact.element.japanese_label,
        "english_label": fact.element.english_label,
        "context_id": fact.context_id,
        "start_date": fact.start_date.isoformat() if fact.start_date else None,
        "end_date": fact.end_date.isoformat() if fact.end_date else None,
        "instant_date": fact.instant_date.isoformat() if fact.instant_date else None,
        "segments": ",".join(fact.segments),
        "period": fact.period,
        "quarter": fact.quarter,
        "consolidated": fact.consolidated,
        "previous_current": fact.previous_current,
        "forecast": fact.forecast,
        "is_nil": fact.is_nil,
        "value": fact.value if is_numeric else None,
        "text": None if is_numeric else str(fact.value),
    }


def build_fact_rows(
    numerics: list[NumericData],
    nonnumerics: list[NonNumericData],
    zip_file: Path,
) -> list[dict]:
    source_file = zip_file.name
    return [_fact_row(fact, source_file) for fact in [*numerics, *nonnumerics]]


def _read_processed_source_files(output_path: Path) -> set[str]:
    if not output_path.exists():
        return set()
    existing = pl.read_csv(output_path, columns=["source_file"], infer_schema_length=0)
    return set(existing["source_file"].to_list())


def append_zip_to_csv(
    zip_file: Path,
    session: Session,
    taxonomy_index: TaxonomyIndex,
    work_dir: Path,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """zip1件分をパースしてCSVに追記する。

    既にCSVのsource_file列にこのzip名が記録されている場合は何もしない(冪等)。
    呼び出し側は「未変換かどうか」を判定する必要がなく、常に呼び出してよい。
    """
    code = zip_file.parent.name
    output_path = output_dir / f"{code}.csv"
    if zip_file.name in _read_processed_source_files(output_path):
        return

    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.unpack_archive(zip_file, extract_dir=work_dir)

        documents = collect_documents(work_dir, zip_file, session)
        rows = []
        for document in documents:
            numerics, nonnumerics = collect_data_from_document(document, taxonomy_index)
            rows += build_fact_rows(numerics, nonnumerics, zip_file)
    finally:
        if work_dir.exists():
            shutil.rmtree(work_dir)

    if rows:
        append_and_save_csv(
            pl.DataFrame(rows, schema=ROW_SCHEMA), output_path, sort_col="filing_date"
        )

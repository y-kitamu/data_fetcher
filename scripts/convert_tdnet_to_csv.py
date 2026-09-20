"""convert_tdnet_to_csv.py
data/tdnet/raw/{code}/*.zip に保存済みのTDnet決算短信iXBRLアーカイブを解析し、
data/tdnet/csv/{code}.csv に1事実=1行のロング形式で書き出す。

決算短信1件の中には同じ財務項目でも「連結/単体」「当期/前期」「実績/予想」といった
複数のcontextの値が存在するため、集計・選択はせず全context・数値/非数値をそのまま
保持する（src/data_fetcher/db/financial.py::upsert_from_zip がDuckDB向けに行っている
分解と同じ粒度）。

提出日時の解決 (resolve_filing_datetime) がzipファイルごとにkabutan.jpへアクセスする
ため、全銘柄(data/tdnet/raw配下 約4000銘柄・約19万zip)をまとめて変換すると数時間規模の
実行時間になる。--codes で対象銘柄を絞れるようにし、また既に変換済みのzip
(CSVのsource_file列に記録)はスキップして、中断後の再実行でも続きから処理できるように
している。
"""

import argparse
import shutil
from pathlib import Path

import polars as pl
import tqdm
from requests import Session

import data_fetcher
from data_fetcher.domains.tdnet.constants import zip_root_dir
from data_fetcher.domains.tdnet.constants.schema import NonNumericData, NumericData
from data_fetcher.domains.tdnet.document import collect_documents
from data_fetcher.domains.tdnet.numeric_data import collect_data_from_document
from data_fetcher.domains.tdnet.taxonomy_element import collect_all_taxonomies
from data_fetcher.domains.tdnet.taxonomy_index import TaxonomyIndex

OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/tdnet/csv"
WORK_DIR = data_fetcher.constants.PROJECT_ROOT / "data/tdnet/tmp_convert"

# 型混在によるpolarsの推論エラーを避けるため、DataFrame構築時に明示する。
_ROW_SCHEMA = {
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


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codes",
        nargs="*",
        default=None,
        help="対象の証券コード（省略時はdata/tdnet/raw配下の全銘柄）",
    )
    return parser.parse_args()


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


def convert_code(
    code: str,
    session: Session,
    taxonomy_index: TaxonomyIndex,
    work_dir: Path = WORK_DIR,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    zip_files = sorted((zip_root_dir / code).glob("*.zip"))
    if not zip_files:
        data_fetcher.logger.warning(f"No zip files found for {code}")
        return

    output_path = output_dir / f"{code}.csv"
    processed = _read_processed_source_files(output_path)
    pending = [z for z in zip_files if z.name not in processed]
    if not pending:
        return

    for zip_file in pending:
        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        shutil.unpack_archive(zip_file, extract_dir=work_dir)

        documents = collect_documents(work_dir, zip_file, session)
        rows = []
        for document in documents:
            numerics, nonnumerics = collect_data_from_document(document, taxonomy_index)
            rows += build_fact_rows(numerics, nonnumerics, zip_file)

        if rows:
            data_fetcher.append_and_save_csv(
                pl.DataFrame(rows, schema=_ROW_SCHEMA), output_path, sort_col="filing_date"
            )
        else:
            data_fetcher.logger.warning(f"No data extracted from {zip_file.name}")

    if work_dir.exists():
        shutil.rmtree(work_dir)


def main():
    args = parse_args()
    codes = args.codes or sorted(p.name for p in zip_root_dir.iterdir() if p.is_dir())

    session = data_fetcher.get_session()
    taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())

    for code in tqdm.tqdm(codes):
        convert_code(code, session, taxonomy_index)


if __name__ == "__main__":
    data_fetcher.debug.run_debug(main)

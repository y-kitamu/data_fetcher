"""migrate_edinet_financial_csv.py
旧形式の data/edinet/financial/{secCode5桁}.csv (target_summary_taxonomyの17項目のみ) を、
TDnetと同じ1事実=1行のロング形式 data/edinet/csv/{code}.csv (全項目) に移行する。

旧CSVは抽出済みの17項目しか持たないため、data/edinet/doc_list.csv に記録された全書類を
EDINET APIから再取得して全項目を変換する。EDINETは提出から約10年を過ぎた書類本体を提供しない
(一覧には残るが本体は404)ため、再取得できない書類に限り旧CSVの行を新形式に変換して残す。

約17万件の書類を取得するため長時間かかる。銘柄単位でまとめて保存し、既にsource_file列に
記録済みの書類はスキップするので、中断後の再実行で続きから処理できる。--codes で対象銘柄を
絞れる(4桁・5桁どちらでも可)。
"""

import argparse
import csv
import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import tqdm
from requests import Session

import data_fetcher
from data_fetcher.domains.edinet.csv_export import (
    OUTPUT_DIR,
    DocumentUnavailableError,
    build_fact_rows,
    download_document_rows,
    output_code,
    read_processed_doc_ids,
    save_fact_rows,
)
from data_fetcher.domains.tdnet.taxonomy_element import collect_all_taxonomies
from data_fetcher.domains.tdnet.taxonomy_index import Found, TaxonomyIndex

EDINET_DIR = data_fetcher.constants.PROJECT_ROOT / "data/edinet"
DOC_LIST_PATH = EDINET_DIR / "doc_list.csv"
LEGACY_DIR = EDINET_DIR / "financial"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codes",
        nargs="*",
        default=None,
        help="対象の証券コード(4桁/5桁)またはEDINETコード（省略時はdoc_list.csvの全銘柄）",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="並列に処理する銘柄数（API全体で5req/sに制限）"
    )
    return parser.parse_args()


def load_doc_list(path: Path = DOC_LIST_PATH) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return [
            {key: (value or None) for key, value in row.items()}
            for row in csv.DictReader(f)
        ]


def group_docs_by_code(docs: list[dict], codes: list[str] | None) -> dict[str, list[dict]]:
    targets = {code[:4] if code[:1].isdigit() else code for code in codes} if codes else None
    grouped: dict[str, list[dict]] = defaultdict(list)
    for doc in docs:
        code = output_code(doc)
        if targets is None or code in targets:
            grouped[code].append(doc)
    return dict(grouped)


def read_legacy_rows(doc: dict, legacy_dir: Path = LEGACY_DIR) -> pl.DataFrame:
    """旧CSVのうち、この書類から抽出された行(提出日時が一致する行)を返す。"""
    legacy_path = legacy_dir / f"{doc['secCode'] or doc['edinetCode']}.csv"
    if not legacy_path.exists():
        return pl.DataFrame()
    announce_date = datetime.datetime.strptime(
        doc["submitDateTime"], "%Y-%m-%d %H:%M"
    ).strftime("%Y%m%d%H%M")
    legacy = pl.read_csv(legacy_path, infer_schema_length=0)
    # 旧形式はcomprehensive_incomeがnet_incomeと同じ要素を指していたため、要素・context単位で重複を除く
    return legacy.filter(pl.col("announce_date") == announce_date).unique(
        subset=["edinet_key", "period"], keep="first", maintain_order=True
    )


def build_legacy_fact_rows(
    doc: dict, legacy: pl.DataFrame, taxonomy_index: TaxonomyIndex
) -> list[dict]:
    """旧CSVの行を新形式に変換する。

    旧CSVの行をEDINET CSVの行形式に組み直して build_fact_rows に通し、日付は旧CSVで
    算出済みの start_date/end_date をそのまま使う。DEIが無いため doc_period/doc_style/
    doc_consolidated は空になり、StatementPeriodの整形対象からは外れる。
    """
    edinet_rows = []
    for element_id, context_id, value in legacy.select(
        "edinet_key", "period", "value"
    ).iter_rows():
        schema, name = element_id.split(":", 1)
        found = taxonomy_index.lookup(schema, name)
        label = found.element.japanese_label if isinstance(found, Found) else ""
        edinet_rows.append([element_id, label, context_id, "", "", "", "pure", "", value])

    rows = build_fact_rows(doc, edinet_rows, taxonomy_index)
    for row, (start, end) in zip(rows, legacy.select("start_date", "end_date").iter_rows()):
        is_instant = row["context_id"].split("_")[0].endswith("Instant")
        row["start_date"] = None if is_instant else start
        row["end_date"] = None if is_instant else end
        row["instant_date"] = end if is_instant else None

    fiscal_year_ends = [
        end
        for row, (_, end) in zip(rows, legacy.select("start_date", "end_date").iter_rows())
        if row["period"] == "CurrentYear" and end
    ]
    fiscal_year_end = fiscal_year_ends[0] if fiscal_year_ends else None
    for row in rows:
        row["fiscal_year_end"] = fiscal_year_end
        row["doc_period"] = None
    return rows


def migrate_code(
    code: str,
    docs: list[dict],
    session: Session,
    taxonomy_index: TaxonomyIndex,
    output_dir: Path = OUTPUT_DIR,
) -> dict[str, int]:
    output_path = output_dir / f"{code}.csv"
    processed = read_processed_doc_ids(output_path)
    stats = {"fetched": 0, "legacy": 0, "missing": 0, "skipped": 0}
    rows: list[dict] = []
    for doc in sorted(docs, key=lambda d: d["submitDateTime"]):
        if doc["docID"] in processed:
            stats["skipped"] += 1
            continue
        try:
            rows += build_fact_rows(
                doc, download_document_rows(doc["docID"], session), taxonomy_index
            )
            stats["fetched"] += 1
        except DocumentUnavailableError as e:
            legacy = read_legacy_rows(doc)
            if legacy.height == 0:
                data_fetcher.logger.warning(f"No data for {code} {doc['docID']}: {e}")
                stats["missing"] += 1
                continue
            rows += build_legacy_fact_rows(doc, legacy, taxonomy_index)
            stats["legacy"] += 1
        processed.add(doc["docID"])

    save_fact_rows(rows, output_path)
    return stats


def main():
    args = parse_args()
    grouped = group_docs_by_code(load_doc_list(), args.codes)

    # 17万件のzipをHTTPキャッシュ(SQLite)に溜めないようキャッシュなしのセッションを使う
    session = data_fetcher.get_session(max_requests_per_second=5, cache_file=None)
    taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())

    totals: dict[str, int] = defaultdict(int)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(migrate_code, code, docs, session, taxonomy_index): code
            for code, docs in sorted(grouped.items())
        }
        for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
            code = futures[future]
            try:
                for key, count in future.result().items():
                    totals[key] += count
            except Exception as e:
                data_fetcher.logger.error(f"Failed to migrate {code}: {e}")
                totals["failed_codes"] += 1

    data_fetcher.logger.info(f"Migration finished: {dict(totals)}")


if __name__ == "__main__":
    data_fetcher.debug.run_debug(main)

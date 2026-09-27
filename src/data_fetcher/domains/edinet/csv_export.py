"""EDINETのXBRL→CSV(書類取得API type=5)の全行を、TDnetと同じ1事実=1行のロング形式CSVに変換・追記する。

列構成は domains/tdnet/csv_export.py の ROW_SCHEMA に揃え、data/tdnet/csv と同じ
Reader・StatementPeriod整形(shape_statement_periods)で扱えるようにしている。そのため:

- doc_style は会計基準をTDnetの決算短信スタイルコード(edjp/edif/edus)に対応付ける
- consolidated は「連結・個別」列(判別できない行はDEIの連結決算の有無)から
  ConsolidatedMember/NonConsolidatedMember を設定する
  (EDINETのcontext_idには連結側のメンバーが現れず、context_idだけでは連結を判別できない)

日次取得の scripts/fetch_data_from_edinet.py と、旧形式からの移行用
scripts/migrate_edinet_financial_csv.py の両方から使われる共有ロジック。
"""

import csv
import datetime
import io
import re
import time
import zipfile
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta
from loguru import logger
from requests import Session
from requests.exceptions import RequestException

from ...core.constants import PROJECT_ROOT
from ...core.csv_store import append_and_save_csv
from ..tdnet.csv_export import ROW_SCHEMA
from ..tdnet.taxonomy_index import Ambiguous, Found, TaxonomyIndex
from .api import api_key

OUTPUT_DIR = PROJECT_ROOT / "data/edinet/csv"
_DOC_ENDPOINT = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
_TIMEOUT = 30.0
_JST_SUFFIX = "+09:00"

# EDINETのCSVで「値なし」「単位なし」を表す文字
_NIL = "－"

_PERIOD_TYPES = {"FY": "a", "HY": "s", "Q1": "q", "Q2": "q", "Q3": "q", "Q4": "q"}
_ACCOUNTING_STANDARD_STYLES = {
    "Japan GAAP": "edjp",
    "IFRS": "edif",
    "US GAAP": "edus",
}
_CONSOLIDATION_TYPES = {"連結": "ConsolidatedMember", "個別": "NonConsolidatedMember"}
_CONSOLIDATION_MEMBERS = ("ConsolidatedMember", "NonConsolidatedMember")
_MEMBER_SEPARATOR_RE = re.compile(r"(?<=Member)_")


class DocumentUnavailableError(Exception):
    """書類本体を取得できない(EDINETの保存期間(約10年)切れ・通信失敗など)。"""


def download_document_rows(
    doc_id: str, session: Session, max_retries: int = 3
) -> list[list[str]]:
    """書類のXBRL→CSV zipを取得し、zip内の全CSVのデータ行(ヘッダー除く)を返す。

    EDINETのCSVはUTF-16のタブ区切りで、値(テキストブロック等)にカンマ・改行を含むため
    csvモジュールでパースする。
    """
    url = _DOC_ENDPOINT.format(doc_id=doc_id)
    params = {"type": "5", "Subscription-Key": api_key}
    for attempt in range(max_retries):
        try:
            res = session.get(url, params=params, timeout=_TIMEOUT)
            break
        except RequestException as e:
            logger.warning(f"Failed to get document {doc_id} ({e}). Retry {attempt + 1}.")
            time.sleep(2**attempt)
    else:
        raise DocumentUnavailableError(f"{doc_id}: request failed {max_retries} times")

    try:
        archive = zipfile.ZipFile(io.BytesIO(res.content), mode="r")
    except zipfile.BadZipFile:
        # 保存期間切れの書類は一覧には残るが、本体はJSONの404が返る
        raise DocumentUnavailableError(f"{doc_id}: {res.text[:200]}")

    rows = []
    with archive:
        for filename in archive.namelist():
            if not filename.endswith(".csv"):
                continue
            text = archive.read(filename).decode("utf-16")
            reader = csv.reader(io.StringIO(text), delimiter="\t")
            next(reader, None)
            rows += [row for row in reader if len(row) >= 9]
    return rows


def output_code(doc: dict) -> str:
    """出力ファイル名・code列に使うコード。TDnetに合わせ証券コードは4桁にする。"""
    sec_code = doc.get("secCode")
    return sec_code[:4] if sec_code else doc["edinetCode"]


def _parse_date(value: str | None) -> datetime.date | None:
    if not value or value == _NIL:
        return None
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        return None


class _PeriodResolver:
    """context_idの期間部分(例: Prior1YearDuration)を実日付に解決する。

    type=5のCSVにはcontextの日付が含まれないため、DEIの会計期間(無ければAPIメタデータの
    periodStart/periodEnd)を起点に、Prior{N}/Next{N} を N年ずらして求める。
    """

    def __init__(
        self,
        fiscal_year_start: datetime.date | None,
        fiscal_year_end: datetime.date | None,
        period_end: datetime.date | None,
        filing_date: datetime.date,
    ):
        self.fiscal_year_start = fiscal_year_start
        self.fiscal_year_end = fiscal_year_end
        self.period_end = period_end
        self.filing_date = filing_date

    def resolve(
        self, period: str, is_instant: bool
    ) -> tuple[datetime.date | None, datetime.date | None]:
        if period == "FilingDate":
            return self.filing_date, self.filing_date

        base, years = period, 0
        for prefix, sign in (("Prior", -1), ("Next", 1), ("Current", 0)):
            if period.startswith(prefix):
                rest = period[len(prefix) :]
                digits = "".join(c for c in rest if c.isdigit())
                base = rest[len(digits) :]
                years = sign * (int(digits) if digits else (1 if sign else 0))
                break

        if base == "Year":
            start, end = self.fiscal_year_start, self.fiscal_year_end
        elif base in ("YTD", "Interim") or base.startswith("AccumulatedQ"):
            start, end = self.fiscal_year_start, self.period_end
        elif base == "Quarter":
            end = self.period_end
            start = end - relativedelta(months=3) + relativedelta(days=1) if end else None
        else:
            return None, None

        shift = relativedelta(years=years)
        start = start + shift if start else None
        end = end + shift if end else None
        return (None if is_instant else start), end


def _split_context(context_id: str) -> tuple[str, bool, list[str]]:
    # 提出者独自メンバー名は "jpcrp030000-asr_E00012-000XxxMember" のように"_"を含むため、
    # "Member"の直後の"_"だけを区切りとみなす
    head, _, rest = context_id.partition("_")
    members = _MEMBER_SEPARATOR_RE.split(rest) if rest else []
    if head.endswith("Instant"):
        return head[: -len("Instant")], True, members
    if head.endswith("Duration"):
        return head[: -len("Duration")], False, members
    return head, False, members


def _english_label(element_id: str, taxonomy_index: TaxonomyIndex | None) -> str | None:
    if taxonomy_index is None or ":" not in element_id:
        return None
    schema, name = element_id.split(":", 1)
    result = taxonomy_index.lookup(schema, name)
    if isinstance(result, Found):
        return result.element.english_label
    if isinstance(result, Ambiguous):
        return result.elements[0].english_label
    return None


def _to_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def build_fact_rows(
    doc: dict,
    rows: list[list[str]],
    taxonomy_index: TaxonomyIndex | None = None,
) -> list[dict]:
    """EDINET CSVの行(要素ID,項目名,コンテキストID,相対年度,連結・個別,期間・時点,
    ユニットID,単位,値)をROW_SCHEMAの辞書に変換する。

    `doc`はEDINET書類一覧APIの1件(docID, edinetCode, secCode, submitDateTime,
    periodStart, periodEnd)。
    """
    dei = {row[0]: row[8] for row in rows if row[0].startswith("jpdei_cor:")}
    submitted = datetime.datetime.strptime(doc["submitDateTime"], "%Y-%m-%d %H:%M")
    period_start = _parse_date(doc.get("periodStart"))
    period_end = _parse_date(doc.get("periodEnd"))

    fiscal_year_start = _parse_date(dei.get("jpdei_cor:CurrentFiscalYearStartDateDEI")) or period_start
    fiscal_year_end = _parse_date(dei.get("jpdei_cor:CurrentFiscalYearEndDateDEI"))
    if fiscal_year_end is None and fiscal_year_start is not None:
        fiscal_year_end = fiscal_year_start + relativedelta(years=1, days=-1)
    current_period_end = _parse_date(dei.get("jpdei_cor:CurrentPeriodEndDateDEI")) or period_end
    resolver = _PeriodResolver(
        fiscal_year_start, fiscal_year_end, current_period_end, submitted.date()
    )

    consolidated_flag = dei.get("jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI")
    doc_fields = {
        "code": output_code(doc),
        "filing_date": submitted.date().isoformat(),
        "filing_datetime": submitted.isoformat() + _JST_SUFFIX,
        "fiscal_year_end": fiscal_year_end.isoformat() if fiscal_year_end else None,
        "doc_period": _PERIOD_TYPES.get(dei.get("jpdei_cor:TypeOfCurrentPeriodDEI", "")),
        "doc_consolidated": {"true": "c", "false": "n"}.get(consolidated_flag or ""),
        "doc_style": _ACCOUNTING_STANDARD_STYLES.get(dei.get("jpdei_cor:AccountingStandardsDEI", "")),
        "source_file": doc["docID"],
    }

    default_consolidated = {
        "true": "ConsolidatedMember",
        "false": "NonConsolidatedMember",
    }.get(consolidated_flag or "", "")

    label_cache: dict[str, str | None] = {}
    fact_rows = []
    for element_id, label, context_id, _relative, consolidation, _ptype, unit, _unit_name, value in (
        row[:9] for row in rows
    ):
        period, is_instant, members = _split_context(context_id)
        start, end = resolver.resolve(period, is_instant)

        if "NonConsolidatedMember" in members:
            consolidated = "NonConsolidatedMember"
        else:
            # IFRSの連結財務諸表や経営指標等は「連結・個別」が「その他」になるため、
            # 列で判別できない行は書類の連結決算の有無(DEI)に従う
            consolidated = _CONSOLIDATION_TYPES.get(consolidation, default_consolidated)
        segments = [m for m in members if m not in _CONSOLIDATION_MEMBERS]

        is_nil = value in ("", _NIL)
        number = None if is_nil or unit in ("", _NIL) else _to_float(value)

        if element_id not in label_cache:
            label_cache[element_id] = _english_label(element_id, taxonomy_index)

        fact_rows.append(
            {
                **doc_fields,
                "element_id": element_id,
                "japanese_label": label,
                "english_label": label_cache[element_id],
                "context_id": context_id,
                "start_date": start.isoformat() if start and not is_instant else None,
                "end_date": end.isoformat() if end and not is_instant else None,
                "instant_date": end.isoformat() if end and is_instant else None,
                "segments": ",".join(segments),
                "period": period,
                "quarter": "",
                "consolidated": consolidated,
                "previous_current": "",
                "forecast": "",
                "is_nil": is_nil,
                "value": number,
                "text": None if is_nil or number is not None else value,
            }
        )
    return fact_rows


def read_processed_doc_ids(output_path: Path) -> set[str]:
    if not output_path.exists():
        return set()
    existing = pl.read_csv(output_path, columns=["source_file"], infer_schema_length=0)
    return set(existing["source_file"].to_list())


def save_fact_rows(rows: list[dict], output_path: Path) -> None:
    if rows:
        append_and_save_csv(
            pl.DataFrame(rows, schema=ROW_SCHEMA), output_path, sort_col="filing_date"
        )


def append_document_to_csv(
    doc: dict,
    session: Session,
    taxonomy_index: TaxonomyIndex | None,
    output_dir: Path = OUTPUT_DIR,
    processed: set[str] | None = None,
) -> bool:
    """書類1件を取得・変換してCSVに追記する。追記した場合True。

    既にCSVのsource_file列にdocIDが記録されている場合は何もしない(冪等)。
    `processed`を渡すと既変換docIDの判定にそれを使い(CSVの再読込を省略)、追記後に更新する。
    取得できない書類は DocumentUnavailableError を送出する。
    """
    output_path = output_dir / f"{output_code(doc)}.csv"
    if processed is None:
        processed = read_processed_doc_ids(output_path)
    if doc["docID"] in processed:
        return False

    rows = build_fact_rows(doc, download_document_rows(doc["docID"], session), taxonomy_index)
    save_fact_rows(rows, output_path)
    processed.add(doc["docID"])
    return len(rows) > 0

import datetime
from pathlib import Path

from pydantic import BaseModel, Field


class DocumentType(BaseModel):
    """報告書の種類"""

    name: str
    aliases: list[str]  # 報告書の別名
    ident_categories: list[str]


class Document(BaseModel):
    """提出された報告書の情報"""

    filepath: Path
    doc_type: list[DocumentType]
    security_code: str
    filing_date: datetime.datetime
    fiscal_year_end: datetime.date
    # ファイル名からparseした文字列
    period: str = Field(description="報告の期間")  # one of `periods`
    consolidated: str = Field(description="連結/非連結")  # one of `consolidated_types`
    style: str = Field(description="報告書のスタイル")  # one of `report_styles`


class TaxonomyElement(BaseModel):
    japanese_label: str
    english_label: str
    element_id: str  # namespaceを含む完全なelement_id (例: "jpcrp_cor:NetSalesRevenue")
    period_type: str
    abstract: bool
    balance: str


class NumericData(BaseModel):
    document: Document
    element: TaxonomyElement
    context_id: str
    start_date: datetime.date | None
    end_date: datetime.date | None
    instant_date: datetime.date | None
    segments: list[str]
    period: str
    quarter: str
    consolidated: str
    forecast: str
    value: float


class NonNumericData(BaseModel):
    document: Document
    element: TaxonomyElement
    start_date: datetime.date | None
    end_date: datetime.date | None
    instant_date: datetime.date | None
    segments: list[str]
    context_id: str
    period: str
    quarter: str
    consolidated: str
    forecast: str
    value: str | bool

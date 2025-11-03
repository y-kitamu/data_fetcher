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
    period: str = Field(description="報告の期間")  # one of `periods`
    consolidated: str = Field(description="連結/非連結")  # one of `consolidated_types`
    style: str = Field(description="報告書のスタイル")  # one of `report_styles`
    security_code: str
    filing_date: datetime.date
    fiscal_year_end: datetime.date


class TaxonomyElement(BaseModel):
    japanese_label: str
    english_label: str
    namespace: str
    element_id: str
    period_type: str

    def __eq__(self, other) -> bool:
        if not isinstance(other, TaxonomyElement):
            return False
        return (
            self.japanese_label == other.japanese_label
            and self.english_label == other.english_label
            and self.namespace == other.namespace
            and self.element_id == other.element_id
        )


class NumericData(BaseModel):
    document: Document
    element: TaxonomyElement
    period: str
    quarter: str
    consolidated: str
    forecast: str
    value: float

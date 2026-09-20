"""タクソノミ要素のインデックスと照合。"""

from __future__ import annotations

from dataclasses import dataclass

from .constants.schema import TaxonomyElement


@dataclass
class Found:
    element: TaxonomyElement


@dataclass
class NotFound:
    pass


@dataclass
class Ambiguous:
    elements: list[TaxonomyElement]


LookupResult = Found | NotFound | Ambiguous


class TaxonomyIndex:
    """要素ID(`namespace:name`)からTaxonomyElementをO(1)で引くためのインデックス。"""

    def __init__(self, elements: list[TaxonomyElement]) -> None:
        self._by_id: dict[str, list[TaxonomyElement]] = {}
        for elem in elements:
            self._by_id.setdefault(elem.element_id, []).append(elem)

    @classmethod
    def from_elements(cls, elements: list[TaxonomyElement]) -> TaxonomyIndex:
        return cls(elements)

    def lookup(self, schema: str, name: str) -> LookupResult:
        matches = self._by_id.get(f"{schema}:{name}", [])
        if len(matches) == 0:
            return NotFound()
        if len(matches) > 1:
            return Ambiguous(elements=matches)
        return Found(element=matches[0])

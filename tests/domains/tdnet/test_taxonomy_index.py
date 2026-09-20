from data_fetcher.domains.tdnet.constants.schema import TaxonomyElement
from data_fetcher.domains.tdnet.taxonomy_index import Ambiguous, Found, NotFound, TaxonomyIndex


def _elem(element_id: str) -> TaxonomyElement:
    return TaxonomyElement(
        japanese_label="ラベル",
        english_label="Label",
        element_id=element_id,
        period_type="duration",
        abstract=False,
        balance="",
    )


def test_lookup_found():
    index = TaxonomyIndex.from_elements([_elem("jppfs_cor:NetSales")])
    result = index.lookup("jppfs_cor", "NetSales")
    assert isinstance(result, Found)
    assert result.element.element_id == "jppfs_cor:NetSales"


def test_lookup_not_found():
    index = TaxonomyIndex.from_elements([_elem("jppfs_cor:NetSales")])
    result = index.lookup("jppfs_cor", "NonExistentElement")
    assert isinstance(result, NotFound)


def test_lookup_ambiguous():
    index = TaxonomyIndex.from_elements(
        [_elem("jppfs_cor:Dup"), _elem("jppfs_cor:Dup")]
    )
    result = index.lookup("jppfs_cor", "Dup")
    assert isinstance(result, Ambiguous)
    assert len(result.elements) == 2

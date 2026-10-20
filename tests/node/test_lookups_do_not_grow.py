"""Looking up a CURIE that has no synonyms, descriptions or properties must not store anything.

SynonymFactory, DescriptionFactory and PropertyList each index their data in a defaultdict, and
write_compendium() looks up every identifier it writes. Indexing a defaultdict with ``[curie]`` stores an
empty entry for each miss, so a streamed compendium of tens of millions of identifiers that mostly have
none (Publication: ~41M PMIDs) grew by ~2 KB per identifier until the lookups switched to ``.get()``.
"""

from collections import defaultdict

import pytest

from src.node import DescriptionFactory, SynonymFactory
from src.predicates import HAS_ALTERNATIVE_ID, HAS_EXACT_SYNONYM
from src.properties import Property, PropertyList

pytestmark = pytest.mark.unit


def test_synonym_lookup_miss_stores_nothing():
    """get_synonyms() should return a known CURIE's synonyms and leave the index unchanged for an unknown one."""
    sf = object.__new__(SynonymFactory)
    sf.synonyms = {"PMID": defaultdict(set, {"PMID:1": {(HAS_EXACT_SYNONYM, "Water")}})}
    sf.common_synonyms = defaultdict(set)

    assert sf.get_synonyms(["PMID:1", "PMID:2"]) == {(HAS_EXACT_SYNONYM, "Water")}
    assert list(sf.synonyms["PMID"]) == ["PMID:1"]


def test_description_lookup_miss_stores_nothing():
    """get_descriptions() should return a known CURIE's descriptions and leave the index unchanged for an
    unknown one."""
    df = object.__new__(DescriptionFactory)
    df.descriptions = {"PMID": defaultdict(set, {"PMID:1": {"A description."}})}
    df.common_descriptions = defaultdict(list)

    descriptions = df.get_descriptions(["PMID:1", "PMID:2"])
    assert descriptions["PMID:1"] == {"A description."}
    assert not descriptions["PMID:2"]
    assert list(df.descriptions["PMID"]) == ["PMID:1"]


def test_property_lookup_miss_stores_nothing():
    """PropertyList.get_all() should return a known CURIE's properties and an empty set for an unknown one,
    without indexing it."""
    properties = PropertyList()
    prop = Property(curie="PMID:1", predicate=HAS_ALTERNATIVE_ID, value="PMID:100")
    properties.add_properties({prop})

    assert properties.get_all("PMID:1", HAS_ALTERNATIVE_ID) == {prop}
    assert properties.get_all("PMID:2", HAS_ALTERNATIVE_ID) == set()
    assert properties.get_all("PMID:2") == set()
    assert list(properties._properties_by_curie) == ["PMID:1"]

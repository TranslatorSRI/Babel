from collections import defaultdict

import pytest

from src.node import SynonymFactory
from src.util import get_config

BIOLINK_VERSION = get_config()["biolink_version"]

# Spelled out rather than imported from src.predicates: these tests assert the predicate load_synonyms() writes,
# which src/node.py takes from src.predicates, so importing it would compare the constant with itself.
HAS_EXACT_SYNONYM = "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym"


@pytest.mark.unit
def test_load_synonyms_single_column(tmp_path):
    """load_synonyms() must handle single-column lines (identifier with no label) without dropping them."""
    label_dir = tmp_path / "CHEMBL.COMPOUND"
    label_dir.mkdir()
    (label_dir / "labels").write_text("CHEMBL.COMPOUND:CHEMBL1\tWater\nCHEMBL.COMPOUND:CHEMBL2\n")
    sf = object.__new__(SynonymFactory)
    sf.synonym_dir = tmp_path
    sf.synonyms = {}
    sf.common_synonyms = defaultdict(set)

    sf.load_synonyms("CHEMBL.COMPOUND")

    assert (HAS_EXACT_SYNONYM, "Water") in sf.synonyms["CHEMBL.COMPOUND"]["CHEMBL.COMPOUND:CHEMBL1"]
    assert (HAS_EXACT_SYNONYM, "") in sf.synonyms["CHEMBL.COMPOUND"]["CHEMBL.COMPOUND:CHEMBL2"]

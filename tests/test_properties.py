"""Unit tests for src.properties.

Property's only validation gate is `supported_predicates`, so these cover what that set admits and
rejects, and that a value survives the JSONL round trip unaltered -- which matters most for the
chemical structure properties, whose values carry characters (semicolons, slashes, brackets,
backslashes) that a naive serializer or splitter would mangle.
"""

import gzip

import pytest

from src.predicates import (
    CHEMROF_CHARGE,
    CHEMROF_FORMULA,
    CHEMROF_INCHI,
    CHEMROF_INCHI_KEY,
    CHEMROF_MASS,
    CHEMROF_MONOISOTOPIC_MASS,
    CHEMROF_SMILES,
    HAS_ALTERNATIVE_ID,
    HAS_ROLE,
)
from src.properties import Property, PropertyList, supported_predicates


@pytest.mark.unit
@pytest.mark.parametrize(
    "predicate",
    [
        HAS_ALTERNATIVE_ID,
        HAS_ROLE,
        CHEMROF_SMILES,
        CHEMROF_INCHI,
        CHEMROF_INCHI_KEY,
        CHEMROF_FORMULA,
        CHEMROF_MASS,
        CHEMROF_MONOISOTOPIC_MASS,
        CHEMROF_CHARGE,
    ],
)
def test_supported_predicates_are_constructible(predicate):
    """Every predicate in the supported set must actually pass Property's own gate."""
    assert predicate in supported_predicates
    assert Property(curie="CHEBI:1", predicate=predicate, value="x").predicate == predicate


@pytest.mark.unit
def test_unsupported_predicate_raises():
    """The gate is the only thing standing between a typo'd predicate and a property file nothing
    can query, so it must fail loudly rather than accept an unknown URI."""
    with pytest.raises(ValueError, match="not supported"):
        Property(curie="CHEBI:1", predicate="https://example.org/made-up", value="x")


@pytest.mark.unit
def test_structure_values_survive_the_jsonl_round_trip(tmp_path):
    """A structure value must come back byte-identical.

    The InChI here is a real multi-component one -- its semicolons separate per-component layers and
    are part of the value. The SMILES carries backslashes and brackets. Both are the kind of string
    a line-based format mishandles quietly.
    """
    written = [
        Property(curie="CHEBI:29124", predicate=CHEMROF_INCHI, value="InChI=1S/2O.U/q;;+1", source="t"),
        Property(curie="CHEBI:3312", predicate=CHEMROF_SMILES, value=r"C/C=C\[N+]([O-])=O.[Na+]", source="t"),
    ]
    path = tmp_path / "props.jsonl.gz"
    with gzip.open(path, "wt") as f:
        for prop in written:
            f.write(prop.to_json_line())

    pl = PropertyList()
    assert pl.add_properties_jsonl_gz(str(path)) == 2
    assert pl.properties == set(written)
    assert {p.value for p in pl.get_all("CHEBI:29124", CHEMROF_INCHI)} == {"InChI=1S/2O.U/q;;+1"}


@pytest.mark.unit
def test_same_curie_carries_several_predicates():
    """Property.value is a single str, so a chemical's several structure facts are several rows;
    get_all() must be able to separate them again."""
    pl = PropertyList()
    pl.add_properties(
        {
            Property(curie="CHEBI:29124", predicate=CHEMROF_FORMULA, value="O2U"),
            Property(curie="CHEBI:29124", predicate=CHEMROF_CHARGE, value="1"),
            Property(curie="CHEBI:29124", predicate=HAS_ROLE, value="CHEBI:75771"),
        }
    )

    assert len(pl.get_all("CHEBI:29124")) == 3
    assert {p.value for p in pl.get_all("CHEBI:29124", CHEMROF_CHARGE)} == {"1"}

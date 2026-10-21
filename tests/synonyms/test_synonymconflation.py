"""Unit tests for src/synonyms/synonymconflation.py.

conflate_synonyms() merges the synonym records of cliques that a conflation file says are the same
thing: the lead clique's record is kept and the others' names, types, taxa and identifier counts are
folded into it, in the order the conflation file lists them. It drives two of Babel's most expensive
rules -- `geneprotein_conflated_synonyms` (15,719 s) and `drugchemical_conflated_synonyms`
(9,949 s) in the babel-1.18 benchmarks -- and had no tests before these.

The synonym and compendium records below are copied verbatim from a local anatomy build
(`babel_outputs/synonyms/AnatomicalEntity.txt.gz` and
`babel_outputs/compendia/AnatomicalEntity.txt`); their CURIEs are named in each constant so they
can be re-derived. Only the conflation file is authored here, because a conflation pairing anatomy
cliques does not occur in a real build -- it stands in for GeneProtein.txt, which has the same
`["primary", "secondary", ...]` JSONL shape.
"""

import gzip
import json

import pytest

from src.synonyms.synonymconflation import conflate_synonyms

# UMLS:C0733999 "Scapular line" -- the conflation primary. Clique also holds FMA:14613.
SCAPULAR_LINE_SYNONYM = {
    "curie": "UMLS:C0733999",
    "names": ["Scapular line", "Linea scapularis"],
    "types": ["AnatomicalEntity", "PhysicalEssence", "OntologyClass", "NamedThing", "Entity"],
    "preferred_name": "Scapular line",
    "shortest_name_length": 13,
    "clique_identifier_count": 2,
    "taxa": [],
    "taxon_specific": False,
}
SCAPULAR_LINE_CLIQUE = {
    "type": "biolink:AnatomicalEntity",
    "ic": None,
    "identifiers": [
        {"i": "UMLS:C0733999", "l": "Scapular line", "d": [], "t": []},
        {"i": "FMA:14613", "l": "", "d": [], "t": []},
    ],
    "preferred_name": "Scapular line",
    "taxa": [],
}

# UMLS:C1182464 "Right thyrocervical artery" -- the conflation secondary. Clique also holds FMA:70347.
THYROCERVICAL_SYNONYM = {
    "curie": "UMLS:C1182464",
    "names": ["Right thyrocervical artery"],
    "types": ["AnatomicalEntity", "PhysicalEssence", "OntologyClass", "NamedThing", "Entity"],
    "preferred_name": "Right thyrocervical artery",
    "shortest_name_length": 26,
    "clique_identifier_count": 2,
    "taxa": [],
    "taxon_specific": False,
}
THYROCERVICAL_CLIQUE = {
    "type": "biolink:AnatomicalEntity",
    "ic": None,
    "identifiers": [
        {"i": "UMLS:C1182464", "l": "Right thyrocervical artery", "d": [], "t": []},
        {"i": "FMA:70347", "l": "", "d": [], "t": []},
    ],
    "preferred_name": "Right thyrocervical artery",
    "taxa": [],
}

# UMLS:C0450491 "LI15" -- named in no conflation, so it must pass through untouched.
UNCONFLATED_SYNONYM = {
    "curie": "UMLS:C0450491",
    "names": ["LI15", "LI15 (body structure)"],
    "types": ["AnatomicalEntity", "PhysicalEssence", "OntologyClass", "NamedThing", "Entity"],
    "preferred_name": "LI15",
    "shortest_name_length": 4,
    "clique_identifier_count": 1,
    "taxa": [],
    "taxon_specific": False,
}
UNCONFLATED_CLIQUE = {
    "type": "biolink:AnatomicalEntity",
    "ic": None,
    "identifiers": [{"i": "UMLS:C0450491", "l": "LI15", "d": [], "t": []}],
    "preferred_name": "LI15",
    "taxa": [],
}


def write_jsonl(path, records, compress=False):
    """Write `records` as JSONL, gzipped if `compress`, and return the path."""
    text = "".join(json.dumps(record) + "\n" for record in records)
    if compress:
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.write(text)
    else:
        path.write_text(text)
    return path


def run_conflation(tmp_path, synonyms, cliques, conflations):
    """Run conflate_synonyms over the given records and return the output records."""
    synonyms_gz = write_jsonl(tmp_path / "synonyms.txt.gz", synonyms, compress=True)
    compendium = write_jsonl(tmp_path / "compendium.txt", cliques)
    conflation = write_jsonl(tmp_path / "conflation.txt", conflations)
    output = tmp_path / "conflated.txt.gz"

    conflate_synonyms([str(synonyms_gz)], [str(compendium)], [str(conflation)], str(output))

    with gzip.open(output, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


# CONFLATING SYNONYM RECORDS


@pytest.mark.unit
def test_synonyms_of_conflated_cliques_are_merged_into_the_lead_record(tmp_path):
    """Two conflated cliques should produce one record under the primary's CURIE, with the
    secondary's names appended after the primary's and the identifier counts summed."""
    records = run_conflation(
        tmp_path,
        [SCAPULAR_LINE_SYNONYM, THYROCERVICAL_SYNONYM],
        [SCAPULAR_LINE_CLIQUE, THYROCERVICAL_CLIQUE],
        [["UMLS:C0733999", "UMLS:C1182464"]],
    )

    assert len(records) == 1
    conflated = records[0]
    assert conflated["curie"] == "UMLS:C0733999"
    assert conflated["preferred_name"] == "Scapular line"
    # Primary's names first, in conflation order, then the secondary's.
    assert conflated["names"] == ["Scapular line", "Linea scapularis", "Right thyrocervical artery"]
    assert conflated["shortest_name_length"] == 13
    assert conflated["clique_identifier_count"] == 4
    assert conflated["taxon_specific"] is False


@pytest.mark.unit
def test_a_synonym_named_in_no_conflation_passes_through_unchanged(tmp_path):
    """A record whose CURIE appears in no conflation should be written out byte-for-byte as read,
    not rebuilt -- that is the path the overwhelming majority of records take."""
    records = run_conflation(
        tmp_path,
        [UNCONFLATED_SYNONYM, SCAPULAR_LINE_SYNONYM, THYROCERVICAL_SYNONYM],
        [UNCONFLATED_CLIQUE, SCAPULAR_LINE_CLIQUE, THYROCERVICAL_CLIQUE],
        [["UMLS:C0733999", "UMLS:C1182464"]],
    )

    passed_through = [record for record in records if record["curie"] == "UMLS:C0450491"]
    assert passed_through == [UNCONFLATED_SYNONYM]


@pytest.mark.unit
def test_a_synonym_keyed_on_a_non_leader_identifier_is_dropped(tmp_path):
    """A synonym keyed on a clique member the conflation file does not name is silently dropped.

    This pins current, wrong behaviour -- see https://github.com/NCATSTranslator/Babel/issues/989.
    Step 1.1 of conflate_synonyms is meant to map every identifier of a conflated clique onto that
    clique's preferred ID so records keyed on a non-leader identifier still get folded in. It does
    not work: `ids` (synonymconflation.py:87) is a one-shot `map` object, so the outer loop that
    searches for a conflated identifier consumes it and the inner loop that registers the clique
    sees only whatever came after the match. The matching identifier itself -- the one the
    conflation file names, and the one step 3 looks up -- is therefore never registered, so the
    expansion never fires and the record is neither merged nor passed through.

    **Invert this assertion when #989 is fixed**: FMA:70347's "Thyrocervical trunk" should appear
    in the conflated record's names, and nothing should be lost.
    """
    fma_keyed = dict(THYROCERVICAL_SYNONYM, curie="FMA:70347", names=["Thyrocervical trunk"])
    records = run_conflation(
        tmp_path,
        [SCAPULAR_LINE_SYNONYM, fma_keyed],
        [SCAPULAR_LINE_CLIQUE, THYROCERVICAL_CLIQUE],
        [["UMLS:C0733999", "UMLS:C1182464"]],
    )

    assert len(records) == 1
    assert records[0]["curie"] == "UMLS:C0733999"
    assert "Thyrocervical trunk" not in records[0]["names"]

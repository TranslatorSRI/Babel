"""Tests for SAPBERT training-data export."""

import gzip
import itertools
import json

import pytest

from src.exporters import sapbert
from src.exporters.sapbert import convert_synonyms_to_sapbert


def _write_synonyms(path, entries):
    with gzip.open(path, "wt", encoding="utf-8") as output:
        for entry in entries:
            output.write(json.dumps(entry) + "\n")


def _read_sapbert_rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as output:
        return [line.rstrip("\n").split("||") for line in output]


@pytest.mark.unit
def test_convert_synonyms_to_sapbert_skips_singleton_preferred_name_pair(tmp_path):
    """A CellLine-style entry whose only name equals its preferred name should not emit term||term."""
    synonym_file = tmp_path / "CellLine.txt.gz"
    sapbert_file = tmp_path / "sapbert" / "CellLine.txt.gz"
    _write_synonyms(
        synonym_file,
        [
            {
                "curie": "CLO:0022591",
                "preferred_name": "GM12814 cell",
                "names": ["GM12814 cell"],
                "types": ["CellLine"],
            }
        ],
    )

    convert_synonyms_to_sapbert(str(synonym_file), str(sapbert_file))

    assert _read_sapbert_rows(sapbert_file) == []


@pytest.mark.unit
def test_convert_synonyms_to_sapbert_keeps_singleton_distinct_from_preferred_name(tmp_path):
    """A one-name entry should still emit a row when the synonym differs from the preferred name."""
    synonym_file = tmp_path / "Disease.txt.gz"
    sapbert_file = tmp_path / "sapbert" / "Disease.txt.gz"
    _write_synonyms(
        synonym_file,
        [
            {
                "curie": "MONDO:0000001",
                "preferred_name": "Preferred Label",
                "names": ["alternate label"],
                "types": ["Disease"],
            }
        ],
    )

    convert_synonyms_to_sapbert(str(synonym_file), str(sapbert_file))

    assert _read_sapbert_rows(sapbert_file) == [
        ["biolink:Disease", "MONDO:0000001", "Preferred Label", "preferred label", "alternate label"]
    ]


@pytest.mark.unit
def test_convert_synonyms_to_sapbert_deduplicates_after_normalization(tmp_path):
    """Names that collapse after lowercasing and pipe cleanup should only contribute distinct pairs."""
    synonym_file = tmp_path / "Example.txt.gz"
    sapbert_file = tmp_path / "sapbert" / "Example.txt.gz"
    _write_synonyms(
        synonym_file,
        [
            {
                "curie": "EXAMPLE:1",
                "preferred_name": "Example Label",
                "names": ["Alpha||Beta", "alpha|beta", "Gamma"],
                "types": ["NamedThing"],
            }
        ],
    )

    convert_synonyms_to_sapbert(str(synonym_file), str(sapbert_file))

    rows = _read_sapbert_rows(sapbert_file)
    assert len(rows) == 1
    assert rows[0][:3] == ["biolink:NamedThing", "EXAMPLE:1", "Example Label"]
    assert set(rows[0][3:]) == {"alpha|beta", "gamma"}


@pytest.mark.unit
def test_convert_synonyms_to_sapbert_skips_singleton_pair_without_lowercasing(tmp_path, monkeypatch):
    """With LOWERCASE_ALL_NAMES off, a lone name identical to the preferred name should still be dropped."""
    monkeypatch.setattr(sapbert, "LOWERCASE_ALL_NAMES", False)
    synonym_file = tmp_path / "CellLine.txt.gz"
    sapbert_file = tmp_path / "sapbert" / "CellLine.txt.gz"
    _write_synonyms(
        synonym_file,
        [
            {
                "curie": "CLO:0022591",
                "preferred_name": "GM12814 cell",
                "names": ["GM12814 cell"],
                "types": ["CellLine"],
            }
        ],
    )

    convert_synonyms_to_sapbert(str(synonym_file), str(sapbert_file))

    assert _read_sapbert_rows(sapbert_file) == []


@pytest.mark.unit
def test_convert_synonyms_to_sapbert_cleans_pipes_in_preferred_name(tmp_path):
    """A preferred name containing '||' should be collapsed to a single pipe so the row keeps five columns."""
    synonym_file = tmp_path / "Example.txt.gz"
    sapbert_file = tmp_path / "sapbert" / "Example.txt.gz"
    _write_synonyms(
        synonym_file,
        [
            {
                "curie": "EXAMPLE:2",
                "preferred_name": "Alpha||Beta",
                "names": ["one", "two"],
                "types": ["NamedThing"],
            }
        ],
    )

    convert_synonyms_to_sapbert(str(synonym_file), str(sapbert_file))

    rows = _read_sapbert_rows(sapbert_file)
    assert len(rows) == 1
    assert rows[0][:3] == ["biolink:NamedThing", "EXAMPLE:2", "Alpha|Beta"]
    assert set(rows[0][3:]) == {"one", "two"}


@pytest.mark.unit
def test_convert_synonyms_to_sapbert_deduplicates_pairs_within_biolink_type(tmp_path):
    """The same normalized name pair should be written once per Biolink type across the output file."""
    synonym_file = tmp_path / "Mixed.txt.gz"
    sapbert_file = tmp_path / "sapbert" / "Mixed.txt.gz"
    _write_synonyms(
        synonym_file,
        [
            {
                "curie": "MONDO:0000001",
                "preferred_name": "Disease One",
                "names": ["shared one", "shared two"],
                "types": ["Disease"],
            },
            {
                "curie": "MONDO:0000002",
                "preferred_name": "Disease Two",
                "names": ["shared two", "shared one"],
                "types": ["Disease"],
            },
            {
                "curie": "HP:0000001",
                "preferred_name": "Phenotype One",
                "names": ["shared one", "shared two"],
                "types": ["PhenotypicFeature"],
            },
        ],
    )

    convert_synonyms_to_sapbert(str(synonym_file), str(sapbert_file))

    rows = _read_sapbert_rows(sapbert_file)
    assert len(rows) == 2
    assert rows[0][:3] == ["biolink:Disease", "MONDO:0000001", "Disease One"]
    assert rows[1][:3] == ["biolink:PhenotypicFeature", "HP:0000001", "Phenotype One"]
    assert {frozenset(row[3:]) for row in rows} == {frozenset({"shared one", "shared two"})}


# SYNONYM PAIR SAMPLING


@pytest.mark.unit
def test_sample_name_pairs_returns_every_pair_when_under_the_cap():
    """A clique with fewer pairs than the cap should contribute all of them."""
    names = ["a", "b", "c", "d"]
    assert sorted(sapbert.sample_name_pairs(names, 50)) == sorted(itertools.combinations(names, 2))


@pytest.mark.unit
@pytest.mark.parametrize("count_names", [12, 40, 500])
def test_sample_name_pairs_samples_distinct_pairs_over_the_cap(count_names):
    """Above the cap, both the enumerating and the rejection-sampling branch return distinct real pairs."""
    names = [f"name {i}" for i in range(count_names)]
    name_pairs = sapbert.sample_name_pairs(names, 50)

    assert len(name_pairs) == 50
    assert len({tuple(sorted(name_pair)) for name_pair in name_pairs}) == 50
    for first, second in name_pairs:
        assert first in names
        assert second in names
        assert first != second

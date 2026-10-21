"""Tests for src/metadata/provenance.py."""

import pytest
import yaml

from src.metadata.provenance import write_concord_metadata

# CONCORD METADATA


@pytest.mark.unit
def test_concord_metadata_counts_rows(tmp_path):
    """Should count concords, distinct CURIEs, predicates and prefix pairs from a well-formed file."""
    concord = tmp_path / "NCIT_UniProtKB"
    concord.write_text("NCIT:C150023\teq\tUniProtKB:Q9C0C9\nNCIT:C179160\teq\tUniProtKB:Q15119\n")
    metadata = tmp_path / "metadata.yaml"
    write_concord_metadata(metadata, name="test", concord_filename=concord)
    counts = yaml.safe_load(metadata.read_text())["counts"]["concords"]
    assert counts["count_concords"] == 2
    assert counts["count_distinct_curies"] == 4
    assert counts["prefix_counts"] == {"eq(NCIT, UniProtKB)": 2}


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_row",
    [
        "NCIT:C150023\teq\n",  # two columns
        "NCIT:C150023\teq\tUniProtKB:Q9C0C9\textra\n",  # four columns
        "\n",  # blank line
    ],
)
def test_concord_metadata_raises_on_a_row_without_three_columns(tmp_path, bad_row):
    """Should raise, naming the file and line, rather than skip a row that isn't three columns."""
    concord = tmp_path / "concord"
    concord.write_text("NCIT:C179160\teq\tUniProtKB:Q15119\n" + bad_row)
    with pytest.raises(ValueError, match=r"concord:2 has \d tab-separated columns, not 3"):
        write_concord_metadata(tmp_path / "metadata.yaml", name="test", concord_filename=concord)

"""Near-end-to-end tests for `write_compendium()` on disease and phenotype cliques.

Each test feeds a small clique into `write_compendium()` and asserts on the
JSONL `preferred_name` in the output file. Labels for the CURIEs used here
live in `tests/fixtures/compendium/babel_downloads/{PREFIX}/labels`.

Issue context:
  https://github.com/NCATSTranslator/Babel/issues/597 — original report
  https://github.com/NCATSTranslator/Babel/issues/711 — HP:0001508 demoted
  https://github.com/NCATSTranslator/Babel/issues/714 — MONDO:0011479 demoted
  https://github.com/NCATSTranslator/Babel/issues/723 — MONDO:0005578 demoted
"""

import pytest

from src.babel_utils import write_compendium

from .conftest import assert_preferred_name


@pytest.mark.unit
def test_pots_label_not_demoted(babel_test_env):
    """MONDO:0011479 "postural orthostatic tachycardia syndrome" (40 chars) must
    not be demoted to UMLS:C2930833 "Irritable heart" (14 chars).
    https://github.com/NCATSTranslator/Babel/issues/714
    """
    write_compendium(
        metadata_yamls=[],
        synonym_list=[{"MONDO:0011479", "UMLS:C2930833"}],
        ofname="POTS.txt",
        node_type="biolink:Disease",
        labels={},
        icrdf_filename=babel_test_env.icrdf_path,
    )
    [record] = babel_test_env.read_records("POTS.txt")
    assert_preferred_name(record, "postural orthostatic tachycardia syndrome")


@pytest.mark.unit
def test_failure_to_thrive_not_demoted(babel_test_env):
    """HP:0001508 "Failure to thrive" (18 chars) must not be demoted to
    UMLS:C4531021 "Undergrowth" (10 chars).
    https://github.com/NCATSTranslator/Babel/issues/711
    """
    write_compendium(
        metadata_yamls=[],
        synonym_list=[{"HP:0001508", "UMLS:C4531021"}],
        ofname="FailureToThrive.txt",
        node_type="biolink:PhenotypicFeature",
        labels={},
        icrdf_filename=babel_test_env.icrdf_path,
    )
    [record] = babel_test_env.read_records("FailureToThrive.txt")
    assert_preferred_name(record, "Failure to thrive")


@pytest.mark.unit
def test_arthritic_joint_disease_not_demoted(babel_test_env):
    """MONDO:0005578 "arthritic joint disease" (22 chars) must not be demoted to
    DOID:848 "arthritis" (9 chars).
    https://github.com/NCATSTranslator/Babel/issues/723
    """
    write_compendium(
        metadata_yamls=[],
        synonym_list=[{"MONDO:0005578", "DOID:848"}],
        ofname="ArthriticJointDisease.txt",
        node_type="biolink:Disease",
        labels={},
        icrdf_filename=babel_test_env.icrdf_path,
    )
    [record] = babel_test_env.read_records("ArthriticJointDisease.txt")
    assert_preferred_name(record, "arthritic joint disease")

"""Pipeline tests for the GARD_label concord, run against a real built concord.

Skipped by default unless pytest is run with --pipeline.  Run with:
    uv run pytest tests/pipeline/test_gard_label.py --pipeline --no-cov -v

GARD_label is the only concord in this pipeline derived from labels rather than an asserted mapping,
which is safe only because of the three guards in ``build_gard_label_concord()``'s docstring. The
unit tests pin the guards against synthetic inputs; these say whether the real registry and the real
vocabularies still have the properties the guards assume. See docs/sources/GARD/label-matches/README.md.
"""

import collections
import os

import pytest

from src.prefixes import GARD
from src.util import Text, get_config
from tests.pipeline.conftest import _intermediate_concord_path


@pytest.fixture
def gard_label_concord():
    """The built disease/concords/GARD_label file, or skip if this checkout has no disease build."""
    path = _intermediate_concord_path("diseasephenotype", "GARD_label")
    if not os.path.exists(path):
        pytest.skip(f"{path} not built; run `uv run snakemake -c all {path}` first")
    return path


def _pairs(concord_path):
    with open(concord_path) as inf:
        return [tuple(line.rstrip("\n").split("\t")) for line in inf]


@pytest.mark.pipeline
def test_gard_label_rows_are_well_formed(gard_label_concord):
    """Every row is an unpadded GARD subject and a target from the configured match pool.

    A padded subject (GARD:0027461) would join neither GARD's own ids file nor MONDO's and DOID's
    unpadded xrefs, so the row would look present and do nothing -- the failure mode
    normalize_gard_curie() exists to prevent.
    """
    pairs = _pairs(gard_label_concord)
    assert pairs, "GARD_label is empty; either the registry is fully mapped now or the match broke"

    # Compared case-insensitively: disease_gard_label_match_prefixes names ids/labels *directories*
    # ("Orphanet"), while the CURIEs those files carry use Babel's prefix spelling ("orphanet:").
    pool = {prefix.upper() for prefix in get_config()["disease_gard_label_match_prefixes"]}
    for subject, predicate, target in pairs:
        assert Text.get_prefix_or_none(subject) == GARD, f"non-GARD subject: {subject}"
        assert not subject.split(":", 1)[1].startswith("0"), f"zero-padded GARD subject: {subject}"
        assert predicate == "xref", f"unexpected predicate: {predicate}"
        assert Text.get_prefix_or_none(target) in pool, f"target outside the match pool: {target}"


@pytest.mark.pipeline
def test_gard_label_emits_at_most_one_row_per_gard_id(gard_label_concord):
    """Guard 2, on the real concord.

    Each subject is a GARD id no other concord names, so it is a single-identifier clique and one
    pair can only union it into the target's clique. A second row for the same subject breaks that:
    the two rows would connect their two targets through the GARD id, fusing pre-existing cliques.
    This is the invariant the whole design rests on, so it is checked here as well as in the unit
    tests -- a bug in the emit loop would be invisible in the compendium until a clique diff caught
    it.
    """
    counts = collections.Counter(subject for subject, _, _ in _pairs(gard_label_concord))
    repeated = {subject: n for subject, n in counts.items() if n > 1}
    assert not repeated, f"GARD ids with more than one label-match row: {dict(list(repeated.items())[:5])}"


@pytest.mark.pipeline
def test_gard_label_subjects_appear_in_no_other_disease_concord(gard_label_concord):
    """Guard 1, on the real concord: every subject is a GARD id no other concord places.

    Read from the concord files rather than from MONDO_GARD and DOID by name, so a source that
    starts emitting GARD xrefs is covered here too. If this fails, GARD_label is re-deciding a GARD
    id that already sits in a curated clique, and glom() will drop the pair (both cliques hold a
    unique prefix) or fuse them (neither does) depending on the pair -- neither outcome intended.
    """
    subjects = {subject for subject, _, _ in _pairs(gard_label_concord)}
    for concord in get_config()["disease_concords"]:
        if concord == "GARD_label":
            continue
        path = _intermediate_concord_path("diseasephenotype", concord)
        if not os.path.exists(path):
            pytest.skip(f"{path} not built")
        with open(path) as inf:
            claimed = {curie for line in inf for curie in line.rstrip("\n").split("\t") if curie in subjects}
        assert not claimed, f"{concord} already places GARD ids GARD_label matched: {sorted(claimed)[:5]}"

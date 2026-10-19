"""Pipeline tests for the MONDO_GARD concord, run against a real built concord.

Skipped by default unless pytest is run with --pipeline.  Run with:
    uv run pytest tests/pipeline/test_mondo_gard.py --pipeline --no-cov -v

MONDO_GARD is the one place this pipeline reads `oboInOwl:hasDbXref`, and it has no bad-xrefs file.
The build enforces the 1:1 property it relies on (OVERUSE_FILTERED_CONCORDS drops a GARD id two MONDO
terms claim); these assertions say whether the *source* still has that property, so a MONDO release
that loses it is noticed rather than silently filtered. See docs/sources/MONDO/README.md.
"""

import collections
import os

import pytest

from src.prefixes import GARD, MONDO
from tests.pipeline.conftest import _intermediate_concord_path


@pytest.fixture
def mondo_gard_concord():
    """The built disease/concords/MONDO_GARD file, or skip if this checkout has no disease build."""
    path = _intermediate_concord_path("diseasephenotype", f"{MONDO}_{GARD}")
    if not os.path.exists(path):
        pytest.skip(f"{path} not built; run `uv run snakemake -c all get_disease_obo_relationships` first")
    return path


def _pairs(concord_path):
    with open(concord_path) as inf:
        return [tuple(line.rstrip("\n").split("\t")) for line in inf]


@pytest.mark.pipeline
def test_mondo_gard_targets_only_gard(mondo_gard_concord):
    """The allowlist is fail-closed at exactly one prefix.

    MONDO's other hasDbXref targets are not equivalences -- ICD9, ICD-O and icd11.foundation name
    disease families, HP crosses the disease/phenotype line -- so a target from any other namespace
    means `allowed_prefixes` was widened without the review that should accompany it.
    """
    pairs = _pairs(mondo_gard_concord)
    assert pairs, "MONDO_GARD is empty; the UberGraph query returned nothing"
    subject_prefixes = {s.split(":", 1)[0] for s, _, _ in pairs}
    target_prefixes = {o.split(":", 1)[0] for _, _, o in pairs}
    assert subject_prefixes == {MONDO}, f"unexpected subject prefixes: {sorted(subject_prefixes)}"
    assert target_prefixes == {GARD}, f"unexpected target prefixes: {sorted(target_prefixes)}"


@pytest.mark.pipeline
def test_mondo_gard_is_one_to_one(mondo_gard_concord):
    """No GARD id claimed by two MONDO terms, and no MONDO term claiming two GARD ids.

    This is the property that makes it safe to glom these rows unfiltered. A many-to-one target is
    how DOID's ICD codes fused 61 mutually-exclusive subtypes into one clique (#1031); if this ever
    fails, the concord needs `remove_overused_xrefs` scoping rather than a bigger allowlist.
    """
    by_target = collections.defaultdict(set)
    by_subject = collections.defaultdict(set)
    for subject, _predicate, target in _pairs(mondo_gard_concord):
        by_target[target].add(subject)
        by_subject[subject].add(target)

    overused = {t: sorted(s) for t, s in by_target.items() if len(s) > 1}
    multi = {s: sorted(t) for s, t in by_subject.items() if len(t) > 1}
    assert not overused, f"GARD ids claimed by more than one MONDO term: {dict(list(overused.items())[:5])}"
    assert not multi, f"MONDO terms with more than one GARD id: {dict(list(multi.items())[:5])}"


@pytest.mark.pipeline
def test_mondo_gard_ids_are_unpadded(mondo_gard_concord):
    """MONDO writes the registry's zero-padded form; normalize_gard_curie must have stripped it.

    A padded id here joins neither GARD's own ids file nor DOID's xrefs, so the row would look
    present and do nothing.
    """
    padded = [o for _, _, o in _pairs(mondo_gard_concord) if o.split(":", 1)[1].startswith("0")]
    assert not padded, f"{len(padded)} zero-padded GARD targets survived normalization: {padded[:5]}"

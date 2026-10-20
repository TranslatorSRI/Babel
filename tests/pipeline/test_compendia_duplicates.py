"""Finished-compendia duplicate checks: identifiers shared between Protein and the chemical compendia.

The ids-file partition tests (test_vocabulary_partitioning.py, test_umls.py) compare what each
pipeline *claims*; they pass while the finished compendia still overlap, because cross-references
re-join identifiers the ids files kept apart (Babel issues #308, #513 --
docs/sources/UMLS/ProteinChemicalDuplicates.md). These tests read the two duplicate reports a
finished build writes and count the rows that span Protein and a chemical compendium.

They pin known-imperfect behaviour, per tests/CLAUDE.md: while the duplicates exist the tests
xfail with the count, and they FAIL if the count grows past the 2026jul22 baseline, so a change
that makes the overlap worse is noticed. When the fix in #849's plan lands, the xfail disappears on
its own; then delete the baselines.

They are pipeline tests, skipped by default. They need a build directory holding
`reports/umls/duplicate-curies.csv` and `reports/duckdb/duplicate_curies.tsv` -- a local
`babel_outputs/` (the default) or any build directory passed as `--build-dir`, e.g. one copied from
https://stars.renci.org/var/babel_outputs/<build>/. Run with:
    uv run pytest tests/pipeline/test_compendia_duplicates.py --pipeline --no-cov -v --build-dir data/2026jul22
"""

import os

import pytest

from src.reports.duplicate_curie_routes import (
    DUCKDB_DUPLICATES_REPORT,
    UMLS_DUPLICATES_REPORT,
    chemical_compendia,
    load_duckdb_duplicates,
    load_umls_duplicates,
    spans_pair,
)

# 2026jul22 counts (docs/sources/UMLS/protein-chemical-duplicates/2026jul22.md). Lower them when a
# change reduces the overlap; delete them once the count reaches zero.
BASELINE_UMLS_CUIS = 542
BASELINE_MESH_DESCRIPTORS = 17631

TRACKING_ISSUE = "https://github.com/NCATSTranslator/Babel/issues/308"


def _report_or_skip(build_dir, relative_path):
    path = os.path.join(build_dir, relative_path)
    if not os.path.exists(path):
        pytest.skip(f"{path} not found; pass --build-dir pointing at a finished build")
    return path


@pytest.mark.pipeline
def test_no_umls_cuis_shared_between_protein_and_chemicals(build_dir):
    """No UMLS CUI should be in both Protein and a chemical compendium (#308); pinned at the 2026jul22 count."""
    rows = load_umls_duplicates(_report_or_skip(build_dir, UMLS_DUPLICATES_REPORT))
    shared = [row["umls_curie"] for row in rows if spans_pair(row["compendia"], "Protein", chemical_compendia())]
    assert len(shared) <= BASELINE_UMLS_CUIS, (
        f"{len(shared)} UMLS CUIs are in both Protein and a chemical compendium, up from {BASELINE_UMLS_CUIS} "
        f"in 2026jul22: {sorted(shared)[:10]} (see {TRACKING_ISSUE})"
    )
    if shared:
        pytest.xfail(f"{len(shared)} UMLS CUIs still shared between Protein and chemicals (see {TRACKING_ISSUE})")


@pytest.mark.pipeline
def test_no_mesh_descriptors_shared_between_protein_and_chemicals(build_dir):
    """No MeSH descriptor should be in both Protein and a chemical compendium (#308); pinned at the 2026jul22 count."""
    rows = load_duckdb_duplicates(_report_or_skip(build_dir, DUCKDB_DUPLICATES_REPORT))
    shared = [
        row["curie"]
        for row in rows
        if row["curie"].startswith("MESH:") and spans_pair(row["compendia"], "Protein", chemical_compendia())
    ]
    assert len(shared) <= BASELINE_MESH_DESCRIPTORS, (
        f"{len(shared)} MeSH descriptors are in both Protein and a chemical compendium, up from "
        f"{BASELINE_MESH_DESCRIPTORS} in 2026jul22: {sorted(shared)[:10]} (see {TRACKING_ISSUE})"
    )
    if shared:
        pytest.xfail(
            f"{len(shared)} MeSH descriptors still shared between Protein and chemicals (see {TRACKING_ISSUE})"
        )

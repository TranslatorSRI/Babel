"""Unit tests for the source-impact detail files (CSV/JSON/TSV).

Drives the real ``src.tools.source_impact_report.cli.main`` over a synthetic intermediate root
(offline, so ``unit``-marked) and asserts the six detail files written into the report's
``<output-stem>/`` subdirectory are correct, complete, and deterministic.

The synthetic source ``NEWSOURCE`` is arranged against an ``EXISTING`` Babel set to yield:
two pure-new singleton cliques, one expanded clique (a structurally-new member), and one
merged clique — so every detail file has content to assert on.

Test groups
-----------
- Content correctness: detail files contain the expected rows and values.
- Determinism: two runs over the same inputs produce byte-identical output.
- CLI flags: ``--no-detail-files`` skips the subdirectory entirely.
"""

import csv
import json

import pytest

from src.reports.source_impact_details import (
    NEW_CLIQUES_CSV,
    NEW_CLIQUES_FULL_CSV,
    NEW_XREFS_FULL_CSV,
    NEW_XREFS_SUMMARY_CSV,
)
from src.tools.source_impact_report.cli import main


def _write(path, text):
    """Create parent directories as needed and write *text* to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


@pytest.fixture
def synthetic_intermediate(tmp_path):
    """Populate a minimal two-source anatomy intermediate tree under *tmp_path*.

    EXISTING contributes four ids; NEWSOURCE contributes four ids and three concord rows
    that join NEWSRC:2 into an existing clique (expanded), and NEWSRC:3 into two existing
    cliques simultaneously (merged), leaving NEWSRC:1 and NEWSRC:4 as pure-new singletons.
    """
    anatomy = tmp_path / "intermediate" / "anatomy"
    _write(
        anatomy / "ids" / "EXISTING",
        "UBERON:0001\tbiolink:AnatomicalEntity\n"
        "UBERON:0002\tbiolink:AnatomicalEntity\n"
        "GO:0000003\tbiolink:CellularComponent\n"
        "UBERON:0010\tbiolink:AnatomicalEntity\n",
    )
    _write(
        anatomy / "ids" / "NEWSOURCE",
        "NEWSRC:1\tbiolink:AnatomicalEntity\n"
        "NEWSRC:2\tbiolink:AnatomicalEntity\n"
        "NEWSRC:3\tbiolink:AnatomicalEntity\n"
        "NEWSRC:4\tbiolink:GrossAnatomicalStructure\n",
    )
    _write(
        anatomy / "concords" / "NEWSOURCE",
        "NEWSRC:2\txref\tUBERON:0001\nNEWSRC:3\txref\tUBERON:0002\nNEWSRC:3\txref\tGO:0000003\n",
    )
    return {"intermediate_root": tmp_path / "intermediate", "source": "NEWSOURCE"}


def _run(synthetic_intermediate, output, extra=()):
    """Invoke ``main`` in synthetic mode against *synthetic_intermediate* and return the exit code."""
    return main(
        [
            "--source",
            synthetic_intermediate["source"],
            "--mode",
            "synthetic",
            "--intermediate-root",
            str(synthetic_intermediate["intermediate_root"]),
            "--output",
            str(output),
            "--format",
            "md",
            "--no-biolink-lookup",  # keep the test fully offline
            *extra,
        ]
    )


def _read_csv(path):
    """Read a CSV file at *path* and return its rows as a list of dicts."""
    with path.open() as f:
        return list(csv.DictReader(f))


@pytest.mark.unit
def test_detail_files_written_with_expected_content(synthetic_intermediate, tmp_path):
    """All four detail files are created and contain the rows expected from the synthetic fixture.

    Checks new-cliques-top-N.csv (two pure-new singletons), modified-cliques.csv (one expanded, one
    merged row), modified-cliques.json (full structure including before_clique_leaders),
    new-xrefs.csv (one row per concord row) and new-xrefs-summary.csv (the two join pathways
    those rows group into).
    """
    output = tmp_path / "impact-report.md"
    assert _run(synthetic_intermediate, output) == 0

    details = tmp_path / "impact-report"
    assert details.is_dir()

    # new-cliques-top-N.csv — the two pure-new singletons (NEWSRC:1, NEWSRC:4), both under the cap.
    new_cliques = _read_csv(details / NEW_CLIQUES_CSV)
    ids = {r["preferred_id"] for r in new_cliques}
    assert ids == {"NEWSRC:1", "NEWSRC:4"}
    assert all(r["member_count"] == "1" for r in new_cliques)

    # modified-cliques.csv — one row per added/preexisting identifier. The expanded clique
    # gains NEWSRC:2 and the merge is bridged by NEWSRC:3, both structurally new.
    modified = _read_csv(details / "modified-cliques.csv")
    added = {r["added_id"] for r in modified if r["added_kind"] == "added"}
    assert added == {"NEWSRC:2", "NEWSRC:3"}
    change_kinds = {r["added_id"]: r["change_kind"] for r in modified}
    assert change_kinds["NEWSRC:2"] == "expanded"
    assert change_kinds["NEWSRC:3"] == "merged"

    # modified-cliques.json — full structure for the expanded + merged clique.
    entries = json.loads((details / "modified-cliques.json").read_text())
    assert {e["change_kind"] for e in entries} == {"expanded", "merged"}
    merged_entry = next(e for e in entries if e["change_kind"] == "merged")
    assert sorted(merged_entry["before_clique_leaders"]) == ["GO:0000003", "UBERON:0002"]
    assert "NEWSRC:3" in merged_entry["added_source_curies"]

    # new-xrefs.csv — the three rows from NEWSOURCE's own concord, all "added".
    full_xrefs = _read_csv(details / NEW_XREFS_FULL_CSV)
    assert len(full_xrefs) == 3
    assert all(r["asserted_by"] == "NEWSOURCE" for r in full_xrefs)
    assert all(r["status"] == "added" for r in full_xrefs)

    # new-xrefs-summary.csv — those rows grouped into join pathways: NEWSRC->UBERON (NEWSRC:2 and
    # NEWSRC:3) and NEWSRC->GO (NEWSRC:3). Both groups are under the example budget, so all three
    # rows survive as examples, biggest pathway first.
    summary = _read_csv(details / NEW_XREFS_SUMMARY_CSV)
    pathways = {(r["prefix_1"], r["prefix_2"]): int(r["xref_count"]) for r in summary}
    assert pathways == {("NEWSRC", "UBERON"): 2, ("GO", "NEWSRC"): 1}
    assert len(summary) == 3
    assert all(r["predicate"] == "xref" and r["status"] == "added" for r in summary)
    assert int(summary[0]["xref_count"]) == 2

    # The full new-cliques table is written alongside the capped one; both are tiny here.
    assert len(_read_csv(details / NEW_CLIQUES_FULL_CSV)) == 2


@pytest.mark.unit
def test_detail_files_are_deterministic(synthetic_intermediate, tmp_path):
    """Two runs over the same intermediate tree produce byte-identical detail files."""
    out_a = tmp_path / "a" / "impact-report.md"
    out_b = tmp_path / "b" / "impact-report.md"
    assert _run(synthetic_intermediate, out_a) == 0
    assert _run(synthetic_intermediate, out_b) == 0
    for fname in (
        NEW_CLIQUES_CSV,
        NEW_CLIQUES_FULL_CSV,
        "modified-cliques.csv",
        "modified-cliques.json",
        NEW_XREFS_SUMMARY_CSV,
        NEW_XREFS_FULL_CSV,
    ):
        a = (tmp_path / "a" / "impact-report" / fname).read_bytes()
        b = (tmp_path / "b" / "impact-report" / fname).read_bytes()
        assert a == b, f"{fname} differs between runs — output is not deterministic"


@pytest.mark.unit
def test_no_detail_files_flag_skips_subdirectory(synthetic_intermediate, tmp_path):
    """Passing ``--no-detail-files`` writes the report markdown but skips the detail subdirectory."""
    output = tmp_path / "impact-report.md"
    assert _run(synthetic_intermediate, output, extra=("--no-detail-files",)) == 0
    assert output.exists()
    assert not (tmp_path / "impact-report").exists()


# --- which detail files the report links -----------------------------------------
#
# The report writes six detail files and .gitignore commits only two of them, so a *link* to one of
# the other four resolves on the machine that generated the report and nowhere else.


@pytest.mark.unit
def test_report_links_only_the_detail_files_the_repo_commits():
    """A link is emitted for a committed detail file and withheld for a gitignored one.

    tests/test_docs_links.py catches the symptom -- a committed report linking a file no checkout
    has -- but only once such a report is committed, and only on a machine that has not just
    generated it locally. This pins the renderer itself, and pins COMMITTED_DETAIL_FILES against
    .gitignore so the two cannot drift apart silently.
    """
    from src.reports.source_impact import COMMITTED_DETAIL_FILES, _detail_link
    from src.reports.source_impact_details import (
        MODIFIED_CLIQUES_CSV,
        NEW_CLIQUES_CSV,
        NEW_CLIQUES_FULL_CSV,
        NEW_XREFS_FULL_CSV,
        NEW_XREFS_SUMMARY_CSV,
    )
    from src.util import get_repo_root

    committed = _detail_link("impact-report", NEW_CLIQUES_CSV, "Sample of new cliques")
    assert f"[`impact-report/{NEW_CLIQUES_CSV}`](impact-report/{NEW_CLIQUES_CSV})" in committed

    for filename in (MODIFIED_CLIQUES_CSV, NEW_CLIQUES_FULL_CSV, NEW_XREFS_FULL_CSV):
        line = _detail_link("impact-report", filename, "Full list")
        assert f"`impact-report/{filename}`" in line, f"{filename} should still be named"
        assert f"]({filename})" not in line and f"](impact-report/{filename})" not in line, (
            f"{filename} is gitignored; naming it is fine but linking it resolves nowhere"
        )
        assert "not committed" in line and "source-impact-report" in line, (
            "an uncommitted file's bullet must say so and how to regenerate it"
        )

    assert _detail_link(None, NEW_CLIQUES_CSV, "Sample") is None, "no bullet when no details dir"

    # The set the renderer links must be exactly the set .gitignore does not exclude.
    gitignored = {
        line.strip().rsplit("/", 1)[-1]
        for line in (get_repo_root() / ".gitignore").read_text().splitlines()
        if line.strip().startswith("docs/sources/*/impact-report/")
    }
    assert COMMITTED_DETAIL_FILES == {NEW_CLIQUES_CSV, NEW_XREFS_SUMMARY_CSV}
    assert not (COMMITTED_DETAIL_FILES & gitignored), (
        f"COMMITTED_DETAIL_FILES names a file .gitignore excludes: {sorted(COMMITTED_DETAIL_FILES & gitignored)}"
    )

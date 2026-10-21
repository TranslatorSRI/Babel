"""Tests for the release-note drafting script (``releases/scripts/draft_release_notes.py``).

Covers the two parts that are easy to get quietly wrong: which PRs are folded away as routine, and
the number formatting in the `Summary of changes` table (a dropped `\\-` escape or a mis-shaped
from-zero row is invisible until the published note renders).
"""

import http.client
import importlib.util
import json
import sys

import pytest

from src.util import get_repo_root

_SCRIPT = get_repo_root() / "releases" / "scripts" / "draft_release_notes.py"


def _load_module():
    """Import the script by path: `releases/` is deliberately not a package under `src/`."""
    spec = importlib.util.spec_from_file_location("draft_release_notes", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


drn = _load_module()


def _pr(title, author="gaurav", number=1, title_known=True):
    return drn.PullRequest("Babel", "NCATSTranslator/Babel", number, title, author, title_known)


@pytest.mark.unit
@pytest.mark.parametrize(
    "title,author,expected",
    [
        # Real changes a consumer should read about.
        ("Add MP, kept disjoint from HP, with an xref allowlist and HP/MP taxa", "gaurav", False),
        ("Type DrugBank foods as biolink:Food, extracts as ComplexMolecularMixture (#828)", "gaurav", False),
        # "Update" alone is substantive; only "Update <dep> requirement from ..." is routine.
        ("Update node_normalizer/server.py to return a 400", "gaurav", False),
        # Routine: version increments, dependency bumps, merge commits, and anything from a bot.
        ("Increment version to Babel v1.17", "gaurav", True),
        ("Incremented version to v1.6.2", "gaurav", True),
        ("build(deps-dev): bump snakefmt from 2.0.2 to 2.0.3", "dependabot[bot]", True),
        ("Update setuptools requirement from >=64 to >=83.0.0", "gaurav", True),
        ("Upgraded packages using `uv sync --upgrade`", "gaurav", True),
        ("Merge pull request #123 from NCATSTranslator/some-branch", "gaurav", True),
        ("Something a bot did that we cannot pattern-match", "dependabot[bot]", True),
    ],
)
def test_routine_classification(title, author, expected):
    assert _pr(title, author).is_routine is expected


@pytest.mark.unit
def test_a_pr_with_no_known_title_is_never_routine():
    """A merge commit's subject matches ROUTINE_TITLE_PATTERNS, but it describes how the PR was
    merged, not what it did.

    Babel squash-merges, so every PR arrives with its real title; NodeNorm and NameRes need not, and
    folding a substantive service change into the collapsed "routine" section is how it gets skipped
    during triage. Unknown is a reason to look, not to fold away -- but a bot's merge commit is
    still a bot's.
    """
    merge_subject = "Merge pull request #472 from TranslatorSRI/remove-redundant-ensembl-id-code"
    assert _pr(merge_subject, title_known=False).is_routine is False
    assert _pr(merge_subject, title_known=True).is_routine is True
    assert _pr(merge_subject, author="dependabot[bot]", title_known=False).is_routine is True


@pytest.mark.unit
def test_a_repository_with_no_baseline_version_gets_a_visible_todo():
    """A repository whose PR range can't be derived must say so, not vanish from the checklist.

    The manifest entries from 2025mar31 back predate the practice of recording NodeNorm/NameRes
    versions, so drafting against one of those leaves a repository with no baseline. An omitted
    section reads as "nothing changed there", which is exactly what this checklist exists to stop.
    """
    reason = "`releases/releases.yaml` records no baseline NameRes version for the previous release."
    lines = drn.pull_request_sections({"Babel": [_pr("Something real")], "NameRes": reason})
    text = "\n".join(lines)

    assert "### NameRes (range unknown)" in text
    assert f"_TODO: {reason}_" in text
    assert "### Babel (1 PRs)" in text


@pytest.mark.unit
def test_version_bumps_flag_a_key_the_baseline_config_lacks():
    """A version key added since the baseline (mesh_year, #1144) must not silently drop out of the note."""
    bullets = drn.version_bump_bullets(
        {"umls_version": "2025AB", "rxnorm_version": "01062025"},
        {"umls_version": "2026AA", "rxnorm_version": "01062025", "mesh_year": "2026"},
        "babel-1.18",
    )
    assert bullets[0] == "- Updated UMLS from 2025AB to 2026AA."
    assert len(bullets) == 2
    assert "MeSH 2026" in bullets[1] and "`babel-1.18` has no `mesh_year`" in bullets[1]


@pytest.mark.unit
def test_pull_request_markdown_is_an_unchecked_checklist_item():
    line = _pr("Add MP, kept disjoint from HP", number=886).as_markdown()
    assert line == (
        "- [ ] [Babel #886](https://github.com/NCATSTranslator/Babel/pull/886) — Add MP, kept disjoint from HP (@gaurav)"
    )


@pytest.mark.unit
def test_summary_of_changes_formats_every_kind_of_row(tmp_path):
    """A growing row, a shrinking row, an unchanged row and a from-zero row."""
    tables = tmp_path / "reports" / "tables"
    tables.mkdir(parents=True)
    (tables / "prefix_comparison_overall.csv").write_text(
        "Metric,Previous,Current,Absolute change,Percent change\n"
        "All CURIEs,688983999,605864191,-83119808,-12.1%\n"
        "All cliques (approx),490293340,388490111,-101803229,-20.8%\n"
        "AnatomicalEntity CURIEs,249584,252287,2703,+1.1%\n"
        "CellLine CURIEs,38810,38810,0,+0.0%\n"
        "Food CURIEs,0,932,932,NEW\n"
    )

    table = drn.summary_of_changes(tmp_path, "2025sep1", "2026jul22")
    rows = [[cell.strip() for cell in line.strip("|").split("|")] for line in table.splitlines()]

    assert rows[0] == ["Filename", "2025sep1", "2026jul22", "Diff", "% Diff"]
    # "All CURIEs"/"All cliques" are renamed to the labels the published notes have always used.
    assert rows[2] == ["Count of CURIEs in all files", "688,983,999", "605,864,191", "\\-83,119,808", "\\-12.1%"]
    # The source metric is `All cliques (approx)` -- a HyperLogLog estimate, ~2% out. 2026jul22's
    # table says 388,490,111 where the exact `clique_count` in its summary JSON is 398,671,998, so
    # the label has to keep saying so.
    assert rows[3][0] == "Count of cliques in all files (approx)"
    assert rows[4] == ["AnatomicalEntity", "249,584", "252,287", "+2,703", "+1.1%"]
    # An unchanged row is a bare "0", not "+0".
    assert rows[5] == ["CellLine", "38,810", "38,810", "0", "+0.0%"]
    # A compendium that did not exist before reads as Infinity%, matching 2025mar31.md's CellLine row.
    assert rows[6] == ["Food", "0", "932", "+932", "Infinity%"]


@pytest.mark.unit
def test_summary_of_changes_labels_the_baseline_the_build_actually_used(tmp_path, capsys):
    """The baseline column must name the release the *numbers* came from.

    The caller passes the manifest's nearest earlier deployed release, but the CSV was produced
    against whatever `previous_release` was pinned in config.yaml at build time. Those agree today
    and nothing enforces it: a build pinned to 2025mar31 would publish 2025mar31's numbers under a
    2025sep1 heading, with no way to tell from the note.
    """
    tables = tmp_path / "reports" / "tables"
    tables.mkdir(parents=True)
    (tables / "prefix_comparison_overall.csv").write_text(
        "Metric,Previous,Current,Absolute change,Percent change\nAll CURIEs,100,200,100,+100.0%\n"
    )
    (tables / "prefix_comparison.md").write_text(
        "# Prefix comparison: 2026jul22 vs 2025mar31\n\n"
        "Compared release `2026jul22` against baseline `2025mar31` "
        "(`releases/2025mar31/reports/duckdb/prefix_report.json`).\n"
    )

    table = drn.summary_of_changes(tmp_path, "2025sep1", "2026jul22")

    assert [cell.strip() for cell in table.splitlines()[0].strip("|").split("|")][:3] == [
        "Filename",
        "2025mar31",
        "2026jul22",
    ]
    assert "comparison is against 2025mar31, not the manifest's previous release 2025sep1" in capsys.readouterr().err


@pytest.mark.unit
def test_the_committed_archive_works_as_a_build_dir():
    """`releases/<build>/` mirrors the build directory, so every build-dir reader must accept it.

    This is the whole reason the archive is not flattened by kind: a note stays re-draftable from the
    repository alone, years after the build directory is gone. A layout change that breaks it would
    otherwise only show up the next time someone needed an old note.
    """
    archive = get_repo_root() / "releases" / "2026jul22"

    assert drn.comparison_baseline(archive) == "2025sep1"
    assert drn.notable_changes(archive)
    table = drn.summary_of_changes(archive, "2025sep1", "2026jul22")
    assert "Count of CURIEs in all files" in table
    assert drn.unexplained_movers(archive)


@pytest.mark.unit
def test_a_note_drafts_from_the_archive_with_no_build_dir(monkeypatch, capsys):
    """Omitting `--build-dir` falls back to `releases/<build>/`, so an old note needs only the repo.

    The four build-dir readers degrade to TODOs when they get nothing, so a broken fallback does not
    fail -- it quietly produces a note with an empty count table, which is the failure mode the whole
    archive exists to prevent. Assert the real numbers arrive.
    """
    monkeypatch.setattr(drn, "fetch_pull_requests", lambda *_args: [])
    entry = {"id": "2026jul22", "title": "Babel 2026jul22", "build": "2026jul22"}
    previous = {"id": "2025sep1", "title": "Babel 2025sep1", "build": "2025sep1"}
    monkeypatch.setattr(drn, "find_release", lambda *_args: (entry, previous))

    assert drn.main(["2026jul22", "--offline"]) == 0
    captured = capsys.readouterr()

    assert "Reading build reports from the archived release" in captured.err
    assert "Count of cliques in all files (approx)" in captured.out
    assert "605,864,191" in captured.out  # the real CURIE total, not a TODO
    assert "no `prefix_comparison_overall.csv` found" not in captured.out


@pytest.mark.unit
def test_summary_of_changes_is_none_without_a_report(tmp_path):
    assert drn.summary_of_changes(tmp_path, "2025sep1", "2026jul22") is None


@pytest.mark.unit
def test_summary_of_changes_renders_a_header_only_report(tmp_path):
    """A comparison that ran and produced no rows should still render a table, not crash the draft.

    The column-width calculation reduces over the data rows, so an empty set used to raise
    `TypeError` from `max()` and take the whole note with it.
    """
    tables = tmp_path / "reports" / "tables"
    tables.mkdir(parents=True)
    (tables / "prefix_comparison_overall.csv").write_text("Metric,Previous,Current,Absolute change,Percent change\n")

    table = drn.summary_of_changes(tmp_path, "2025sep1", "2026jul22")

    lines = table.splitlines()
    assert len(lines) == 2  # header and separator, no data rows
    assert [cell.strip() for cell in lines[0].strip("|").split("|")] == [
        "Filename",
        "2025sep1",
        "2026jul22",
        "Diff",
        "% Diff",
    ]


@pytest.mark.unit
def test_baseline_skips_releases_that_were_never_deployed():
    """v1.11 sits between 2025sep1 and 2025mar31 but never shipped, so it cannot be a baseline."""
    manifest = drn.load_manifest(get_repo_root() / "releases" / "releases.yaml")
    entry, previous = drn.find_release(manifest, "2025sep1")
    assert entry["id"] == "2025sep1"
    assert previous["id"] == "2025mar31"


@pytest.mark.unit
def test_ranges_use_the_last_service_version_deployed_against_each_build():
    """NameRes v1.5.2 shipped after v1.6.2, so the baseline is what was deployed, not what sorts highest."""
    manifest = drn.load_manifest(get_repo_root() / "releases" / "releases.yaml")
    entry, previous = drn.find_release(manifest, "2026jul22")
    assert drn._range_for("Babel", entry, previous) == ("2025sep1", "2026jul22")
    assert drn._range_for("NodeNorm", entry, previous) == ("v2.4.1", "v2.5.1")
    assert drn._range_for("NameRes", entry, previous) == ("v1.5.2", "v1.7.0")


@pytest.mark.unit
def test_every_release_note_is_listed_in_the_manifest():
    """Drift guard: a new note in releases/ must be added to releases.yaml or the tooling won't see it."""
    releases_dir = get_repo_root() / "releases"
    # A note is `releases/<build>/README.md`, keyed by the directory it sits in. The one exception is
    # v1.11, which has no build of its own and is filed under 2025sep1 as `v1.11.md`, so a note that
    # is not a README is keyed by its own stem instead. Top-level `*.md` is deliberately out of
    # scope: README.md and ARTIFACTS.md are not notes, and the transitional redirect stubs left
    # behind by https://github.com/NCATSTranslator/Babel/issues/1021 are not either.
    on_disk = {p.parent.name if p.stem == "README" else p.stem for p in releases_dir.glob("*/*.md")}
    # Keyed by build, because that is what the directory is named; entries with no build (v1.11) are
    # keyed by id, which is the name their note file carries.
    in_manifest = {entry.get("build") or entry["id"] for entry in drn.load_manifest(releases_dir / "releases.yaml")}
    assert on_disk == in_manifest


@pytest.mark.unit
@pytest.mark.parametrize(
    "message,expected",
    [
        # Squash merges, the normal case across all three repos.
        ("Add MP, kept disjoint from HP (#886)", (886, "Add MP, kept disjoint from HP")),
        # A title that itself cites an issue number must not be mistaken for the PR number.
        ("Type DrugBank foods as biolink:Food (#828) (#918)", (918, "Type DrugBank foods as biolink:Food (#828)")),
        # Older merge commits name the PR but carry no title.
        ("Merge pull request #472 from TranslatorSRI/remove-redundant-ensembl-id-code", (472, None)),
        # Direct commits with no PR reference are skipped, not guessed at.
        ("Fixed a typo", None),
        ("Refs #123 but not a squash merge", None),
    ],
)
def test_commit_message_pr_extraction(message, expected):
    squash = drn._SQUASH_RE.match(message)
    merge = drn._MERGE_RE.match(message)
    if expected is None:
        assert squash is None and merge is None
        return
    number, title = expected
    if title is None:
        assert squash is None
        assert int(merge.group("number")) == number
    else:
        assert int(squash.group("number")) == number
        assert squash.group("title") == title


@pytest.mark.unit
def test_provenance_block_records_the_deployed_service_versions():
    """The whole point of releases.yaml: a note must say which NodeNorm/NameRes shipped with it."""
    manifest = drn.load_manifest(get_repo_root() / "releases" / "releases.yaml")
    entry, previous = drn.find_release(manifest, "2026jul22")
    block = "\n".join(drn.provenance_block(entry, previous))

    assert "https://stars.renci.org/var/babel_outputs/2026jul22/" in block
    assert "branch `babel-1.18.1`" in block
    assert "Biolink Model v4.4.3" in block
    # Both NodeNorm versions deployed against this build are listed, not just the latest.
    assert "NodeNorm: [v2.5.0]" in block and "[v2.5.1]" in block
    assert "NameRes: [v1.7.0]" in block
    # The note is written to `releases/2026jul22/README.md`, so the archive is a sibling of the note
    # and the previous release is one directory over.
    assert "Previous release: [Babel 2025sep1](../2025sep1/README.md)" in block
    # 2026jul22 is a real tag, so the tag link is emitted.
    assert "[tagged 2026jul22](https://github.com/NCATSTranslator/Babel/releases/tag/2026jul22)" in block
    assert "[Summary tables](./reports/tables/)" in block
    assert "[Prefix report](./reports/duckdb/prefix_report.json)" in block


@pytest.mark.unit
def test_a_sibling_note_is_linked_by_build_not_release_id():
    """Release directories are named by build, and for the older releases that is not the id.

    `TranslatorFuguJuly2024`'s note and archive both live under `2024jul13`, so a link written from
    the release id points at a path that never existed -- which is what the archive links used to
    do, uncaught because the four notes carrying them are historical. The archive is now a sibling
    of the note and needs no name at all; the cross-note link is where the distinction still bites.
    """
    manifest = drn.load_manifest(get_repo_root() / "releases" / "releases.yaml")
    entry, previous = drn.find_release(manifest, "TranslatorGuppyAugust2024")
    block = "\n".join(drn.provenance_block(entry, previous, get_repo_root()))

    assert 'Previous release: [Translator "Fugu" July 2024](../2024jul13/README.md)' in block
    assert "TranslatorFuguJuly2024" not in block
    # And the files both links point at are really there.
    assert (get_repo_root() / "releases" / "2024jul13" / "README.md").exists()
    assert (get_repo_root() / "releases" / "2024aug18" / "reports" / "content" / "compendia_report.json").exists()


@pytest.mark.unit
def test_provenance_does_not_link_a_build_that_was_never_tagged():
    """`build` names the output directory, which is usually -- not always -- also a git tag.

    `releases.yaml` carries three builds that were never tagged in this repo (2024aug18, 2024mar24,
    2023nov5), and rendering them as `[tagged X](.../releases/tag/X)` publishes a 404 as the note's
    provenance.
    """
    manifest = drn.load_manifest(get_repo_root() / "releases" / "releases.yaml")
    entry, previous = drn.find_release(manifest, "TranslatorGuppyAugust2024")
    block = "\n".join(drn.provenance_block(entry, previous, get_repo_root()))

    assert "releases/tag/2024aug18" not in block
    assert "untagged in this repository as of drafting (`2024aug18`)" in block
    # The build's own output directory is still linked -- that URL is real.
    assert "https://stars.renci.org/var/babel_outputs/2024aug18/" in block


@pytest.mark.unit
def test_provenance_block_hedges_an_approximate_babel_version():
    """The 2024 notes say "approx"; a manifest entry marked approx must not imply precision."""
    manifest = drn.load_manifest(get_repo_root() / "releases" / "releases.yaml")
    entry, _ = drn.find_release(manifest, "TranslatorGuppyAugust2024")
    assert "approx [Babel v1.8.0]" in "\n".join(drn.provenance_block(entry, None))


# STATUS ENDPOINTS

# Trimmed from the real responses, so the shapes are the ones actually served:
#   https://nodenormalization-exp.apps.renci.org/status
#   https://name-resolution-sri.renci.org/status
NODENORM_STATUS = {
    "status": "running",
    "version": "2.5.1",
    "babel_version": "2026jul22",
    "databases": {
        "eq_id_to_id_db": {"dbname": "id-id", "count": 605837726, "used_memory_rss_human": "53.78G"},
        "chemical_drug_db": {"dbname": "chemical-drug-db", "count": 104863, "used_memory_rss_human": "210.54M"},
        "id_to_eqids_db": {"dbname": "id-eq-id", "count": 398664426, "used_memory_rss_human": "94.61G"},
    },
}
# NameRes v1.5.2 (Dev at the time of writing) nests the Solr counters under "solr"...
NAMERES_STATUS = {
    "status": "ok",
    "babel_version": "2025sep1",
    "nameres_version": "v1.5.2",
    "solr": {"numDocs": 425583002, "maxDoc": 425586610, "size": "142.17 GB"},
}
# ...while v1.7.0 (Exp, serving 2026jul22) promotes them to the top level. Trimmed verbatim from
# https://name-resolution-exp.apps.renci.org/status -- both shapes are live simultaneously, and
# reading only the nested one yields a blank Solr row rather than an error.
NAMERES_STATUS_V17 = {
    "status": "ok",
    "message": "Reporting results from primary core.",
    "babel_version": "2026jul22",
    "nameres_version": "v1.7.0",
    "numDocs": 331513708,
    "maxDoc": 331516999,
    "deletedDocs": 3291,
    "size": "109.75 GB",
}


@pytest.mark.unit
def test_summary_table_uses_the_published_row_order_not_the_json_order():
    """NodeNorm returns its databases in whatever order it likes; the note's order is fixed."""
    rows = [line for line in drn.summary_table(NODENORM_STATUS, NAMERES_STATUS) if line.startswith("| ")]
    ids = [row.split("|")[2].strip() for row in rows[1:]]
    assert ids == ["eq_id_to_id_db", "id_to_eqids_db", "chemical_drug_db", "name_lookup"]
    assert "605,837,726" in rows[1] and "53.78G" in rows[1]
    # The Solr row comes from NameRes, not NodeNorm.
    assert "425,583,002" in rows[-1] and "142.17 GB" in rows[-1]


@pytest.mark.unit
def test_summary_table_reads_solr_counters_from_either_nameres_shape():
    """v1.5.2 nests them under `solr`, v1.7.0 puts them at the top level; both must populate the row.

    Reading only one shape leaves the Solr row *blank* rather than erroring, so the note would ship
    with a missing number that nothing flags.
    """
    for status, docs, size in (
        (NAMERES_STATUS, "425,583,002", "142.17 GB"),
        (NAMERES_STATUS_V17, "331,513,708", "109.75 GB"),
    ):
        row = [line for line in drn.summary_table(NODENORM_STATUS, status) if line.startswith("| ")][-1]
        assert row.split("|")[2].strip() == "name_lookup"
        assert docs in row and size in row


@pytest.mark.unit
def test_summary_table_keeps_a_database_it_has_never_seen():
    """A new Redis database must be appended, not silently dropped for being unrecognised."""
    status = {"databases": dict(NODENORM_STATUS["databases"])}
    status["databases"]["brand_new_db"] = {"dbname": "new-thing", "count": 7, "used_memory_rss_human": "1M"}
    rows = [line for line in drn.summary_table(status, None) if line.startswith("| ")]
    assert rows[-2].split("|")[2].strip() == "brand_new_db"


@pytest.mark.unit
def test_summary_table_falls_back_to_a_blank_skeleton_when_nodenorm_is_unreachable():
    rows = [line for line in drn.summary_table(None, None) if line.startswith("| ")]
    ids = [row.split("|")[2].strip() for row in rows[1:]]
    assert ids == drn.SUMMARY_DB_ORDER + ["name_lookup"]
    assert all(row.split("|")[3].strip() == "" for row in rows[1:])


@pytest.mark.unit
def test_deployed_version_check_warns_only_on_a_mismatch():
    """NameRes was still serving 2025sep1 when 2026jul22's note was written; copying its Solr
    numbers in unremarked would have described the previous release."""
    assert drn.check_deployed_version(NODENORM_STATUS, "NodeNorm", "2026jul22") == []
    warning = drn.check_deployed_version(NAMERES_STATUS, "NameRes", "2026jul22")
    assert len(warning) == 1
    assert "serving Babel 2025sep1, not 2026jul22" in warning[0]
    # An unreachable service is not a mismatch; the blank table already says the numbers are missing.
    assert drn.check_deployed_version(None, "NameRes", "2026jul22") == []


@pytest.mark.unit
def test_fetch_status_returns_none_instead_of_raising(capsys):
    """A note is worth drafting when the services are down or not yet deployed."""
    assert drn.fetch_status("http://127.0.0.1:1/status") is None
    assert "warning: could not read" in capsys.readouterr().err


@pytest.mark.unit
@pytest.mark.parametrize(
    "error",
    [
        # urllib wraps what happens while *opening* the connection, but these are raised out of
        # `response.read()` and reach the caller unwrapped: a RemoteDisconnected is an OSError and an
        # IncompleteRead is an HTTPException, so neither is caught by `except urllib.error.URLError`.
        http.client.RemoteDisconnected("Remote end closed connection without response"),
        http.client.IncompleteRead(b'{"databases"'),
        ValueError("Expecting value: line 1 column 1 (char 0)"),
    ],
)
def test_fetch_status_survives_a_connection_dropped_mid_read(monkeypatch, capsys, error):
    """A service restarted mid-draft must degrade to the blank table, not abort the whole note."""

    class _Dying:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self):
            raise error

    monkeypatch.setattr(drn.urllib.request, "urlopen", lambda *_a, **_kw: _Dying())

    assert drn.fetch_status("https://nodenormalization-exp.apps.renci.org/status") is None
    assert "warning: could not read" in capsys.readouterr().err


@pytest.mark.unit
def test_fetch_pull_requests_surfaces_ghs_own_error(monkeypatch):
    """A failing `gh api` must report *why*, not just that it exited non-zero.

    `capture_output=True` hides gh's stderr, and that message -- an unknown tag, an expired token, a
    rate limit -- is the entire diagnosis. Unlike a missing tag, none of these is guessable from the
    exit code.
    """
    import subprocess

    def fake_run(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, "gh", stderr="gh: Not Found (HTTP 404)\n")

    monkeypatch.setattr(drn.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="Not Found"):
        drn.fetch_pull_requests("Babel", "NCATSTranslator/Babel", "2025sep1", "2026jul22")


@pytest.mark.unit
def test_a_failed_compare_degrades_like_a_missing_baseline(monkeypatch, capsys):
    """A build name GitHub can't resolve must cost one section, not the whole note.

    `releases.yaml` records three build names that were never tagged in this repo (2024aug18,
    2024mar24, 2023nov5), so drafting against one of them raises out of `fetch_pull_requests()` --
    where the sibling case, a repository with no recorded baseline version, already degrades to a
    visible TODO.
    """

    def fake_fetch(repo_key, *_args):
        if repo_key == "Babel":
            raise RuntimeError("`gh api` failed comparing 2024aug18...2026jul22: gh: Not Found (HTTP 404)")
        return []

    monkeypatch.setattr(drn, "fetch_pull_requests", fake_fetch)
    entry = {"id": "2026jul22", "title": "Babel 2026jul22", "build": "2026jul22", "nodenorm": ["v1.1.0"]}
    previous = {"id": "2024aug18", "title": "Old", "build": "2024aug18", "nodenorm": ["v1.0.0"]}

    text = drn.draft(entry, previous, None, drn.REPO_ROOT)

    assert "### Babel (range unknown)" in text
    assert "Could not list Babel pull requests: `gh api` failed" in text
    assert "Not Found (HTTP 404)" in capsys.readouterr().err
    # The repositories that did resolve are still listed.
    assert "### NodeNorm (0 PRs)" in text


# SECTION STRUCTURE


@pytest.mark.unit
def test_draft_emits_the_house_section_order():
    """The skeleton must match the structure `releases/2026jul22.md` converged on.

    That note was hand-reorganized twice (c22e83b4, 5ac3dff0) away from the Bugfixes/Updates/New
    features skeleton the script used to emit, and both passes moved the same direction. Pinning the
    order here is what stops the next release being hand-rebuilt a third time.

    Two orderings are load-bearing rather than cosmetic:

    - `## Bugfixes` leads, so the section a consumer must act on is the first thing they see.
    - `## Areas that changed substantially` follows it, above the per-repository notes: after "what
      must I act on", "what moved" is what a consumer reads next. It is deliberately far from the
      two tables it explains, which a reader who wants the numbers can scroll to.

    The section is only emitted when there is a build directory to draw movements from, so the
    second case below passes the committed 2026jul22 archive.
    """
    entry = {"id": "2026jul22", "title": "Babel 2026jul22", "build": "2026jul22"}
    text = drn.draft(entry, None, None, drn.REPO_ROOT)
    headings = [line for line in text.splitlines() if line.startswith("#")]

    assert headings == [
        "# Babel 2026jul22",
        "## Bugfixes",
        "## Babel changes",
        "### Updates",
        "### New features and identifier/mapping additions",
        "### Improvements to Babel's output",
        "### Development and infrastructure",
        "### Minor changes and fixes",
        "### Known issues and caveats",
        "## NodeNorm Redis",
        "## NameRes Solr",
        "## All changes in this release",
        "## Deployed database sizes",
        "## Compendium size comparison",
    ]

    with_build = drn.draft(entry, None, drn.REPO_ROOT / "releases" / "2026jul22", drn.REPO_ROOT)
    with_headings = [line for line in with_build.splitlines() if line.startswith("## ")]

    assert with_headings[:3] == [
        "## Bugfixes",
        "## Areas that changed substantially",
        "## Babel changes",
    ]


@pytest.mark.unit
def test_unexplained_movers_flags_every_large_compendium_move(tmp_path):
    """A compendium that moved a lot but is explained nowhere must surface as an unchecked item.

    Nothing flagged these before: the build's own comparison warns per *prefix*, while the note is
    written per *compendium*. Polypeptide fell 97% in 2026jul22 and reached review unmentioned.
    """
    tables = tmp_path / "reports" / "tables"
    tables.mkdir(parents=True)
    (tables / "prefix_comparison_overall.csv").write_text(
        "Metric,Previous,Current,Absolute change,Percent change\n"
        "All CURIEs,688983999,605864191,-83119808,-12.1%\n"  # aggregate: never a mover
        "Polypeptide CURIEs,166,5,-161,-97.0%\n"
        "Food CURIEs,0,932,932,NEW\n"
        "Disease CURIEs,632330,639398,7068,1.1%\n"  # below threshold
    )

    movers = drn.unexplained_movers(tmp_path)

    assert "- [ ] **Polypeptide** (-97.0%, \\-161 CURIEs)" in movers
    assert "- [ ] **Food** (new this release)" in movers
    assert not [line for line in movers if "Disease" in line]
    assert not [line for line in movers if "All CURIEs" in line]


@pytest.mark.unit
def test_unexplained_movers_is_silent_without_a_build(tmp_path):
    """No CSV means no claim either way, rather than an empty checklist that reads as 'all clear'."""
    assert drn.unexplained_movers(tmp_path) == []


@pytest.mark.unit
def test_umls_mesh_skew_bullet_reports_the_build_count(tmp_path):
    """The build's UMLS/MeSH skew count should be drafted as a bullet naming both versions."""
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "umls_mesh_skew.json").write_text(
        json.dumps(
            {
                "umls_version": "2026AA",
                "mesh_year": "2026",
                "umls_msh_codes_missing_from_mesh_excluding_categories": 1234,
            }
        )
    )
    bullet = drn.umls_mesh_skew_bullet(tmp_path)
    assert bullet.startswith("- 1,234 MeSH codes in UMLS 2026AA have no record in MeSH 2026")


@pytest.mark.unit
def test_umls_mesh_skew_bullet_is_silent_for_an_older_build(tmp_path):
    """A build from before the report existed should get no bullet rather than a broken one."""
    assert drn.umls_mesh_skew_bullet(tmp_path) is None

"""Unit tests for src/model/source.py.

A Babel source can vary along three axes simultaneously (see src/model/source.py):

- **babel_pipeline** — which intermediate directory the source contributes to
  (e.g. ``anatomy``, ``chemical``). MESH spans several; most sources span one.
- **biolink_type** — the Biolink class URI written in the ids file's second column.
  Multiple types may appear in one ids file (e.g. UBERON mixes ``AnatomicalEntity``
  and ``GrossAnatomicalStructure``).
- **prefix** — the CURIE prefix of the identifiers. Rare but supported: a single
  source file can emit ``PREFIXA:…`` and ``PREFIXB:…`` rows.

Test groups
-----------
scan_concords_for_curies:
    Row-matching and asserter-recording logic. Key behaviours: matches either endpoint
    of a concord triple; records the concord file path relative to ``concords_dir`` as
    the asserter; skips ``metadata-*`` sidecars; recurses into subdirectories
    (e.g. ``UNICHEM/UNICHEM_*``).

summarize_xref_groups:
    Grouping concord rows into join pathways: prefix-pair canonicalisation, keeping the two
    assertion directions apart, predicate separation, example sampling, and no dedupe.

discover_source — structure:
    Baseline (single everything) plus one test per axis, confirming that
    ``SourceContribution`` correctly aggregates across each dimension.

discover_source — edge cases:
    Missing source name, missing intermediate root, and metadata sidecar filtering.
"""

import pytest

from src.model.source import discover_source, scan_concords_for_curies, summarize_xref_groups

# ---------------------------------------------------------------------------
# scan_concords_for_curies
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scan_concords_for_curies_matches_either_endpoint_and_records_asserter(tmp_path):
    """Matches rows where the source CURIE is subject or object; skips rows and files with no match.

    Also verifies that ``asserted_by`` is the concord file path relative to ``concords_dir``
    (``"UBERON"``, not the full path), and that metadata sidecars are never scanned even if
    they contain matching content.
    """
    concords = tmp_path / "anatomy" / "concords"
    concords.mkdir(parents=True)
    # EMAPA's own concord is empty; its xrefs live in UBERON's concord.
    (concords / "EMAPA").write_text("")
    (concords / "UBERON").write_text(
        "UBERON:1\txref\tEMAPA:10\n"  # source CURIE on the object side
        "EMAPA:20\tskos:exactMatch\tCL:2\n"  # source CURIE on the subject side
        "UBERON:3\txref\tCL:4\n"  # no source CURIE — skipped
    )
    # Metadata sidecars must be ignored.
    (concords / "metadata-UBERON.yaml").write_text("UBERON:1\txref\tEMAPA:10\n")

    rows = scan_concords_for_curies(concords, {"EMAPA:10", "EMAPA:20"})

    assert ("UBERON:1", "xref", "EMAPA:10", "UBERON") in rows
    assert ("EMAPA:20", "skos:exactMatch", "CL:2", "UBERON") in rows
    assert all(r[3] == "UBERON" for r in rows), "asserted_by is the concord file path relative to concords_dir"
    assert not any("UBERON:3" in r for r in rows), "rows without a source CURIE are dropped"
    assert len(rows) == 2


@pytest.mark.unit
def test_scan_concords_for_curies_missing_dir_returns_empty(tmp_path):
    """Returns an empty list rather than raising when the concords directory does not exist."""
    assert scan_concords_for_curies(tmp_path / "nope", {"EMAPA:1"}) == []


# ---------------------------------------------------------------------------
# summarize_xref_groups
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_summarize_xref_groups_canonicalizes_the_prefix_pair():
    """Rows written in opposite directions by the same file should collapse into one pathway.

    Whether a concord writes ``A -> B`` or ``B -> A`` is an artifact of how that source generates
    its file, so the pair is sorted and both orientations count towards the same group.
    """
    rows = [
        ("EMAPA:1", "xref", "UBERON:1", "UBERON"),
        ("UBERON:2", "xref", "EMAPA:2", "UBERON"),
    ]
    groups = summarize_xref_groups(rows, "anatomy", "EMAPA")
    assert len(groups) == 1
    assert (groups[0].prefix_1, groups[0].prefix_2) == ("EMAPA", "UBERON")
    assert groups[0].count == 2


@pytest.mark.unit
def test_summarize_xref_groups_keeps_the_two_assertion_directions_apart():
    """A pathway asserted by the new source must stay separate from the same pair asserted elsewhere.

    This is the regression that matters when canonicalising the prefix pair: MP asserting a mapping
    to HP is a bridge this addition introduces (``added``), while HP asserting one to MP may predate
    it entirely (``from_other_source``). They share a sorted prefix pair, so only ``asserted_by`` and
    ``status`` being part of the group key keeps them distinguishable — which is the whole question
    the impact report answers.
    """
    rows = [
        ("MP:1", "xref", "HP:1", "MP"),
        ("HP:2", "xref", "MP:2", "HP"),
    ]
    groups = summarize_xref_groups(rows, "disease", "MP")
    assert {(g.asserted_by, g.status, g.count) for g in groups} == {
        ("MP", "added", 1),
        ("HP", "from_other_source", 1),
    }
    assert all((g.prefix_1, g.prefix_2) == ("HP", "MP") for g in groups)


@pytest.mark.unit
def test_summarize_xref_groups_separates_predicates_and_orders_by_count():
    """Each predicate is its own pathway, and the biggest pathway is listed first."""
    rows = [("MP:1", "exactMatch", "HP:1", "MP")]
    rows += [(f"MP:{i}", "closeMatch", f"HP:{i}", "MP") for i in range(2, 6)]
    groups = summarize_xref_groups(rows, "disease", "MP")
    assert [(g.predicate, g.count) for g in groups] == [("closeMatch", 4), ("exactMatch", 1)]


@pytest.mark.unit
def test_summarize_xref_groups_examples_span_the_group_and_are_stable():
    """Examples are evenly spread across a large group, capped, and identical across calls.

    Even spacing rather than the first N, so the sample covers the identifier range instead of
    clustering on the lowest IDs. A group smaller than the budget keeps every row.
    """
    rows = [(f"UBERON:{i:04d}", "xref", f"EMAPA:{i:04d}", "UBERON") for i in range(100)]
    (group,) = summarize_xref_groups(rows, "anatomy", "EMAPA", examples_per_group=5)
    assert group.count == 100
    assert group.examples == (
        ("UBERON:0000", "EMAPA:0000"),
        ("UBERON:0020", "EMAPA:0020"),
        ("UBERON:0040", "EMAPA:0040"),
        ("UBERON:0060", "EMAPA:0060"),
        ("UBERON:0080", "EMAPA:0080"),
    )
    assert summarize_xref_groups(rows, "anatomy", "EMAPA", examples_per_group=5)[0].examples == group.examples

    (small,) = summarize_xref_groups(rows[:3], "anatomy", "EMAPA", examples_per_group=5)
    assert len(small.examples) == 3


@pytest.mark.unit
def test_summarize_xref_groups_does_not_dedupe_identical_triples():
    """The same triple asserted by two files is two assertions, so it lands in two groups."""
    rows = [
        ("UBERON:1", "xref", "EMAPA:1", "UBERON"),
        ("UBERON:1", "xref", "EMAPA:1", "MA"),
    ]
    groups = summarize_xref_groups(rows, "anatomy", "EMAPA")
    assert sorted(g.asserted_by for g in groups) == ["MA", "UBERON"]
    assert all(g.count == 1 for g in groups)


# ---------------------------------------------------------------------------
# discover_source — structure
# ---------------------------------------------------------------------------


def _make_source_tree(root, source_name, pipeline, ids_lines=None, concord_lines=None):
    """Write minimal ids/ and concords/ files under ``root/<pipeline>/`` for one source."""
    ids_dir = root / pipeline / "ids"
    concords_dir = root / pipeline / "concords"
    ids_dir.mkdir(parents=True, exist_ok=True)
    concords_dir.mkdir(parents=True, exist_ok=True)
    if ids_lines is not None:
        (ids_dir / source_name).write_text("\n".join(ids_lines) + "\n")
    if concord_lines is not None:
        (concords_dir / source_name).write_text("\n".join(concord_lines) + "\n")


@pytest.mark.unit
def test_discover_single_prefix_single_type_single_pipeline(tmp_path):
    """Baseline: one source, one babel_pipeline, one biolink_type, one prefix.

    Verifies the full shape of ``SourceContribution`` — pipelines, prefixes,
    declared_biolink_types, total counts, and the per-pipeline ``declared_type_counts``
    and ``concord_partner_prefix_counts`` breakdowns.
    """
    _make_source_tree(
        tmp_path,
        "EMAPA",
        "anatomy",
        ids_lines=["EMAPA:1\tbiolink:AnatomicalEntity", "EMAPA:2\tbiolink:AnatomicalEntity"],
        concord_lines=["EMAPA:1\txref\tUBERON:1"],
    )

    contrib = discover_source("EMAPA", tmp_path)

    assert contrib.pipelines == frozenset({"anatomy"})
    assert contrib.prefixes == frozenset({"EMAPA"})
    assert contrib.declared_biolink_types == frozenset({"biolink:AnatomicalEntity"})
    assert contrib.total_identifier_count == 2
    assert contrib.total_concord_row_count == 1

    pc = contrib.by_pipeline["anatomy"]
    assert pc.declared_type_counts == {"biolink:AnatomicalEntity": 2}
    assert pc.concord_partner_prefix_counts == {"UBERON": 1}


@pytest.mark.unit
def test_discover_multi_biolink_type_within_one_semantic_type(tmp_path):
    """An ids file may mix biolink types in its second column — UBERON does this with
    AnatomicalEntity and GrossAnatomicalStructure."""
    _make_source_tree(
        tmp_path,
        "UBERON",
        "anatomy",
        ids_lines=[
            "UBERON:1\tbiolink:AnatomicalEntity",
            "UBERON:2\tbiolink:GrossAnatomicalStructure",
            "UBERON:3\tbiolink:AnatomicalEntity",
        ],
    )

    contrib = discover_source("UBERON", tmp_path)

    assert contrib.declared_biolink_types == frozenset({"biolink:AnatomicalEntity", "biolink:GrossAnatomicalStructure"})
    pc = contrib.by_pipeline["anatomy"]
    assert pc.declared_type_counts == {
        "biolink:AnatomicalEntity": 2,
        "biolink:GrossAnatomicalStructure": 1,
    }


@pytest.mark.unit
def test_discover_multi_pipeline(tmp_path):
    """MESH-style source present in two babel_pipeline directories (anatomy and chemical)."""
    _make_source_tree(
        tmp_path,
        "MESH",
        "anatomy",
        ids_lines=["MESH:A1\tbiolink:AnatomicalEntity"],
    )
    _make_source_tree(
        tmp_path,
        "MESH",
        "chemical",
        ids_lines=["MESH:C1\tbiolink:ChemicalEntity", "MESH:C2\tbiolink:ChemicalEntity"],
    )

    contrib = discover_source("MESH", tmp_path)

    assert contrib.pipelines == frozenset({"anatomy", "chemical"})
    assert contrib.total_identifier_count == 3
    assert contrib.declared_biolink_types == frozenset({"biolink:AnatomicalEntity", "biolink:ChemicalEntity"})
    assert len(contrib.by_pipeline["anatomy"].all_curies) == 1
    assert len(contrib.by_pipeline["chemical"].all_curies) == 2


@pytest.mark.unit
def test_discover_multi_prefix(tmp_path):
    """A source may write rows under more than one prefix (rare but supported)."""
    _make_source_tree(
        tmp_path,
        "WEIRD",
        "anatomy",
        ids_lines=[
            "PREFIXA:1\tbiolink:AnatomicalEntity",
            "PREFIXB:1\tbiolink:AnatomicalEntity",
        ],
    )

    contrib = discover_source("WEIRD", tmp_path)

    assert contrib.prefixes == frozenset({"PREFIXA", "PREFIXB"})
    pc = contrib.by_pipeline["anatomy"]
    assert {p: len(c) for p, c in pc.curies_by_prefix.items()} == {
        "PREFIXA": 1,
        "PREFIXB": 1,
    }


# ---------------------------------------------------------------------------
# discover_source — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_discover_missing_source_returns_empty_contribution(tmp_path):
    """A source name with no ids or concords files anywhere returns an empty contribution."""
    (tmp_path / "anatomy" / "ids").mkdir(parents=True)
    (tmp_path / "anatomy" / "concords").mkdir(parents=True)

    contrib = discover_source("NONEXISTENT", tmp_path)
    assert contrib.by_pipeline == {}
    assert contrib.pipelines == frozenset()
    assert contrib.total_identifier_count == 0


@pytest.mark.unit
def test_discover_raises_when_intermediate_root_missing(tmp_path):
    """Passing a non-existent intermediate root raises FileNotFoundError rather than silently returning empty."""
    with pytest.raises(FileNotFoundError):
        discover_source("EMAPA", tmp_path / "missing")


@pytest.mark.unit
def test_scan_concords_for_curies_recurses_into_subdirectories(tmp_path):
    """Concord files in subdirectories (e.g. chemicals/concords/UNICHEM/UNICHEM_*) must be
    scanned; asserted_by should be the relative path from concords_dir."""
    concords = tmp_path / "chemicals" / "concords"
    unichem_dir = concords / "UNICHEM"
    unichem_dir.mkdir(parents=True)
    (unichem_dir / "UNICHEM_7").write_text("PUBCHEM.COMPOUND:1\txref\tCHEMBL.COMPOUND:2\n")
    (unichem_dir / "UNICHEM_22").write_text("PUBCHEM.COMPOUND:1\txref\tCHEBI:999\n")
    # Metadata sidecars in subdirectories are also skipped.
    (unichem_dir / "metadata-UNICHEM_7.yaml").write_text("name: unichem\n")

    rows = scan_concords_for_curies(concords, {"PUBCHEM.COMPOUND:1"})

    assert len(rows) == 2
    asserters = {r[3] for r in rows}
    assert asserters == {"UNICHEM/UNICHEM_7", "UNICHEM/UNICHEM_22"}


@pytest.mark.unit
def test_discover_skips_metadata_yaml_files(tmp_path):
    """discover_source should only treat files literally named <SOURCE>, not metadata-* siblings."""
    concord_dir = tmp_path / "anatomy" / "concords"
    concord_dir.mkdir(parents=True)
    (concord_dir / "EMAPA").write_text("EMAPA:1\txref\tUBERON:1\n")
    (concord_dir / "metadata-EMAPA.yaml").write_text("name: build_anatomy_obo_relationships()\n")
    (tmp_path / "anatomy" / "ids").mkdir(parents=True)

    contrib = discover_source("EMAPA", tmp_path)
    assert contrib.pipelines == frozenset({"anatomy"})
    assert contrib.total_concord_row_count == 1


@pytest.mark.unit
def test_discover_source_counts_only_the_named_concords(tmp_path):
    """``concord_names`` defaults to the source name, and a differently-named concord is invisible
    until the caller names it.

    That default is why `source-impact-report --source GARD` alone reported zero cross-references
    once GARD grew a `GARD_label` concord. A naming rule cannot replace the flag: `GARD_label` is
    GARD's own data while `MONDO_GARD` is MONDO's data *about* GARD, and both contain "GARD".
    """
    _make_source_tree(
        tmp_path,
        "GARD",
        "disease",
        ids_lines=["GARD:1\tbiolink:Disease"],
        concord_lines=None,
    )
    concords_dir = tmp_path / "disease" / "concords"
    (concords_dir / "GARD_label").write_text("GARD:1\txref\tNCIT:C1\n")
    (concords_dir / "MONDO_GARD").write_text("MONDO:1\txref\tGARD:2\n")

    default = discover_source("GARD", tmp_path)
    assert default.concord_names == frozenset()
    assert default.total_concord_row_count == 0

    named = discover_source("GARD", tmp_path, concord_names=["GARD", "GARD_label"])
    assert named.concord_names == frozenset({"GARD_label"}), "a named concord that does not exist is skipped"
    assert named.total_concord_row_count == 1
    assert named.by_pipeline["disease"].concord_partner_prefix_counts == {"NCIT": 1}
    assert "MONDO_GARD" not in named.concord_names, "another source's concord about GARD is not GARD's"


@pytest.mark.unit
def test_discover_source_concatenates_several_concords(tmp_path):
    """A source may write more than one concord; rows and partner counts come from all of them."""
    _make_source_tree(tmp_path, "GARD", "disease", ids_lines=["GARD:1\tbiolink:Disease"], concord_lines=None)
    concords_dir = tmp_path / "disease" / "concords"
    (concords_dir / "GARD").write_text("GARD:1\txref\tNCIT:C1\n")
    (concords_dir / "GARD_label").write_text("GARD:2\txref\tMESH:D1\n")

    contrib = discover_source("GARD", tmp_path, concord_names=["GARD", "GARD_label"])

    assert contrib.total_concord_row_count == 2
    assert contrib.by_pipeline["disease"].concord_partner_prefix_counts == {"NCIT": 1, "MESH": 1}

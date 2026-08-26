"""
Unit tests for src/createcompendia/diseasephenotype.py.

Sections:

- ``# --- UMLS semantic-type tree mapping ---`` exercises the UMLS
  semantic-type-tree → Biolink category map that ``write_umls_ids`` hands to
  ``umls.write_umls_ids``. The map is built inline, so we capture it by mocking
  the downstream ``umls.write_umls_ids`` call rather than running a real MRSTY
  parse -- keeping these tests fast and offline.
- ``# --- MONDO_close parsing in compute_cliques_for_impact_report ---`` guards
  the 3-column ``MONDO_close`` concord reader against a column-count regression, and
  checks that excluding MONDO also skips its own MONDO_close close-match data.
- ``# --- classify_disease_clique ---`` checks the per-clique biolink typing used
  by both the real build and the source-impact report.
- ``# --- write_phenotype_taxa ---`` checks the per-prefix taxa file (HP->human,
  MP->mammal) derived from a phenotype ids file.
- ``# --- split_mutually_exclusive_cliques (HP/MP disjointness) ---`` checks that a
  glommed clique holding both HP and MP is split (MP peeled out, HP side kept).
- ``# --- MP data-quality guards ---`` checks that MP gets the same same-prefix
  overmerge guard (``DISEASE_UNIQUE_PREFIXES``) and overused-xref filtering
  (``OVERUSE_FILTERED_CONCORDS``) that MONDO/HP already have.
- ``# --- DOID ICD xref overuse filtering ---`` checks that a DOID ICD xref is dropped exactly
  when its code is claimed by 2+ DOID terms -- an ICD code names a disease *family* -- while
  DOID's 1:1 ICD rows and its other namespaces are left alone.
- ``# CURIE PREFIX NORMALIZATION`` checks the per-source rename maps in
  ``config.yaml: disease_xref_prefixes``: that every target is a prefix Babel defines, that an
  unknown one raises, and that DOID's map covers every prefix DOID actually emits.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.babel_utils import glom, norm, remove_overused_xrefs
from src.categories import DISEASE, PHENOTYPIC_FEATURE
from src.createcompendia import diseasephenotype
from src.prefixes import DOID, GARD, ICD10CM, MONDO
from src.ubergraph import build_sets
from src.util import Text, get_config
from tests.conftest import assert_taxa_file_valid, glom_dict_from_cliques

# --- UMLS semantic-type tree mapping ---


def _capture_umlsmap(tmp_path):
    """Run write_umls_ids with the downstream call mocked, returning the category map it built."""
    badumlsfile = tmp_path / "badumls.txt"
    badumlsfile.write_text("# no blocked CUIs\n")
    with patch.object(diseasephenotype.umls, "write_umls_ids") as mock_write:
        diseasephenotype.write_umls_ids(
            mrsty=str(tmp_path / "MRSTY.RRF"),  # never read: write_umls_ids is mocked
            outfile=str(tmp_path / "out"),
            badumlsfile=str(badumlsfile),
        )
    assert mock_write.call_count == 1, "expected diseasephenotype to delegate to umls.write_umls_ids exactly once"
    # umls.write_umls_ids(mrsty, category_map, outfile, ...): the map is the 2nd positional arg.
    return mock_write.call_args.args[1]


@pytest.mark.unit
def test_finding_and_lab_result_trees_are_not_claimed(tmp_path):
    """
    Regression guard for #569: the disease/phenotype compendium must NOT claim UMLS
    "Finding" (A2.2 / T033) or "Laboratory or Test Result" (A2.2.1 / T034). Leaving them
    unmapped is what lets them fall through to the leftover UMLS sweep, where STY_OVERRIDES
    re-types them (T033 → biolink:Phenomenon, T034 → biolink:ClinicalFinding). If either
    tree is re-added here the override never fires, so fail loudly.
    """
    umlsmap = _capture_umlsmap(tmp_path)
    assert "A2.2" not in umlsmap, 'A2.2 "Finding" (T033) must stay unclaimed so leftover re-types it -- see #569'
    assert "A2.2.1" not in umlsmap, (
        'A2.2.1 "Lab/Test Result" (T034) must stay unclaimed so leftover re-types it -- see #569'
    )


@pytest.mark.unit
def test_phenotype_trees_remain_claimed(tmp_path):
    """A2.2.2 (Sign or Symptom) and A2.3 (Organism Attribute) genuinely are phenotypic features."""
    umlsmap = _capture_umlsmap(tmp_path)
    assert umlsmap.get("A2.2.2") == PHENOTYPIC_FEATURE
    assert umlsmap.get("A2.3") == PHENOTYPIC_FEATURE


@pytest.mark.unit
def test_disease_trees_remain_claimed(tmp_path):
    """The core disease semantic-type trees must still map to biolink:Disease."""
    umlsmap = _capture_umlsmap(tmp_path)
    for tree in [
        "B2.2.1.2.1",
        "A1.2.2.1",
        "A1.2.2.2",
        "B2.3",
        "B2.2.1.2",
        "B2.2.1.2.1.1",
        "B2.2.1.2.2",
        "A1.2.2",
        "B2.2.1.2.1.2",
    ]:
        assert umlsmap.get(tree) == DISEASE, f"{tree} should map to {DISEASE}"


# --- MONDO_close parsing in compute_cliques_for_impact_report ---


def _write_lines(p, lines):
    """Write an iterable of strings to path ``p`` as newline-terminated rows."""
    p.write_text("".join(f"{line}\n" for line in lines))
    return str(p)


@pytest.mark.unit
def test_mondo_close_accepts_three_column_concord(tmp_path):
    """MONDO_close is a 3-column (subject, predicate, object) concord written by
    ubergraph.build_sets(); compute_cliques_for_impact_report() must parse it without
    raising. Regression guard: a reader that assumes 2 columns rejects every real row
    and aborts the whole disease build.
    """
    ids = _write_lines(tmp_path / "MONDO", [f"MONDO:0000001\t{DISEASE}"])
    mondoclose = _write_lines(
        tmp_path / "MONDO_close",
        [
            "MONDO:0000739\toio:closeMatch\tMEDDRA:10051962",
            "",  # blank line must be skipped, not crash
            "MONDO:0000740\toio:closeMatch\tMEDDRA:10001229",
        ],
    )

    dicts, types = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[],
        identifiers=[ids],
        mondoclose=mondoclose,
        badxrefs={},
    )

    assert "MONDO:0000001" in dicts
    assert types["MONDO:0000001"] == DISEASE


@pytest.mark.unit
def test_mondo_close_rejects_malformed_row(tmp_path):
    """A MONDO_close row that is not exactly three tab-separated columns is malformed
    and must raise a clear RuntimeError rather than silently mis-parsing or hitting an
    IndexError deep inside glom.
    """
    ids = _write_lines(tmp_path / "MONDO", [f"MONDO:0000001\t{DISEASE}"])
    mondoclose = _write_lines(tmp_path / "MONDO_close", ["MONDO:0000739\tMEDDRA:10051962"])

    with pytest.raises(RuntimeError, match="not a valid MONDO_close entry"):
        diseasephenotype.compute_cliques_for_impact_report(
            concordances=[],
            identifiers=[ids],
            mondoclose=mondoclose,
            badxrefs={},
        )


@pytest.mark.unit
def test_excluding_mondo_skips_basename_discovered_mondo_close(tmp_path):
    """excluded_sources={"MONDO"} must also skip a basename-discovered MONDO_close (it's
    MONDO's own close-match data), not just MONDO's ids/concord files.

    Regression guard: MONDO_close used to be pulled out of `concordances` -- and therefore
    always read -- before the excluded_sources filter got a chance to run, so a
    ``--source MONDO`` impact-report "before" computation would still load MONDO's
    close-match data even though MONDO was supposed to be fully absent. Demonstrated here via
    a malformed MONDO_close file: if it's ever opened it raises, so "no raise" proves it was
    skipped.
    """
    ids = _write_lines(tmp_path / "HP", [f"HP:0000001\t{PHENOTYPIC_FEATURE}"])
    bad_mondo_close = _write_lines(tmp_path / "MONDO_close", ["MONDO:0000739\tMEDDRA:10051962"])

    with pytest.raises(RuntimeError, match="not a valid MONDO_close entry"):
        diseasephenotype.compute_cliques_for_impact_report(
            concordances=[bad_mondo_close],
            identifiers=[ids],
            badxrefs={},
        )

    dicts, types = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[bad_mondo_close],
        identifiers=[ids],
        excluded_sources={"MONDO"},
        badxrefs={},
    )
    assert set(dicts.keys()) == {"HP:0000001"}


@pytest.mark.unit
@pytest.mark.parametrize("excluded", ["MONDO", "GARD"])
def test_excluding_either_side_skips_mondo_gard(tmp_path, excluded):
    """MONDO_GARD is MONDO's data about GARD, so excluding either source must skip it.

    Otherwise a ``--source GARD`` impact-report "before" run already holds every GARD CURIE through
    MONDO_GARD and reports that adding GARD merged nothing, and a ``--source MONDO`` run under-counts
    MONDO by the ~15.9k joins that concord carries.
    """
    ids = _write_lines(tmp_path / "HP", [f"HP:0000001\t{PHENOTYPIC_FEATURE}"])
    mondo_gard = _write_lines(tmp_path / f"{MONDO}_{GARD}", ["MONDO:0009846\txref\tGARD:418"])

    dicts, _ = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[mondo_gard], identifiers=[ids], badxrefs={}
    )
    assert dicts["MONDO:0009846"] == {"MONDO:0009846", "GARD:418"}

    dicts, _ = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[mondo_gard], identifiers=[ids], excluded_sources={excluded}, badxrefs={}
    )
    assert set(dicts.keys()) == {"HP:0000001"}


@pytest.mark.unit
def test_excluding_mondo_skips_explicit_mondoclose_argument(tmp_path):
    """Excluding MONDO must skip MONDO_close even when `mondoclose` is passed explicitly
    (the production build_compendium() call shape), not only when auto-discovered."""
    ids = _write_lines(tmp_path / "HP", [f"HP:0000001\t{PHENOTYPIC_FEATURE}"])
    bad_mondo_close = _write_lines(tmp_path / "MONDO_close", ["MONDO:0000739\tMEDDRA:10051962"])

    dicts, types = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[],
        identifiers=[ids],
        mondoclose=bad_mondo_close,
        excluded_sources={"MONDO"},
        badxrefs={},
    )
    assert set(dicts.keys()) == {"HP:0000001"}


# --- classify_disease_clique ---


@pytest.mark.unit
def test_classify_disease_clique_trusts_mondo_over_other_members():
    """A mixed clique containing a MONDO term should be typed from MONDO's declared type,
    regardless of what other members (e.g. an MP phenotype) declare. This is the case the
    MP impact report hit: a disease clique that an MP cross-reference expanded must still
    classify as biolink:Disease so the report picks MONDO (not DOID) as the preferred id."""
    clique = frozenset({"DOID:0050545", "MONDO:0018677", "HP:0030853", "MP:0004133"})
    types = {
        "MONDO:0018677": DISEASE,
        "HP:0030853": PHENOTYPIC_FEATURE,
        "MP:0004133": PHENOTYPIC_FEATURE,
        # DOID intentionally has no declared type to prove MONDO is what's trusted.
    }
    assert diseasephenotype.classify_disease_clique(clique, types) == DISEASE


@pytest.mark.unit
def test_classify_disease_clique_falls_through_to_hp_then_mp():
    """When no MONDO is present the classifier should trust HP next, then MP. A clique with
    only HP and MP members should take HP's declared type."""
    clique = frozenset({"HP:0001638", "MP:0005330"})
    types = {"HP:0001638": PHENOTYPIC_FEATURE, "MP:0005330": PHENOTYPIC_FEATURE}
    assert diseasephenotype.classify_disease_clique(clique, types) == PHENOTYPIC_FEATURE


@pytest.mark.unit
def test_classify_disease_clique_skips_trusted_prefix_with_missing_type():
    """If a trusted prefix's CURIE has no entry in the types map (concords out of sync),
    the classifier should fall through to the next trusted prefix rather than raising."""
    clique = frozenset({"MONDO:0000001", "HP:0001638"})
    types = {"HP:0001638": PHENOTYPIC_FEATURE}  # MONDO present but untyped
    assert diseasephenotype.classify_disease_clique(clique, types) == PHENOTYPIC_FEATURE


@pytest.mark.unit
def test_classify_disease_clique_majority_vote_breaks_ties_by_order():
    """With no trusted prefix present, the classifier should take a majority vote over
    declared types, breaking ties by the ``order`` list (DISEASE before PHENOTYPIC_FEATURE)."""
    clique = frozenset({"UMLS:C1", "UMLS:C2"})
    types = {"UMLS:C1": DISEASE, "UMLS:C2": PHENOTYPIC_FEATURE}  # 1-1 tie -> DISEASE wins
    assert diseasephenotype.classify_disease_clique(clique, types) == DISEASE


@pytest.mark.unit
def test_classify_disease_clique_returns_none_when_no_types():
    """A clique whose members are all absent from the types map should return None so the
    source-impact report can render it blank; create_typed_sets turns that None into a
    RuntimeError instead."""
    clique = frozenset({"FOO:1", "BAR:2"})
    assert diseasephenotype.classify_disease_clique(clique, {}) is None


@pytest.mark.unit
def test_create_typed_sets_drops_untypable_clique_with_warning(caplog):
    """create_typed_sets should skip (not crash on) a clique with no declared type for any
    member, logging a warning. The HP/MP split can strand such a clique (an identifier in a
    concord but absent from every ids file), and one stray must not abort the whole build."""
    import logging

    with caplog.at_level(logging.WARNING):
        typed_sets = diseasephenotype.create_typed_sets({frozenset({"FOO:1"})}, {})
    assert typed_sets == {}, "untypeable clique should be dropped, not emitted"
    assert any("untypeable" in r.message.lower() for r in caplog.records), "expected a warning about the dropped clique"


# --- write_phenotype_taxa ---


@pytest.mark.unit
def test_write_phenotype_taxa_assigns_taxon_to_every_id(tmp_path):
    """Every identifier in the ids file should get exactly one row mapping it to the given
    taxon, and the biolink-type column of the ids file should be dropped. This is how HP
    terms become NCBITaxon:9606 and MP terms NCBITaxon:40674 in the compendia."""
    idfile = tmp_path / "HP"
    idfile.write_text(f"HP:0000118\t{PHENOTYPIC_FEATURE}\nHP:0001234\t{PHENOTYPIC_FEATURE}\n")
    outfile = tmp_path / "taxa"
    diseasephenotype.write_phenotype_taxa(str(idfile), "NCBITaxon:9606", str(outfile))
    rows = assert_taxa_file_valid(str(outfile))
    assert rows == [["HP:0000118", "NCBITaxon:9606"], ["HP:0001234", "NCBITaxon:9606"]]


@pytest.mark.unit
def test_write_phenotype_taxa_skips_blank_lines(tmp_path):
    """A blank trailing line in the ids file must not produce a malformed taxa row."""
    idfile = tmp_path / "MP"
    idfile.write_text(f"MP:0000001\t{PHENOTYPIC_FEATURE}\n\n")
    outfile = tmp_path / "taxa"
    diseasephenotype.write_phenotype_taxa(str(idfile), "NCBITaxon:40674", str(outfile))
    assert outfile.read_text() == "MP:0000001\tNCBITaxon:40674\n"


@pytest.mark.unit
def test_write_phenotype_taxa_rejects_non_ncbitaxon(tmp_path):
    """A taxon that is not an NCBITaxon CURIE is a configuration error and must raise,
    rather than silently writing a malformed taxa file the TaxonFactory can't use."""
    idfile = tmp_path / "HP"
    idfile.write_text(f"HP:0000118\t{PHENOTYPIC_FEATURE}\n")
    with pytest.raises(ValueError, match="NCBITaxon"):
        diseasephenotype.write_phenotype_taxa(str(idfile), "9606", str(tmp_path / "taxa"))


# --- split_mutually_exclusive_cliques (HP/MP disjointness) ---


def _glom_dict(*cliques):
    """Varargs convenience wrapper over the shared glom_dict_from_cliques test helper."""
    return glom_dict_from_cliques(cliques)


@pytest.mark.unit
def test_split_separates_mp_from_hp_keeping_rest():
    """A clique holding HP, MP, and MONDO should split: HP and MONDO stay together (one
    shared object), MP is peeled into its own clique, and the two are distinct objects."""
    dicts = _glom_dict(["HP:0000118", "MP:0004133", "MONDO:0018677"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts)
    assert dicts["HP:0000118"] == {"HP:0000118", "MONDO:0018677"}
    assert dicts["HP:0000118"] is dicts["MONDO:0018677"]
    assert dicts["MP:0004133"] == {"MP:0004133"}
    assert dicts["MP:0004133"] is not dicts["HP:0000118"]


@pytest.mark.unit
def test_split_leaves_mp_without_hp_untouched():
    """A clique with MP and MONDO but no HP should be left intact (MP may merge with non-HP
    disease ids); all members keep pointing at one shared, unchanged set."""
    dicts = _glom_dict(["MP:0002989", "MONDO:0005110"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts)
    assert dicts["MP:0002989"] == {"MP:0002989", "MONDO:0005110"}
    assert dicts["MP:0002989"] is dicts["MONDO:0005110"]


@pytest.mark.unit
def test_split_leaves_pure_hp_clique_untouched():
    """A clique with HP and MONDO but no MP should be unchanged."""
    dicts = _glom_dict(["HP:0001638", "MONDO:0005110"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts)
    assert dicts["HP:0001638"] == {"HP:0001638", "MONDO:0005110"}
    assert dicts["HP:0001638"] is dicts["MONDO:0005110"]


@pytest.mark.unit
def test_split_pulls_all_mp_ids_into_one_clique():
    """Every MP identifier in an HP-bearing clique should be peeled into a single MP clique,
    leaving HP plus any non-group members (e.g. MESH) behind."""
    dicts = _glom_dict(["HP:0001638", "MP:0010412", "MP:0011667", "MESH:D004694"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts)
    assert dicts["MP:0010412"] == {"MP:0010412", "MP:0011667"}
    assert dicts["MP:0010412"] is dicts["MP:0011667"]
    assert dicts["HP:0001638"] == {"HP:0001638", "MESH:D004694"}


@pytest.mark.unit
def test_split_keeps_earliest_occupied_prefix_not_group_zero():
    """When a group's first-listed prefix is absent, the earliest *occupied* prefix should keep
    the out-of-group members. For group [HP, MP, MESH] over an MP+MESH+MONDO clique, MP keeps
    MONDO and MESH is peeled off -- peeling everything after group[0] would strand MONDO alone."""
    dicts = _glom_dict(["MP:0002989", "MESH:D004694", "MONDO:0005110"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts, exclusive_prefix_groups=[["HP", "MP", "MESH"]])
    assert dicts["MP:0002989"] == {"MP:0002989", "MONDO:0005110"}
    assert dicts["MP:0002989"] is dicts["MONDO:0005110"]
    assert dicts["MESH:D004694"] == {"MESH:D004694"}


@pytest.mark.unit
def test_split_applies_each_group_to_the_previous_group_s_remainder():
    """With more than one group, each group should split what the previous group left behind.
    [HP, MP] peels MP off an HP+MP+MESH+UMLS clique; [MESH, UMLS] then splits the HP remainder,
    leaving HP+MESH together and UMLS alone. The peeled MP clique is single-prefix and so cannot
    split again."""
    dicts = _glom_dict(["HP:0001638", "MP:0010412", "MESH:D004694", "UMLS:C0018799"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts, exclusive_prefix_groups=[["HP", "MP"], ["MESH", "UMLS"]])
    assert dicts["MP:0010412"] == {"MP:0010412"}
    assert dicts["HP:0001638"] == {"HP:0001638", "MESH:D004694"}
    assert dicts["HP:0001638"] is dicts["MESH:D004694"]
    assert dicts["UMLS:C0018799"] == {"UMLS:C0018799"}


@pytest.mark.unit
def test_split_matches_prefixes_case_insensitively():
    """Group prefixes should be matched case-insensitively, so a lower-case constant (as
    prefixes.ORPHANET is) still triggers a split rather than silently failing open."""
    dicts = _glom_dict(["HP:0001638", "orphanet:12345"])
    diseasephenotype.split_mutually_exclusive_cliques(dicts, exclusive_prefix_groups=[["HP", "orphanet"]])
    assert dicts["HP:0001638"] == {"HP:0001638"}
    assert dicts["orphanet:12345"] == {"orphanet:12345"}


@pytest.mark.unit
def test_split_then_create_typed_sets_routes_mp_to_phenotypic_feature():
    """End-to-end build/report contract: after splitting an HP+MP+MONDO clique, create_typed_sets
    should route the peeled MP-only clique to PhenotypicFeature and keep the HP/MONDO clique as
    Disease (its pre-split type, since MONDO is trusted first)."""
    dicts = _glom_dict(["HP:0000118", "MP:0004133", "MONDO:0018677"])
    types = {
        "HP:0000118": PHENOTYPIC_FEATURE,
        "MP:0004133": PHENOTYPIC_FEATURE,
        "MONDO:0018677": DISEASE,
    }
    diseasephenotype.split_mutually_exclusive_cliques(dicts)
    typed_sets = diseasephenotype.create_typed_sets({frozenset(x) for x in dicts.values()}, types)
    assert frozenset({"MP:0004133"}) in typed_sets[PHENOTYPIC_FEATURE]
    assert frozenset({"HP:0000118", "MONDO:0018677"}) in typed_sets[DISEASE]


# --- MP data-quality guards ---


@pytest.mark.unit
def test_mp_included_in_unique_prefixes_blocks_same_prefix_merge():
    """DISEASE_UNIQUE_PREFIXES must include MP so two distinct MP ids never merge into one
    clique via a shared bridge -- the same protection MONDO and HP already get.

    Regression guard: MP was added to disease_ids/disease_concords without being added to
    DISEASE_UNIQUE_PREFIXES, silently losing this data-quality guard.
    """
    dicts = {}
    glom(dicts, [("MP:0000001",), ("MP:0000002",)], unique_prefixes=diseasephenotype.DISEASE_UNIQUE_PREFIXES)
    glom(dicts, [("MP:0000001", "MESH:D000001")], unique_prefixes=diseasephenotype.DISEASE_UNIQUE_PREFIXES)
    # MP:0000002 bridging to the same MESH id would merge it with MP:0000001's clique; with MP
    # in unique_prefixes that merge must be rejected, leaving MP:0000002 in its own clique.
    glom(dicts, [("MP:0000002", "MESH:D000001")], unique_prefixes=diseasephenotype.DISEASE_UNIQUE_PREFIXES)

    assert dicts["MP:0000002"] == {"MP:0000002"}
    assert dicts["MP:0000001"] == {"MP:0000001", "MESH:D000001"}


@pytest.mark.unit
def test_mp_concords_are_overuse_filtered(tmp_path):
    """OVERUSE_FILTERED_CONCORDS must include "MP" so an MP xref target shared by multiple MP
    source ids is dropped the same way an overused MONDO/HP/EFO xref target would be.

    Regression guard: MP concords were previously trusted as-is (not filtered through
    remove_overused_xrefs), so a promiscuous MP xref target could silently fuse unrelated MP
    cliques together via the shared target.
    """
    ids = _write_lines(
        tmp_path / "MP_ids",
        [f"MP:0000001\t{PHENOTYPIC_FEATURE}", f"MP:0000002\t{PHENOTYPIC_FEATURE}"],
    )
    concord_dir = tmp_path / "concords"
    concord_dir.mkdir()
    concord = _write_lines(
        concord_dir / "MP",  # basename must be "MP" to hit OVERUSE_FILTERED_CONCORDS
        [
            "MP:0000001\txref\tMESH:D000001",
            "MP:0000002\txref\tMESH:D000001",
        ],
    )

    dicts, types = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[concord],
        identifiers=[ids],
        badxrefs={},
    )

    assert dicts["MP:0000001"] == {"MP:0000001"}
    assert dicts["MP:0000002"] == {"MP:0000002"}


# --- DOID ICD xref overuse filtering ---


@pytest.mark.unit
def test_doid_icd_xref_prefixes_are_the_icd_families():
    """DOID_ICD_XREF_PREFIXES must list every ICD flavour DOID emits, in post-norm() spelling.

    These are the prefixes the overuse filter is scoped to, so a flavour missing here is a
    namespace whose family codes go back to fusing every subtype that cites them. Regression
    guard against the list being emptied or a flavour dropped -- see docs/sources/DOID/mappings.md
    and issue #1029."""
    from src.prefixes import ICD0, ICD9, ICD10, ICD11

    assert diseasephenotype.DOID_ICD_XREF_PREFIXES == [ICD10, ICD9, ICD0, ICD11]


@pytest.mark.unit
def test_doid_overuse_filter_is_scoped_to_icd_and_gard():
    """DOID must be overuse-filtered, but only over its ICD prefixes and GARD.

    Unscoped (None, as MONDO/HP/EFO/MP are) the filter would also discard correct mappings:
    MESH:D010195 "Pancreatitis" is claimed by both DOID:4989 "pancreatitis" and DOID:2913 "acute
    pancreatitis", and dropping the target loses the genuine equivalence along with the too-narrow
    one. Scoped to ICD and GARD it drops only the family codes and the doubly-claimed registry ids.

    MONDO_GARD is scoped to GARD for a different reason: MONDO is a unique prefix, so a GARD id
    a future MONDO release maps twice would otherwise be silently awarded to whichever row sorts
    first."""
    assert diseasephenotype.OVERUSE_FILTERED_CONCORDS["DOID"] == diseasephenotype.DOID_ICD_XREF_PREFIXES + [GARD]
    assert diseasephenotype.OVERUSE_FILTERED_CONCORDS[f"{MONDO}_{GARD}"] == [GARD]
    assert all(diseasephenotype.OVERUSE_FILTERED_CONCORDS[c] is None for c in ("MONDO", "HP", "EFO", "MP"))


@pytest.mark.unit
def test_scoped_overuse_filter_drops_only_the_named_namespace():
    """remove_overused_xrefs(target_prefixes=...) must leave other namespaces alone.

    This is the whole point of the scoping: an ICD family code claimed twice goes, while a MeSH
    target claimed twice -- the pancreatitis case -- stays."""
    pairs = [
        ("DOID:2476", "ICD10:G11.4"),
        ("DOID:0110764", "ICD10:G11.4"),
        ("DOID:13258", "ICD10:A01.0"),
        ("DOID:4989", "MESH:D010195"),
        ("DOID:2913", "MESH:D010195"),
    ]
    kept = remove_overused_xrefs(pairs, target_prefixes=diseasephenotype.DOID_ICD_XREF_PREFIXES)

    assert ("DOID:13258", "ICD10:A01.0") in kept, "a 1:1 ICD row must survive"
    assert not [pair for pair in kept if pair[1] == "ICD10:G11.4"], "an overused ICD row must go"
    assert [pair for pair in kept if pair[1] == "MESH:D010195"] == pairs[3:], "MeSH is out of scope"
    # Unscoped, the same call takes the MeSH rows too -- the behaviour the scoping avoids.
    assert not [pair for pair in remove_overused_xrefs(pairs) if pair[1] == "MESH:D010195"]


@pytest.mark.unit
def test_build_disease_doid_relationships_keeps_icd_in_the_concord():
    """The concord must KEEP DOID's ICD rows -- they are filtered at glom time, not at build time.

    Excluding them here instead would drop the 4,841 1:1 rows along with the 1,584 overused ones,
    and would erase them from the concord the audit tools read. Invert this only alongside a
    decision to go back to a categorical exclusion.

    write_concord_metadata is patched because it opens concord_filename to count rows, which would
    fail here against a path no build produced."""
    with (
        patch.object(diseasephenotype.doid, "build_xrefs") as mock_build,
        patch.object(diseasephenotype, "write_concord_metadata") as mock_meta,
    ):
        diseasephenotype.build_disease_doid_relationships("doid.json", "out", "meta.yaml")

    assert mock_build.call_count == 1
    assert "excluded_target_prefixes" not in mock_build.call_args.kwargs
    assert "overuse-filtered at glom" in mock_meta.call_args.kwargs["description"]


# --- EFO->MP xref exclusion (MP disjointness at the EFO source) ---


@pytest.mark.unit
def test_efo_excluded_xref_prefixes_is_mp():
    """EFO_EXCLUDED_XREF_PREFIXES must list MP so EFO's untrusted direct xrefs to Mammalian
    Phenotype terms are dropped at the source, keeping MP disjoint from EFO. Regression guard
    against the constant being emptied or repointed. See docs/sources/MP/disjointness.md."""
    from src.prefixes import MP

    assert diseasephenotype.EFO_EXCLUDED_XREF_PREFIXES == [MP]


@pytest.mark.unit
def test_build_disease_efo_relationships_forwards_excluded_prefixes():
    """build_disease_efo_relationships must forward EFO_EXCLUDED_XREF_PREFIXES into
    efo.make_concords, so the EFO->MP filter actually runs during the build (not just in the
    handler when a caller opts in)."""
    with patch.object(diseasephenotype.efo, "make_concords") as mock_make:
        diseasephenotype.build_disease_efo_relationships("efo.owl", "ids", "out", "meta.yaml")
    assert mock_make.call_count == 1
    assert mock_make.call_args.kwargs["excluded_target_prefixes"] == diseasephenotype.EFO_EXCLUDED_XREF_PREFIXES


@pytest.mark.unit
def test_read_badxrefs_skips_comments_and_parses_shipped_mondo_file():
    """read_badxrefs must skip ``#`` comment lines and parse the remaining SPACE-separated
    ``subject object`` pairs. The shipped mondo_badxrefs.txt must still drop
    MONDO:0003425 -> SNOMEDCT:78097002: that xref points "ophthalmoplegia" at SNOMED's "Total
    ophthalmoplegia" and competes with the correct UMLS:C0029089 bridge to HP:0000602, so which
    HP the clique keeps would otherwise depend on concord line order. Note the file is
    space-separated while concords are tab-separated -- reformatting it with tabs would silently
    parse every line into a single field and drop every entry."""
    bad_pairs = diseasephenotype.read_badxrefs("input_data/mondo_badxrefs.txt")
    assert ("MONDO:0003425", "SNOMEDCT:78097002") in bad_pairs
    # Comment lines never become pairs.
    assert not any(subject.startswith("#") for subject, _ in bad_pairs)


@pytest.mark.unit
def test_mp_badxrefs_is_wired_up_and_drops_the_bifid_scrotum_xref():
    """The MP concord must be filtered through input_data/mp_badxrefs.txt, which must still drop
    MP:0009203 -> UMLS:C0341787. MP:0009203 is "external male genitalia hypoplasia" (a broad
    underdevelopment term) while UMLS:C0341787 is "Bifid scrotum" (a specific malformation, also
    HP:0000048), so the xref would clique two different concepts.

    Regression guard: the pair is only dropped if "MP" is a key in the badxrefs dict, since
    build_compendia looks the file up by concord basename. The [HP, MP] post-glom split currently
    masks the bad merge, so nothing else in the build would notice this silently regressing.
    See https://github.com/NCATSTranslator/Babel/issues/906 for the live BabelTest assertions.
    """
    assert "MP" in diseasephenotype.DEFAULT_BAD_XREFS
    bad_pairs = diseasephenotype.read_badxrefs(diseasephenotype.DEFAULT_BAD_XREFS["MP"])
    assert ("MP:0009203", "UMLS:C0341787") in bad_pairs


@pytest.mark.unit
def test_doid_overuse_filter_drops_gard_ids_claimed_twice():
    """A GARD id cited by two DOID terms must be dropped from DOID's concord, like an ICD family code.

    GARD:418 "Essential pentosuria" is the worked case: DOID:0111258 "pentosuria" xrefs it correctly
    and DOID:0061030 "hemophilia" xrefs it by typo (``GARD:0418`` for GARD:10418; reported as
    DiseaseOntology/HumanDiseaseOntology#1620). Both cliques hold a MONDO id, so glom() could not
    merge them and would hand the contested id to whichever row it saw first. Scoping the overuse
    filter to GARD as well as ICD removes both rows; MONDO_GARD, glommed earlier, already places
    GARD:418 in the pentosuria clique, so nothing is lost. 12 of DOID's GARD targets are in this
    position (GARD:625 Alport 3B/2, GARD:7674 childhood SMA, ...).
    """
    pairs = [
        ("DOID:0111258", "GARD:418"),
        ("DOID:0061030", "GARD:418"),
        ("DOID:0050012", "GARD:6038"),
        ("DOID:4989", "MESH:D010195"),
        ("DOID:2913", "MESH:D010195"),
    ]
    kept = remove_overused_xrefs(pairs, target_prefixes=diseasephenotype.OVERUSE_FILTERED_CONCORDS["DOID"])

    assert not [pair for pair in kept if pair[1] == "GARD:418"], "a GARD id claimed twice must go"
    assert ("DOID:0050012", "GARD:6038") in kept, "a 1:1 GARD row must survive"
    assert [pair for pair in kept if pair[1] == "MESH:D010195"] == pairs[3:], "MeSH stays out of scope"


@pytest.mark.unit
def test_badxrefs_key_matching_no_concord_raises(tmp_path):
    """A bad-xrefs key that matches no concord basename (a typo, or DEFAULT_BAD_XREFS and the
    snakefile dict drifting apart) must raise rather than silently never filtering -- the
    footgun the DEFAULT_BAD_XREFS docstring warns about. The error should name the offending key.
    """
    ids = _write_lines(tmp_path / "MONDO", [f"MONDO:0000001\t{DISEASE}"])
    concord = _write_lines(tmp_path / "MONDO_concord", ["MONDO:0000001\txref\tMESH:D000001"])

    with pytest.raises(ValueError, match="Mondo"):  # deliberate case typo of the real "MONDO" concord
        diseasephenotype.compute_cliques_for_impact_report(
            concordances=[concord],
            identifiers=[ids],
            badxrefs={"Mondo": str(tmp_path / "whatever.txt")},
        )


@pytest.mark.unit
def test_badxrefs_key_for_excluded_source_does_not_raise(tmp_path):
    """Excluding a source keeps its concord in the list (skipped inside the loop), so its
    bad-xrefs key still matches a basename and must not trip the guard: a `--source MONDO`
    impact-report before-run passes DEFAULT_BAD_XREFS (which carries "MONDO") unchanged."""
    ids = _write_lines(tmp_path / "HP", [f"HP:0000001\t{PHENOTYPIC_FEATURE}"])
    mondo_concord = _write_lines(tmp_path / "MONDO", ["MONDO:0000001\txref\tMESH:D000001"])
    badxrefs_file = _write_lines(tmp_path / "mondo_bad.txt", ["MONDO:0000001 MESH:D000001"])

    dicts, types = diseasephenotype.compute_cliques_for_impact_report(
        concordances=[mondo_concord],
        identifiers=[ids],
        excluded_sources={"MONDO"},
        badxrefs={"MONDO": badxrefs_file},
    )
    assert set(dicts.keys()) == {"HP:0000001"}


# --- MONDO GARD xref exception ---


@pytest.mark.unit
def test_mondo_gard_concord_keeps_only_unpadded_gard_targets(tmp_path):
    """build_disease_obo_relationships must write MONDO's GARD hasDbXrefs, and *only* those.

    Exercises the real call site rather than re-specifying it, because both arguments it passes are
    load-bearing and neither is visible anywhere else: `allowed_prefixes={GARD}` is what keeps
    MONDO's other hasDbXref targets out (ICD9:759.89 alone is claimed by 167 MONDO terms), and
    the `GARD: GARD` entry of config.yaml's disease_xref_prefixes[MONDO] (resolved to
    normalize_gard_curie) is what strips the registry padding MONDO writes -- a padded GARD:0010418
    here would join neither GARD's ids file nor DOID's unpadded xrefs, so the row would look present
    and do nothing.

    The same term is returned for both MONDO roots walked, and must be written once: the overuse
    filter counts rows, so a doubled pair would read as a GARD id claimed by two MONDO terms.

    The targets below are the shapes MONDO really emits; GARD:0010418 is its mapping for
    MONDO:0018660 "hemophilia". See docs/sources/MONDO/README.md.
    """
    xrefs = {
        "MONDO:0018660": {  # "hemophilia"
            "GARD:0010418",  # the registry mapping, zero-padded -- kept, unpadded to GARD:10418
            "ICD9:759.89",  # a family code claimed by 167 MONDO terms -- dropped
            "MedDRA:10001843",  # not 1:1, and unreviewed -- dropped
            "HP:0002754",  # crosses the disease/phenotype boundary -- dropped
            "https://en.wikipedia.org/wiki/Haemophilia",  # a web page -- dropped
        }
    }
    fake_uber = MagicMock()
    fake_uber.get_subclasses_and_xrefs.return_value = xrefs
    fake_uber.get_subclasses_and_exacts.return_value = {}
    fake_uber.get_subclasses_and_close.return_value = {}

    outdir = tmp_path / "concords"
    outdir.mkdir()
    metadata_yamls = {
        name: str(tmp_path / f"metadata-{name}.yaml") for name in ("HP", "MONDO", "MONDO_close", "MONDO_GARD", "MP")
    }

    with patch("src.ubergraph.UberGraph", return_value=fake_uber):
        diseasephenotype.build_disease_obo_relationships(str(outdir), metadata_yamls)

    rows = (outdir / f"{MONDO}_{GARD}").read_text().splitlines()
    assert rows == [f"{MONDO}:0018660\txref\t{GARD}:10418"]


# --- MP xref allowlist ---


@pytest.mark.unit
def test_mp_xref_allowlist_drops_non_phenotype_targets(tmp_path):
    """build_disease_obo_relationships must pass MP_XREF_ALLOWED_PREFIXES to build_sets, keeping
    only phenotype-shaped xref targets (HP/MGI/MPATH/UMLS) and dropping the anatomy, process,
    registry-code, citation and bare-URL targets MP asserts with oboInOwl:hasDbXref.

    The targets below are real rows from the MP UberGraph xref dump. Note the allowlist is matched
    against Text.get_prefix_or_none(), which upper-cases, so "https://..." must be rejected via
    the prefix "HTTPS" -- a lower-case allowlist entry would silently let every URL through.
    """
    xrefs = {
        "MP:0009873": {  # "abnormal aorta tunica media morphology"
            "MA:0002903",  # the anatomical structure that is abnormal -- dropped
            "FMA:19039",  # ditto, human anatomy -- dropped
            "MGI:2173579",  # MGI phenotype-slim term -- kept
        },
        "MP:0002998": {  # "abnormal bone remodeling"
            "GO:0046849",  # the process the phenotype perturbs -- dropped
            "MPATH:720",  # mouse pathology lesion -- kept
        },
        "MP:0012051": {  # "spasticity"
            "HP:0001257",  # genuine phenotype equivalence -- kept
            "UMLS:C0026838",  # ditto -- kept
            "Fyler:4876",  # defunct cardiac-lesion registry code -- dropped
            "CL:0000806",  # the cell type involved -- dropped
            "PMID:1754386",  # a citation -- dropped
            "https://en.wikipedia.org/wiki/Aorta",  # a web page -- dropped
        },
    }

    fake_uber = MagicMock()
    fake_uber.get_subclasses_and_xrefs.return_value = xrefs
    outdir = tmp_path / "concords"
    outdir.mkdir()

    with patch("src.ubergraph.UberGraph", return_value=fake_uber):
        with open(outdir / "MP", "w") as outfile:
            build_sets(
                "MP:0000001",
                {"MP": outfile},
                set_type="xref",
                allowed_prefixes=diseasephenotype.MP_XREF_ALLOWED_PREFIXES,
            )

    targets = {line.rstrip("\n").split("\t")[2] for line in (outdir / "MP").read_text().splitlines()}
    assert targets == {"MGI:2173579", "MPATH:720", "HP:0001257", "UMLS:C0026838"}


@pytest.mark.unit
@pytest.mark.parametrize("argument_name", ["ignore_list", "allowed_prefixes"])
def test_build_sets_rejects_non_upper_case_prefix_filters(argument_name):
    """A prefix filter entry that isn't upper-case can never match Text.get_prefix_or_none()'s
    upper-cased output. build_sets should raise ValueError naming the offending entries rather
    than silently ignoring the filter -- which for ignore_list would fail open. It must raise
    before contacting UberGraph, so no network patching is needed here."""
    with pytest.raises(ValueError, match="can never match.*orphanet"):
        build_sets("MONDO:0000001", {}, set_type="xref", **{argument_name: ["MESH", "orphanet"]})


@pytest.mark.unit
def test_build_sets_accepts_the_allowlist_this_module_ships():
    """The MP allowlist must satisfy build_sets' upper-case check; this pins the constant so a
    future lower-case addition (e.g. "mpath") fails here rather than silently dropping every MP
    xref of that namespace."""
    diseasephenotype_allowlist = diseasephenotype.MP_XREF_ALLOWED_PREFIXES
    assert diseasephenotype_allowlist == [p.upper() for p in diseasephenotype_allowlist]


# CURIE PREFIX NORMALIZATION (config.yaml: disease_xref_prefixes)


@pytest.mark.unit
@pytest.mark.parametrize("source", ["DOID", "HP", "MONDO"])
def test_xref_prefix_map_targets_are_registered_prefixes(source):
    """Every rename target in config.yaml must be a prefix src/prefixes.py defines.

    A typo'd target renames CURIEs into a namespace no ids file carries, which norm() cannot
    detect and glom() happily merges through -- the same silent failure the config block exists
    to prevent, just moved one step later."""
    known = set(Text.prefixmap.values())
    assert (
        set(diseasephenotype.get_xref_prefix_map(source).values())
        - known
        - {rename for rename in diseasephenotype.LOCAL_ID_DEPENDENT_RENAMES.values()}
        == set()
    )


@pytest.mark.unit
def test_xref_prefix_map_raises_on_an_unregistered_target(monkeypatch):
    """An unknown target prefix should fail loudly at load, naming the offender."""
    monkeypatch.setattr(
        diseasephenotype,
        "get_config",
        lambda: {"disease_xref_prefixes": {"DOID": {"NCI": "NOT_A_REAL_PREFIX"}}},
    )
    with pytest.raises(ValueError, match="NOT_A_REAL_PREFIX"):
        diseasephenotype.get_xref_prefix_map("DOID")


@pytest.mark.unit
def test_doid_xref_prefix_map_covers_every_prefix_doid_emits():
    """DOID's map must rename every non-Babel prefix DOID uses, and reach it via the stem.

    These are the spellings in the DOID release of 2026-08-18. `MIM`, `SNOMEDCT_US` and `ORDO`
    were the three that were missing -- 6,483, 5,358 and 2,321 rows respectively, every one of
    them reaching glom() un-renamed, joining nothing and fusing its subjects anyway. GARD is a
    local-id rename, not a prefix one: 28 of DOID's GARD xrefs carry the registry's zero padding."""
    mapping = diseasephenotype.get_xref_prefix_map(DOID)
    assert set(mapping) == {
        "ICD10CM",
        "ICD9CM",
        "ICDO",
        "NCI",
        "SNOMEDCT_US",
        "UMLS_CUI",
        "KEGG",
        "MIM",
        "ORDO",
        "GARD",
    }
    # The stem entry has to cover the dated spellings, which is norm()'s job, not the map's.
    assert norm("SNOMEDCT_US_2026_03_01:267692008", mapping) == "SNOMEDCT:267692008"
    assert norm("MIM:PS303350", mapping) == "OMIM.PS:303350"
    assert norm("MIM:115210", mapping) == "OMIM:115210"
    # Lower-case target: the rename must land on Babel's spelling, which MONDO and HP already use.
    assert norm("ORDO:2822", mapping) == "orphanet:2822"
    # Local-id rename: the prefix stays, the padding goes (the 28 padded ones are all this shape).
    assert norm("GARD:0018564", mapping) == "GARD:18564"
    assert norm("GARD:6038", mapping) == "GARD:6038"
    # MONDO pads every GARD id, and its map must unpad them too or MONDO_GARD joins nothing.
    assert norm("GARD:0010418", diseasephenotype.get_xref_prefix_map(MONDO)) == "GARD:10418"


@pytest.mark.unit
def test_disease_extra_prefixes_are_registered_and_deliberate():
    """config.yaml: disease_extra_prefixes overrides the Biolink Model, so it must stay short.

    Each entry ships a prefix Biolink does not register for biolink:Disease -- deliberately, since
    write_compendium() would otherwise drop it silently *after* it had already merged cliques. The
    entries must be real prefixes from src/prefixes.py, and ICD0 must stay out: an ICD-O code is a
    tumour morphology, so emitting one asserts a disease equivalence nobody has decided (#1037).
    Update this test alongside the config, not instead of it."""
    from src.prefixes import GARD, ICD0, ICD10CM

    extra = get_config()["disease_extra_prefixes"]

    assert extra == [ICD10CM, GARD], "adding a prefix here overrides Biolink; say why in config.yaml first"
    assert set(extra) <= set(Text.prefixmap.values()), "every entry must be a src/prefixes.py constant"
    assert ICD0 not in extra


@pytest.mark.unit
def test_icd10cm_override_expires_when_the_spelling_is_unified():
    """The ICD10CM override must not outlive the reason for it.

    It exists only because MONDO emits `ICD10CM:` while DOID/EFO/HP emit `ICD10:`, and Biolink
    registers only the latter -- so without it MONDO's ~2,030 curated ICD-10 mappings merge cliques
    in glom() and are then dropped by write_compendium(). When
    https://github.com/NCATSTranslator/Babel/issues/1033 lands and MONDO's map renames ICD10CM to
    ICD10, the override becomes dead weight that keeps a non-Biolink prefix alive for no reason.

    Nothing else would notice, so this fails the moment the two config entries disagree: if MONDO
    renames ICD10CM away, ICD10CM must come out of disease_extra_prefixes in the same change."""
    mondo_renames = get_config()["disease_xref_prefixes"][MONDO]
    extra = get_config()["disease_extra_prefixes"]

    if "ICD10CM" in mondo_renames:
        assert ICD10CM not in extra, (
            "MONDO now renames ICD10CM, so nothing emits that prefix any more -- drop it from "
            "config.yaml: disease_extra_prefixes (see issue #1033)."
        )


@pytest.mark.unit
def test_build_compendium_passes_the_extra_prefixes_through():
    """The override is useless unless it reaches write_compendium, so pin the wiring."""
    with (
        patch.object(diseasephenotype, "compute_cliques_for_impact_report", return_value=({}, {})),
        patch.object(diseasephenotype, "create_typed_sets", return_value={DISEASE: [["MONDO:1"]]}),
        patch.object(diseasephenotype, "write_compendium") as mock_write,
    ):
        diseasephenotype.build_compendium([], {}, [], None, {}, "icRDF.tsv")

    assert mock_write.call_args.kwargs["extra_prefixes"] == get_config()["disease_extra_prefixes"]


@pytest.mark.unit
def test_extra_prefixes_are_scoped_to_disease():
    """The override must reach Disease.txt and NOT PhenotypicFeature.txt.

    extra_prefixes is a per-class allowlist, and every entry in disease_extra_prefixes is justified
    on disease grounds -- ICD10CM is a disease classification, GARD a rare-disease registry. Passing
    the list unscoped would let both into a phenotype clique without either facing
    PhenotypicFeature's own prefix filter, which is the check that is supposed to catch a
    disease/phenotype merge going wrong."""
    typed = {DISEASE: [["MONDO:1"]], PHENOTYPIC_FEATURE: [["HP:1"]]}
    with (
        patch.object(diseasephenotype, "compute_cliques_for_impact_report", return_value=({}, {})),
        patch.object(diseasephenotype, "create_typed_sets", return_value=typed),
        patch.object(diseasephenotype, "write_compendium") as mock_write,
    ):
        diseasephenotype.build_compendium([], {}, [], None, {}, "icRDF.tsv")

    by_type = {call.args[3]: call.kwargs["extra_prefixes"] for call in mock_write.call_args_list}

    assert by_type[DISEASE] == get_config()["disease_extra_prefixes"]
    assert by_type[PHENOTYPIC_FEATURE] == []


@pytest.mark.unit
def test_disease_phenotype_boundary_badxrefs_are_shipped_and_parse():
    """The four pairs that keep two diseases out of the phenotype cliques named after them must
    survive in the shipped files, split across the two concords they belong to.

    All four are load-bearing: a replay over a built intermediate set showed that dropping any one
    of them puts DOID:206 "hereditary multiple exostoses" or DOID:0050424 "familial adenomatous
    polyposis" back into a biolink:PhenotypicFeature clique, which registers none of DOID, OMIM or
    orphanet -- so write_compendium() silently drops those identifiers. The clique diff for #1031
    caught exactly that, losing 5 identifiers.

    Note the bridge runs both ways: UMLS asserts C0015306 -> HP:0002762 and HP asserts the reverse,
    so blocking one direction is not enough. Cutting DOID's own edges instead does NOT work --
    DOID:206 still reaches the phenotype through OMIM:133700 -- which is why these sit on the
    UMLS and HP concords rather than a DOID one."""
    umls_pairs = diseasephenotype.read_badxrefs("input_data/umls_badxrefs.txt")
    hp_pairs = diseasephenotype.read_badxrefs("input_data/badHPx.txt")

    assert ("UMLS:C0015306", "HP:0002762") in umls_pairs
    assert ("HP:0002762", "UMLS:C0015306") in hp_pairs
    assert ("HP:0002762", "SNOMEDCT:254044004") in hp_pairs
    assert ("HP:0005227", "MEDDRA:10056981") in hp_pairs
    # HP:0005227's own UMLS mapping is a leaf and must stay -- blocking it would strip a correct
    # phenotype mapping to fix a transitive problem it does not cause.
    assert ("HP:0005227", "UMLS:C1868071") not in hp_pairs


@pytest.mark.unit
def test_mondo_gard_concord_is_registered_in_disease_concords():
    """The MONDO_GARD concord must be listed in config.yaml: disease_concords.

    build_disease_obo_relationships() writes the file either way, and the Snakemake rule declares it
    as an output either way -- but build_compendia only reads the concords config lists. Omitted, the
    file is built on every run, looks correct on disk, and contributes nothing: 15,930 GARD
    identifiers silently revert to single-identifier cliques duplicating MONDO concepts. That is a
    failure with no error message anywhere, which is what makes it worth pinning.

    It must also come BEFORE DOID. 148 of DOID's GARD xref rows put a GARD id on a different MONDO
    clique than MONDO does; MONDO is a unique prefix so the cliques cannot merge, and glom() leaves
    the id with whichever concord claimed it first. An alphabetical tidy-up of the list would
    silently re-home those ids -- see the comment on disease_concords in config.yaml.
    """
    concords = get_config()["disease_concords"]
    assert f"{MONDO}_{GARD}" in concords
    assert concords.index(f"{MONDO}_{GARD}") < concords.index(DOID), "MONDO_GARD must be glommed before DOID"


@pytest.mark.unit
def test_badxrefs_files_are_registered_for_the_concords_they_name():
    """Every DEFAULT_BAD_XREFS key must name a concord the disease build actually produces.

    A bad-xrefs file has to be registered in two places (this dict and the disease_compendia rule),
    and a key matching no concord silently filters nothing -- the footgun docs/sources/CLAUDE.md
    warns about. compute_cliques_for_impact_report() raises on an unknown key, but only once it has
    a concord list to check against; this pins the dict itself against config.yaml."""
    known_concords = set(get_config()["disease_concords"])

    assert set(diseasephenotype.DEFAULT_BAD_XREFS) <= known_concords, (
        f"bad-xrefs keys naming no concord: {sorted(set(diseasephenotype.DEFAULT_BAD_XREFS) - known_concords)}"
    )
    for name, path in diseasephenotype.DEFAULT_BAD_XREFS.items():
        assert diseasephenotype.read_badxrefs(path) is not None, f"{name} bad-xrefs file failed to parse"


# --- GARD_LABEL concord (build_gard_label_concord) ---
#
# GARD publishes no cross-references, so the registry terms MONDO and DOID do not map can only reach
# a clique through a label match. These pin the three guards that make that safe -- skip GARD ids
# another concord places, emit at most one row each, and never target a non-Disease clique -- plus
# the config wiring the guards depend on. Numbers and provenance: docs/sources/GARD/label-matches.md.


def _run_gard_label_concord(tmp_path, *, gard, vocabularies, other_concords=(), extra_ids=()):
    """Run build_gard_label_concord() over tiny inputs, returning the concord rows it wrote.

    :param gard: {gard_curie: label}, written as a GARD labels file.
    :param vocabularies: ordered [(name, {curie: (label, biolink_type)})]; each becomes one ids file
        and one labels file, in that priority order.
    :param other_concords: iterable of iterables of (subject, predicate, object) rows.
    :param extra_ids: extra (curie, biolink_type) pairs, so the guard-3 glom can see identifiers
        that are in a clique but not in the match pool (an HP term, typically).
    """
    gard_labels = tmp_path / "gard_labels"
    gard_labels.write_text("".join(f"{curie}\t{label}\n" for curie, label in gard.items()))

    match_ids, match_labels = [], []
    for name, members in vocabularies:
        idsfile = tmp_path / f"ids_{name}"
        labelsfile = tmp_path / f"labels_{name}"
        idsfile.write_text("".join(f"{curie}\t{biotype}\n" for curie, (_, biotype) in members.items()))
        labelsfile.write_text("".join(f"{curie}\t{label}\n" for curie, (label, _) in members.items()))
        match_ids.append(str(idsfile))
        match_labels.append(str(labelsfile))

    # The guard-3 glom needs every ids file the build has, or its cliques carry no declared types.
    gard_ids = tmp_path / "ids_GARD"
    gard_ids.write_text("".join(f"{curie}\t{DISEASE}\n" for curie in gard))
    other_ids = tmp_path / "ids_extra"
    other_ids.write_text("".join(f"{curie}\t{biotype}\n" for curie, biotype in extra_ids))

    concord_paths = []
    for i, rows in enumerate(other_concords):
        concord = tmp_path / f"concord_{i}"
        concord.write_text("".join("\t".join(row) + "\n" for row in rows))
        concord_paths.append(str(concord))

    mondoclose = tmp_path / "MONDO_close"
    mondoclose.write_text("")
    outfile = tmp_path / "GARD_LABEL"
    diseasephenotype.build_gard_label_concord(
        str(gard_labels),
        match_ids,
        match_labels,
        match_ids + [str(gard_ids), str(other_ids)],
        concord_paths,
        str(mondoclose),
        {},
        str(outfile),
        str(tmp_path / "metadata.yaml"),
    )
    return [tuple(line.split("\t")) for line in outfile.read_text().splitlines()]


@pytest.mark.unit
def test_gard_label_concord_matches_case_insensitively(tmp_path):
    """ "Genu Varum" and "Genu varum" name one disease. Case-sensitive matching is not merely
    stricter, it is measurably *worse* (2.64% wrong against 0.21%): it walks past the right clique on
    a capital letter and then matches some other clique that is capitalized the same way."""
    rows = _run_gard_label_concord(
        tmp_path,
        gard={"GARD:1": "Odontogenic Carcinoma"},
        vocabularies=[("NCIT", {"NCIT:C173720": ("odontogenic carcinoma", DISEASE)})],
    )
    assert rows == [("GARD:1", "xref", "NCIT:C173720")]


@pytest.mark.unit
def test_gard_label_concord_walks_the_pool_in_priority_order(tmp_path):
    """The first vocabulary with a match wins, and matching stops there -- the priority order of
    config.yaml: disease_gard_label_match_prefixes is the whole tie-break."""
    rows = _run_gard_label_concord(
        tmp_path,
        gard={"GARD:1": "Nephroblastoma"},
        vocabularies=[
            ("MONDO", {"MONDO:0006058": ("Nephroblastoma", DISEASE)}),
            ("NCIT", {"NCIT:C3267": ("nephroblastoma", DISEASE)}),
        ],
    )
    assert rows == [("GARD:1", "xref", "MONDO:0006058")], "MONDO precedes NCIT, and only one row is written"


@pytest.mark.unit
def test_gard_label_concord_skips_a_label_two_identifiers_of_one_vocabulary_share(tmp_path):
    """If the vocabulary itself does not say which of its two concepts the registry means, neither
    can we -- and the term is skipped outright rather than falling through to the next vocabulary,
    because an ambiguity in the most-trusted vocabulary is evidence about the label, not about
    MONDO."""
    rows = _run_gard_label_concord(
        tmp_path,
        gard={"GARD:1": "Erythropoietic protoporphyria"},
        vocabularies=[
            (
                "MONDO",
                {
                    "MONDO:0001676": ("Erythropoietic protoporphyria", DISEASE),
                    "MONDO:0008319": ("erythropoietic protoporphyria", DISEASE),
                },
            ),
            ("NCIT", {"NCIT:C1": ("Erythropoietic protoporphyria", DISEASE)}),
        ],
    )
    assert rows == []


@pytest.mark.unit
def test_gard_label_concord_leaves_gard_ids_another_concord_places(tmp_path):
    """Guard 1. A GARD id another concord names already sits in a curated clique; re-deciding it by
    label is what turns a 0.21% error rate into 33 attempted merges of curated MONDO cliques. The
    check reads the concord files rather than a hardcoded (MONDO_GARD, DOID) pair, so a source that
    starts emitting GARD xrefs is covered without touching this code."""
    rows = _run_gard_label_concord(
        tmp_path,
        gard={"GARD:1": "Kartagener syndrome", "GARD:2": "Odontogenic Carcinoma"},
        vocabularies=[
            ("NCIT", {"NCIT:C1": ("Kartagener syndrome", DISEASE), "NCIT:C2": ("Odontogenic Carcinoma", DISEASE)})
        ],
        other_concords=[[("MONDO:0009484", "xref", "GARD:1")]],
    )
    assert rows == [("GARD:2", "xref", "NCIT:C2")], "GARD:1 is claimed by another concord and must be left alone"


@pytest.mark.unit
def test_gard_label_concord_cannot_merge_two_pre_existing_cliques(tmp_path):
    """Guard 2, end to end and this is the invariant the whole design rests on.

    Guard 1 leaves only GARD ids that appear in no other concord, so each is a single-identifier
    clique; emitting at most one row each means a GARD_LABEL pair can only union {GARD:x} into one
    existing clique. Even when a GARD term's label is carried by identifiers in two different
    cliques, the two must stay separate. Emitting every matching identifier instead would have put
    475 existing clique pairs at risk of fusion, which is why this is enforced by construction
    rather than detected by a warning downstream (AGENTS.md, "A log warning is not a control")."""
    rows = _run_gard_label_concord(
        tmp_path,
        gard={"GARD:1": "Torticollis"},
        vocabularies=[
            ("MESH", {"MESH:D014103": ("Torticollis", DISEASE)}),
            ("NCIT", {"NCIT:C1": ("torticollis", DISEASE)}),
        ],
        other_concords=[[("MESH:D014103", "xref", "UMLS:C1"), ("NCIT:C1", "xref", "UMLS:C2")]],
        extra_ids=[("UMLS:C1", DISEASE), ("UMLS:C2", DISEASE)],
    )
    assert rows == [("GARD:1", "xref", "MESH:D014103")]

    ids = {}
    for name in ("ids_MESH", "ids_NCIT", "ids_GARD", "ids_extra"):
        ids[name] = str(tmp_path / name)
    dicts, _ = diseasephenotype.compute_cliques_for_impact_report(
        [str(tmp_path / "concord_0"), str(tmp_path / "GARD_LABEL")],
        list(ids.values()),
        mondoclose=str(tmp_path / "MONDO_close"),
        badxrefs={},
    )
    assert dicts["MESH:D014103"] == {"MESH:D014103", "UMLS:C1", "GARD:1"}
    assert dicts["NCIT:C1"] == {"NCIT:C1", "UMLS:C2"}, "the two cliques the shared label spans must not fuse"


@pytest.mark.unit
def test_gard_label_concord_skips_a_target_whose_clique_is_not_a_disease(tmp_path):
    """Guard 3. write_compendium()'s prefix filter keeps GARD alive in Disease.txt only
    (disease_extra_prefixes is a per-class allowlist, and Biolink registers GARD for no class at
    all), so a GARD id that joins a phenotype clique is not moved to PhenotypicFeature.txt -- it is
    dropped from the build entirely, trading a working single-identifier clique for a vanished
    identifier. Five registry terms are in that position today; a clique diff of the first
    implementation is how they were found.

    The clique that makes them phenotypes forms through UMLS, two hops from the target, so no label
    or ids-file inspection sees it -- hence the reglom. Remove this guard when GARD is registered in
    the Biolink Model (https://github.com/NCATSTranslator/Babel/issues/1051)."""
    rows = _run_gard_label_concord(
        tmp_path,
        gard={"GARD:1": "Myokymia", "GARD:2": "Odontogenic Carcinoma"},
        vocabularies=[("MESH", {"MESH:D020385": ("Myokymia", DISEASE), "MESH:D2": ("Odontogenic Carcinoma", DISEASE)})],
        # MESH:D020385 reaches HP:0002411 through UMLS, exactly as the real Myokymia clique does.
        other_concords=[[("MESH:D020385", "xref", "UMLS:C1"), ("UMLS:C1", "xref", "HP:0002411")]],
        extra_ids=[("UMLS:C1", DISEASE), ("HP:0002411", PHENOTYPIC_FEATURE)],
    )
    assert rows == [("GARD:2", "xref", "MESH:D2")], "the HP-led clique must not gain a GARD identifier"


@pytest.mark.unit
def test_gard_label_concord_is_registered_last_in_disease_concords():
    """GARD_LABEL must be in config.yaml: disease_concords, and must be LAST.

    It is the only concord in this pipeline derived from labels rather than an asserted mapping, so
    it should decide nothing another concord has an opinion about. More concretely, guard 1 -- skip
    any GARD id another concord names -- is *defined* by the rest of this list, and the Snakemake
    rule builds that list by excluding GARD_LABEL from it. Dropping the entry silently reverts 265
    rare diseases to single-identifier cliques with no error anywhere, the same failure mode
    test_mondo_gard_concord_is_registered_in_disease_concords guards."""
    concords = get_config()["disease_concords"]
    assert "GARD_LABEL" in concords
    assert concords[-1] == "GARD_LABEL", f"GARD_LABEL must be glommed last, but disease_concords is {concords}"


@pytest.mark.unit
def test_gard_label_match_prefixes_are_buildable_and_exclude_the_phenotype_ontologies():
    """Every disease_gard_label_match_prefixes entry names both an ids file (disease_ids) and a
    labels file (disease_labelsandsynonyms); the Snakemake rule expands the same list over both
    directories, so an entry in only one of them fails the build with a missing-input error rather
    than anything readable.

    HP and MP must stay out. split_mutually_exclusive_cliques() keeps phenotype and disease cliques
    disjoint on purpose and disease_gard_ids types every registry term biolink:Disease, so matching
    a GARD term onto an HP or MP term asserts an identity this pipeline is built to refuse. (Guard 3
    covers the indirect case, where the target reaches HP through some other clique member.)"""
    config = get_config()
    pool = config["disease_gard_label_match_prefixes"]

    assert set(pool) <= set(config["disease_ids"]), (
        f"no disease ids file for: {sorted(set(pool) - set(config['disease_ids']))}"
    )
    assert set(pool) <= set(config["disease_labelsandsynonyms"]), (
        f"no labels file for: {sorted(set(pool) - set(config['disease_labelsandsynonyms']))}"
    )
    assert "HP" not in pool and "MP" not in pool, "the phenotype ontologies must not be label-match targets"
    assert GARD not in pool, "GARD cannot match against itself"


@pytest.mark.unit
def test_gard_label_concord_rejects_mismatched_pool_lists(tmp_path):
    """The ids and labels lists are positional and must stay in the same priority order; a length
    mismatch means the Snakemake rule's two expand() calls have drifted, and zip() would silently
    match NCIT ids against UMLS labels."""
    with pytest.raises(ValueError, match="parallel lists"):
        diseasephenotype.build_gard_label_concord(
            str(tmp_path / "gard_labels"),
            ["ids_a", "ids_b"],
            ["labels_a"],
            [],
            [],
            None,
            {},
            str(tmp_path / "out"),
            str(tmp_path / "metadata.yaml"),
        )

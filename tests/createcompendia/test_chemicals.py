"""Unit tests for src.createcompendia.chemicals.

Covers write_unichem_concords()'s handling of UniChem compound IDs that already embed their source
prefix (e.g. the CHEBI source stores "CHEBI:12345" rather than a bare "12345"), which previously
produced invalid "CHEBI:CHEBI:12345" CURIEs across the chemical compendia; and make_chebi_relations()'s
reading of the ChEBI SDF, whose tags ChEBI renames between releases.
"""

import gzip
import json
from pathlib import Path

import pytest

from src import categories
from src.categories import (
    CHEMICAL_ENTITY,
    CHEMICAL_MIXTURE,
    COMPLEX_MOLECULAR_MIXTURE,
    DRUG,
    FOOD,
    MOLECULAR_MIXTURE,
    POLYPEPTIDE,
    SMALL_MOLECULE,
)
from src.createcompendia.chemicals import (
    CHEBI_DBX_SOURCE_NAMES,
    create_typed_sets,
    filter_pairs_to_known_ids,
    make_chebi_relations,
    read_chebi_lookup_ids,
    split_chebi_sdf_values,
    write_unichem_concords,
)
from src.datahandlers.unichem import UNICHEM_REFERENCE_TSV_HEADER, UNICHEM_STRUCT_TSV_HEADER
from src.datahandlers.unichem import data_sources as unichem_data_sources
from src.predicates import HAS_ALTERNATIVE_ID
from src.prefixes import CHEBI, KEGGCOMPOUND, PUBCHEMCOMPOUND
from src.util import get_config

# Derive CHEBI's UniChem source ID from the authoritative dict rather than hardcoding it.
CHEBI_SRC_ID = next(k for k, v in unichem_data_sources.items() if v == CHEBI)


def _bare_rows_for_other_sources():
    """One bare-ID row per non-CHEBI source.

    write_unichem_concords() raises if any configured source produces no entries,
    so a focused test still has to feed every source at least one row.
    """
    return [("1", src_id, "100") for src_id in unichem_data_sources if src_id != CHEBI_SRC_ID]


def _write_struct(path):
    """Write a minimal gzipped UniChem structure file with one UCI→InChIKey row."""
    with gzip.open(path, "wt") as out:
        out.write(UNICHEM_STRUCT_TSV_HEADER)
        out.write("1\tInChI=1S/H2O/h1H2\tXLYOFNOQVPJJNP-UHFFFAOYSA-N\n")


def _write_ref(path, rows):
    """Write a UniChem reference file; rows are (uci, src_id, compound_id) tuples (assignment is always '1')."""
    with open(path, "w") as out:
        out.write(UNICHEM_REFERENCE_TSV_HEADER)
        for uci, src_id, compound_id in rows:
            out.write(f"{uci}\t{src_id}\t{compound_id}\t1\n")


@pytest.mark.unit
def test_write_unichem_concords_strips_embedded_chebi_prefix(tmp_path):
    """A CHEBI compound ID stored as 'CHEBI:12345' must yield CHEBI:12345, not CHEBI:CHEBI:12345."""
    struct = tmp_path / "structure.tsv.gz"
    ref = tmp_path / "reference.tsv"
    _write_struct(struct)
    _write_ref(ref, [("1", CHEBI_SRC_ID, "CHEBI:12345"), *_bare_rows_for_other_sources()])

    write_unichem_concords(str(struct), str(ref), str(tmp_path))

    content = (tmp_path / f"UNICHEM_{CHEBI}").read_text()
    assert "CHEBI:12345\t" in content
    assert "CHEBI:CHEBI" not in content


@pytest.mark.unit
def test_write_unichem_concords_raises_on_unexpected_embedded_prefix(tmp_path):
    """A CHEBI row carrying a foreign embedded prefix is a format change worth a loud failure."""
    struct = tmp_path / "structure.tsv.gz"
    ref = tmp_path / "reference.tsv"
    _write_struct(struct)
    _write_ref(ref, [("1", CHEBI_SRC_ID, "FOO:9")])

    with pytest.raises(ValueError, match="unexpected embedded prefix"):
        write_unichem_concords(str(struct), str(ref), str(tmp_path))


@pytest.mark.unit
def test_write_unichem_concords_raises_when_source_produces_no_entries(tmp_path):
    """Any configured source that contributes zero rows should raise RuntimeError."""
    struct = tmp_path / "structure.tsv.gz"
    ref = tmp_path / "reference.tsv"
    _write_struct(struct)
    # Feed rows for every source except CHEBI — CHEBI should trigger the empty-source guard.
    rows = [("1", src_id, "100") for src_id in unichem_data_sources if src_id != CHEBI_SRC_ID]
    _write_ref(ref, rows)

    with pytest.raises(RuntimeError, match="no entries for the following sources"):
        write_unichem_concords(str(struct), str(ref), str(tmp_path))


# ----
# FOOD-AND-EXTRACT TYPE VOTE (issues #828, #935)
# ----


@pytest.mark.unit
def test_create_typed_sets_types_a_structureless_food_clique_as_food():
    """A clique whose only evidence is a DRUGBANK food CURIE, and whose members are all
    biolink:ChemicalEntity, should be typed biolink:Food and keep every member (incl. RXCUI).
    This is the #828/#918 behaviour that must survive the change to a vote."""
    trout = frozenset({"DRUGBANK:DB10626", "UMLS:C2725895", "RXCUI:882482"})
    # ChemicalEntity is what these cliques vote today; Food outranks it, so the evidence wins.
    types = {"UMLS:C2725895": CHEMICAL_ENTITY}

    typed = create_typed_sets({trout}, types, food_types={"DRUGBANK:DB10626": FOOD})

    assert trout in typed[FOOD]
    assert all(trout not in sets for t, sets in typed.items() if t != FOOD)


@pytest.mark.unit
def test_create_typed_sets_does_not_demote_a_small_molecule_to_food():
    """Food evidence must NOT retype a clique that votes for a structure-bearing type (issue #935).

    This is the D-glucose clique from the babel-1.18 build, which the old clique-level override
    typed biolink:Food. DRUGBANK:DB09341 "Dextrose, unspecified form" is a structureless DrugBank
    food row that gloms into the real D-glucose clique via its UMLS/RxNorm concords; the clique
    votes SmallMolecule, which outranks Food, so it must stay a SmallMolecule -- with the food
    CURIE still a member.
    """
    glucose = frozenset(
        {
            "CHEBI:17234",
            "PUBCHEM.COMPOUND:107526",
            "DRUGBANK:DB01914",
            "DRUGBANK:DB09341",
            "MESH:D005947",
            "UMLS:C0017725",
            "RXCUI:4850",
        }
    )
    types = {
        "PUBCHEM.COMPOUND:107526": SMALL_MOLECULE,
        "CHEBI:17234": CHEMICAL_ENTITY,
        "DRUGBANK:DB01914": CHEMICAL_ENTITY,
        "MESH:D005947": CHEMICAL_ENTITY,
    }

    typed = create_typed_sets({glucose}, types, food_types={"DRUGBANK:DB09341": FOOD})

    assert glucose in typed[SMALL_MOLECULE]
    assert glucose not in typed[FOOD]


@pytest.mark.unit
def test_create_typed_sets_types_an_extract_as_a_complex_molecular_mixture():
    """A DRUGBANK allergen extract with ComplexMolecularMixture evidence lands there, not in Food:
    ComplexMolecularMixture outranks Food, so an extract stays an extract."""
    pollen = frozenset({"DRUGBANK:DB10351", "UMLS:C2684343"})

    typed = create_typed_sets({pollen}, {}, food_types={"DRUGBANK:DB10351": COMPLEX_MOLECULAR_MIXTURE})

    assert pollen in typed[COMPLEX_MOLECULAR_MIXTURE]
    assert pollen not in typed[FOOD]


@pytest.mark.unit
def test_a_split_cliques_halves_vote_on_their_own_food_evidence():
    """When a clique is split into a MolecularMixture and a SmallMolecule half (issue #83), each half
    must be typed by the evidence *it* holds, not by the whole pre-split clique's.

    ComplexMolecularMixture outranks MolecularMixture (but not SmallMolecule), so an extract CURIE
    that lands in the small molecule half — where it correctly loses the vote — would otherwise
    still retype the *mixture* half it isn't even a member of.
    """
    clique = frozenset({"PUBCHEM.COMPOUND:962", "PUBCHEM.COMPOUND:22247451", "DRUGBANK:DB10351"})
    types = {
        "PUBCHEM.COMPOUND:962": SMALL_MOLECULE,
        "PUBCHEM.COMPOUND:22247451": MOLECULAR_MIXTURE,
    }

    typed = create_typed_sets({clique}, types, food_types={"DRUGBANK:DB10351": COMPLEX_MOLECULAR_MIXTURE})

    # The extract CURIE is in the small-molecule half, so the mixture half never sees the evidence.
    assert frozenset({"PUBCHEM.COMPOUND:22247451"}) in typed[MOLECULAR_MIXTURE]
    # ...and in its own half SmallMolecule outranks it, so it loses there too.
    assert frozenset({"PUBCHEM.COMPOUND:962", "DRUGBANK:DB10351"}) in typed[SMALL_MOLECULE]
    assert not typed[COMPLEX_MOLECULAR_MIXTURE]


@pytest.mark.unit
def test_food_evidence_beats_a_drug_vote():
    """PINS KNOWN-IMPERFECT BEHAVIOUR (issue #935). chemical_type_order ranks biolink:Drug last, below
    biolink:Food, so a clique that votes Drug and also carries food evidence is typed Food. That is
    mildly wrong -- a drug formulation is not a food -- but it is accepted rather than special-cased:
    it does not occur in any build so far (no clique carrying food evidence holds a Drug member), and
    Drug is last for good reason (see config.yaml: chemical_type_order).

    INVERT this assertion, don't delete it, if the tradeoff is ever revisited -- i.e. if Food starts
    appearing where a drug formulation belongs and Drug is promoted above Food.
    """
    formulation = frozenset({"DRUGBANK:DB09341", "RXCUI:4850"})
    types = {"RXCUI:4850": DRUG}

    typed = create_typed_sets({formulation}, types, food_types={"DRUGBANK:DB09341": FOOD})

    assert formulation in typed[FOOD]
    assert formulation not in typed[DRUG]


@pytest.mark.unit
def test_chemical_type_order_is_well_formed():
    """Every entry in config.yaml's chemical_type_order should be a known src/categories.py constant,
    with no duplicates. create_typed_sets() calls order.index() on every type it sees, so a typo or a
    missing entry is a ValueError tens of millions of cliques into a build."""
    order = get_config()["chemical_type_order"]
    known = {value for name, value in vars(categories).items() if name.isupper() and isinstance(value, str)}

    assert len(order) == len(set(order)), "chemical_type_order contains duplicates"
    assert set(order) <= known, f"unknown Biolink types in chemical_type_order: {set(order) - known}"


@pytest.mark.unit
def test_chemical_type_order_ranks_food_below_structure_bearing_types():
    """Food must rank below every structure-bearing type and above ChemicalEntity (issue #935).

    This is the property that keeps food evidence from demoting a defined molecule, and it is what
    the babel-1.18 D-glucose bug came down to. ComplexMolecularMixture must also outrank Food so an
    extract stays an extract when NCIt also calls the concept a food, and ChemicalMixture likewise:
    a mixture asserts a composition that a whole food does not."""
    order = get_config()["chemical_type_order"]

    for structural in (
        SMALL_MOLECULE,
        MOLECULAR_MIXTURE,
        POLYPEPTIDE,
        COMPLEX_MOLECULAR_MIXTURE,
        CHEMICAL_MIXTURE,
    ):
        assert order.index(structural) < order.index(FOOD), f"{structural} must outrank {FOOD}"
    assert order.index(FOOD) < order.index(CHEMICAL_ENTITY), f"{FOOD} must outrank {CHEMICAL_ENTITY}"


@pytest.mark.unit
def test_create_typed_sets_leaves_a_clique_without_food_evidence_untouched():
    """A clique holding none of the food/extract CURIEs should keep its normal voted type
    (no food_types leakage across cliques)."""
    normal = frozenset({"CHEBI:15377", "PUBCHEM.COMPOUND:962"})
    types = {"CHEBI:15377": SMALL_MOLECULE, "PUBCHEM.COMPOUND:962": SMALL_MOLECULE}

    typed = create_typed_sets({normal}, types, food_types={"DRUGBANK:DB10626": FOOD})

    assert normal in typed[SMALL_MOLECULE]
    assert normal not in typed[FOOD]


# MAKE_CHEBI_RELATIONS / CHEBI SDF TAG NAMES

# tests/data/chebi_abacavir.sdf is the CHEBI:421707 "abacavir" entry copied verbatim out of
# babel_downloads/CHEBI/ChEBI_complete.sdf as downloaded for babel-1.18. It is the single record
# that motivated this fix, and it happens to carry every tag make_chebi_relations() reads, so one
# entry exercises all of CHEBI_SDF_KEYS. Re-derive it by extracting the chunk whose "> <ChEBI ID>"
# tag holds CHEBI:421707.
ABACAVIR_SDF = Path(__file__).parent.parent / "data" / "chebi_abacavir.sdf"

# The header of database_accession.tsv, copied verbatim from
# https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/database_accession.tsv.gz (fetched 2026-07-21).
# Passing it alone gives a dbx with no usable rows, which make_chebi_relations() now rejects -- so
# every test that isn't specifically about that rejection supplies at least one row as well.
DBX_HEADER = "id\tcompound_id\taccession_number\ttype\tstatus_id\tsource_id\n"

# The first KEGG COMPOUND row of that same file, verbatim: CHEBI:3 -> KEGG.COMPOUND:C06147.
DBX_KEGG_ROW = "9\t3\tC06147\tMANUAL_X_REF\t3\t45\n"

# A PubChem Compound row, verbatim: CHEBI:132338 -> PUBCHEM.COMPOUND:101936044.
DBX_PUBCHEM_ROW = "970951\t132338\t101936044\tMANUAL_X_REF\t1\t68\n"

# A CAS row attributed to source_id 45 (KEGG COMPOUND), verbatim. CAS is a shared namespace, so this
# is a CAS registry number ChEBI got *from* KEGG COMPOUND, not a KEGG accession;
# reading source_id at face value would emit "KEGG.COMPOUND:498-15-7". 10,476 real rows look like this.
DBX_CAS_ROW_UNDER_KEGG_SOURCE = "17\t7\t498-15-7\tCAS\t1\t45\n"

# source.tsv, subset to the rows these tests need. Header and the two source rows are verbatim from
# https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/source.tsv.gz (fetched 2026-07-21); the
# trailing description column of the PubChem row is elided for width, which the parser ignores.
SOURCE_TSV = (
    "id\tname\turl\tprefix\tdescription\n"
    "45\tKEGG COMPOUND\thttps://bioregistry.io/kegg.compound:*\tkegg.compound\t\n"
    "68\tPubChem Compound\thttps://bioregistry.io/pubchem.compound:*\tpubchem.compound\t\n"
)

# status.tsv in full, verbatim from the same directory (fetched 2026-07-21) -- it really is this
# short. DBX_KEGG_ROW is status 3 (OK) and DBX_PUBCHEM_ROW is status 1 (CHECKED); 9 (SUBMITTED) is
# the one the ingest drops.
STATUS_TSV = "id\tname\n1\tCHECKED\n3\tOK\n9\tSUBMITTED\n"

# A KEGG row identical in shape to DBX_KEGG_ROW but SUBMITTED (status 9): CHEBI:1690 -> C05478.
# Verbatim from database_accession.tsv; 793 KEGG rows look like this and none are ingested.
DBX_SUBMITTED_KEGG_ROW = "1071122\t1690\tC05478\tMANUAL_X_REF\t9\t45\n"


def _run_make_chebi_relations(
    tmp_path,
    sdf=ABACAVIR_SDF,
    dbx_contents=DBX_HEADER + DBX_KEGG_ROW,
    source_contents=SOURCE_TSV,
    status_contents=STATUS_TSV,
):
    """Run make_chebi_relations() over a fixture SDF, returning (concord lines, Property dicts)."""
    dbx = tmp_path / "database_accession.tsv"
    dbx.write_text(dbx_contents)
    dbx_source = tmp_path / "source.tsv"
    dbx_source.write_text(source_contents)
    dbx_status = tmp_path / "status.tsv"
    dbx_status.write_text(status_contents)
    concord = tmp_path / "CHEBI"
    propfile = tmp_path / "props.jsonl.gz"

    make_chebi_relations(
        str(sdf),
        str(dbx),
        str(dbx_source),
        str(dbx_status),
        str(concord),
        propfile_gz=str(propfile),
        metadata_yaml=str(tmp_path / "metadata.yaml"),
    )

    with gzip.open(propfile, "rt") as inf:
        props = [json.loads(line) for line in inf]
    return concord.read_text().splitlines(), props


@pytest.mark.unit
def test_make_chebi_relations_emits_secondary_chebi_ids(tmp_path):
    """Every ID in a SECONDARY_ID tag should become a hasAlternativeId property on the primary CURIE.

    ChEBI packs them semicolon-delimited onto one line, so a parser that treats the line as a single
    value emits one nonsense CURIE instead of five real ones. CHEBI:520984 is the secondary ID that
    stopped normalizing in babel-1.18 when this ingest broke.
    """
    _, props = _run_make_chebi_relations(tmp_path)

    secondary_ids = {p["value"] for p in props if p["predicate"] == HAS_ALTERNATIVE_ID}
    assert secondary_ids == {"CHEBI:193608", "CHEBI:441792", "CHEBI:2360", "CHEBI:525912", "CHEBI:520984"}
    assert {p["curie"] for p in props} == {"CHEBI:421707"}


@pytest.mark.unit
def test_make_chebi_relations_emits_kegg_and_pubchem_xrefs(tmp_path):
    """The SDF's KEGG COMPOUND and PubChem *Compound* links should both become concord xrefs.

    PubChem is the regression-prone one: ChEBI split a single "PubChem Database Links" tag into
    separate Compound and Substance tags, and only the Compound side belongs in the concord.
    """
    concord_lines, _ = _run_make_chebi_relations(tmp_path)

    assert f"CHEBI:421707\txref\t{KEGGCOMPOUND}:C07624" in concord_lines
    assert f"CHEBI:421707\txref\t{PUBCHEMCOMPOUND}:441300" in concord_lines
    # 85612588 is this entry's "PubChem Substance Database Links" value; a substance ID must never be
    # picked up as a compound.
    assert not any(f"{PUBCHEMCOMPOUND}:85612588" in line for line in concord_lines)


@pytest.mark.unit
def test_make_chebi_relations_splits_multivalue_tags(tmp_path):
    """A semicolon-delimited tag value should never survive into a CURIE.

    Before the fix, a multi-value KEGG line produced "KEGG.COMPOUND:C00001;C00002", which matches
    nothing downstream and was invisible because it still looked like a populated concord.
    """
    concord_lines, props = _run_make_chebi_relations(tmp_path)

    assert not any(";" in line for line in concord_lines)
    assert not any(";" in p["value"] for p in props)


@pytest.mark.unit
@pytest.mark.parametrize(
    "renamed_tag,expected_key",
    [
        # The rename that actually happened: `Secondary ChEBI ID` became `SECONDARY_ID` in
        # babel-1.18, every secondary identifier vanished from the release, and nothing complained.
        ("> <SECONDARY_ID>", "secondary_id"),
        ("> <KEGG COMPOUND Database Links>", "keggcompounddatabaselinks"),
        ("> <PubChem Compound Database Links>", "pubchemcompounddatabaselinks"),
        # The canary keys. make_chebi_relations() reads none of these, but a rename landing on one
        # means ChEBI has reworked the file and the tags we do consume need re-auditing, so the
        # check must fail the build for them too.
        ("> <ChEBI NAME>", "chebiname"),
        ("> <INCHIKEY>", "inchikey"),
        ("> <SMILES>", "smiles"),
    ],
)
def test_make_chebi_relations_raises_when_chebi_renames_a_tag(tmp_path, renamed_tag, expected_key):
    """A renamed SDF tag should fail the build, naming the tag, rather than silently emitting nothing.

    This is the check that babel-1.18 lacked. Canary tags are covered as well as consumed ones: a
    rename we tolerate is a rename nobody re-audits.
    """
    renamed = tmp_path / "renamed.sdf"
    renamed.write_text(ABACAVIR_SDF.read_text().replace(renamed_tag, "> <Renamed By ChEBI>"))

    with pytest.raises(ValueError, match=expected_key):
        _run_make_chebi_relations(tmp_path, sdf=renamed)


@pytest.mark.unit
@pytest.mark.parametrize(
    "emptied_value,expected_key",
    [
        ("CHEBI:193608;CHEBI:441792;CHEBI:2360;CHEBI:525912;CHEBI:520984", "secondary_id"),
        ("> <KEGG COMPOUND Database Links>\nC07624", "keggcompounddatabaselinks"),
        ("> <PubChem Compound Database Links>\n441300", "pubchemcompounddatabaselinks"),
    ],
)
def test_make_chebi_relations_raises_when_one_tag_yields_nothing(tmp_path, emptied_value, expected_key):
    """A tag that is present but yields no values should fail the build, naming that tag.

    Each consumed tag is counted separately rather than in aggregate, because a total-row check
    cannot protect an individual input: PubChem's ~181,000 xrefs could vanish entirely and KEGG's
    ~16,000 would keep it quiet. That is the exact shape of the bug this ingest shipped.

    A tag-name check can't see this at all — the tag is still there, only its values changed shape,
    were truncated, or stopped parsing.
    """
    emptied = tmp_path / "emptied.sdf"
    # Keep the tag line, blank the value, so check_chebi_sdf_keys() passes and this check is what
    # fires. The replaced text starts with the tag line for the two xref cases, so re-emit it.
    replacement = emptied_value.split("\n")[0] + "\n " if emptied_value.startswith("> <") else " "
    emptied.write_text(ABACAVIR_SDF.read_text().replace(emptied_value, replacement))

    with pytest.raises(ValueError, match=expected_key):
        _run_make_chebi_relations(tmp_path, sdf=emptied)


@pytest.mark.unit
def test_split_chebi_sdf_values_joins_values_across_lines():
    """read_sdf() returns a tag's value as a list of lines, so splitting must cover both axes: several
    values on one line and several lines under one tag.

    No tag make_chebi_relations() reads spans more than one line in the babel-1.18 SDF (only
    DEFINITION does, in 5 entries), but the code this replaced concatenated multiple lines, so the
    behaviour is asserted rather than assumed away.
    """
    assert split_chebi_sdf_values(["C00001;C00002", "  C00003  ", "", "  ", "C00004;"]) == [
        "C00001",
        "C00002",
        "C00003",
        "C00004",
    ]


@pytest.mark.unit
def test_make_chebi_relations_emits_database_accession_xrefs(tmp_path):
    """A database_accession.tsv row should become a concord xref, with its prefix resolved through
    source.tsv and its accession read from the accession_number column.

    This is the inverted form of the assertion that pinned issue #954: the branch used to match
    column 3 against "KEGG COMPOUND accession", but that column is `type`, and it read the accession
    from `status_id`. It fired on 0 of 422,561 rows.
    """
    concord_lines, _ = _run_make_chebi_relations(tmp_path, dbx_contents=DBX_HEADER + DBX_KEGG_ROW + DBX_PUBCHEM_ROW)

    assert f"CHEBI:3\txref\t{KEGGCOMPOUND}:C06147" in concord_lines
    assert f"CHEBI:132338\txref\t{PUBCHEMCOMPOUND}:101936044" in concord_lines


@pytest.mark.unit
def test_make_chebi_relations_ignores_non_accession_rows_of_a_wanted_source(tmp_path):
    """A CAS-typed row attributed to the KEGG COMPOUND source must not become a KEGG xref.

    A CAS registry number is a shared identifier many databases redistribute: this exact value also
    arrives under ChemIDplus and NIST. So on a CAS row source_id records who supplied the number, not
    the namespace it belongs to -- unlike MANUAL_X_REF/CITATION/REGISTRY_NUMBER rows, where the
    source is the namespace. 10,476 rows in the real file are CAS numbers attributed to source_id 45,
    so reading source_id at face value would emit "KEGG.COMPOUND:498-15-7".
    """
    concord_lines, _ = _run_make_chebi_relations(
        tmp_path, dbx_contents=DBX_HEADER + DBX_KEGG_ROW + DBX_CAS_ROW_UNDER_KEGG_SOURCE
    )

    assert f"CHEBI:3\txref\t{KEGGCOMPOUND}:C06147" in concord_lines
    assert not any("498-15-7" in line for line in concord_lines)


@pytest.mark.unit
def test_make_chebi_relations_raises_when_chebi_renames_a_dbx_source(tmp_path):
    """A source.tsv that no longer lists an expected source name should fail the build, naming it.

    Resolving source_id by name is what makes a renumbering safe; it is only safe if a *rename* is
    loud, otherwise we have swapped one silent-empty failure for another.
    """
    renamed = SOURCE_TSV.replace("KEGG COMPOUND", "KEGG Compound")

    with pytest.raises(ValueError, match="KEGG COMPOUND"):
        _run_make_chebi_relations(tmp_path, source_contents=renamed)


@pytest.mark.unit
def test_make_chebi_relations_skips_dbx_rows_for_structured_chebis(tmp_path):
    """A dbx row for a CHEBI already in the SDF should be skipped, since the SDF is authoritative for
    those. CHEBI:421707 is the fixture SDF's entry, so its dbx row must not be written twice."""
    duplicate = "1\t421707\tC07624\tMANUAL_X_REF\t1\t45\n"

    # DBX_KEGG_ROW rides along so the dbx still contributes something; a dbx whose every row is
    # skipped trips the empty-input guard, which is the subject of the test below rather than this one.
    concord_lines, _ = _run_make_chebi_relations(tmp_path, dbx_contents=DBX_HEADER + duplicate + DBX_KEGG_ROW)

    assert concord_lines.count(f"CHEBI:421707\txref\t{KEGGCOMPOUND}:C07624") == 1


@pytest.mark.unit
def test_make_chebi_relations_ignores_submitted_dbx_rows(tmp_path):
    """A SUBMITTED row is a depositor's unreviewed claim and must not become an xref, while its
    CHECKED/OK siblings do.

    These feed glom() as equivalences, so an unreviewed one merges cliques on a claim nobody
    checked. 793 KEGG and 30 of the 55 PubChem rows are SUBMITTED, so excluding them costs little.
    Revisit if issue #957 establishes that SUBMITTED is verified some other way.
    """
    concord_lines, _ = _run_make_chebi_relations(
        tmp_path, dbx_contents=DBX_HEADER + DBX_KEGG_ROW + DBX_SUBMITTED_KEGG_ROW
    )

    assert f"CHEBI:3\txref\t{KEGGCOMPOUND}:C06147" in concord_lines
    assert not any("C05478" in line for line in concord_lines)


@pytest.mark.unit
def test_make_chebi_relations_reads_gzipped_lookup_tables(tmp_path):
    """source.tsv/status.tsv should work gzipped as well as plain.

    ChEBI publishes them as .tsv.gz and the pipeline stores the decompressed copies, so both forms
    are in circulation -- docs/sources/CHEBI/README.md's audit invocation points straight at the
    downloads. Reading gzip bytes as UTF-8 fails far from the cause.
    """
    gz_source = tmp_path / "source.tsv.gz"
    with gzip.open(gz_source, "wt") as out:
        out.write(SOURCE_TSV)

    assert read_chebi_lookup_ids(str(gz_source), set(CHEBI_DBX_SOURCE_NAMES)) == {
        "45": "KEGG COMPOUND",
        "68": "PubChem Compound",
    }


@pytest.mark.unit
def test_make_chebi_relations_raises_when_a_lookup_table_is_reshaped(tmp_path):
    """A lookup table whose first two columns are no longer id/name should fail, naming the file.

    source.tsv and status.tsv decide which rows are accepted, so a column reshuffle there would
    silently change the ingest -- the same class of failure as #954 itself, one file further out.
    """
    with pytest.raises(ValueError, match="Unexpected columns"):
        _run_make_chebi_relations(tmp_path, source_contents="source_id\tsource_name\n45\tKEGG COMPOUND\n")


@pytest.mark.unit
def test_make_chebi_relations_raises_when_the_dbx_contributes_nothing(tmp_path):
    """A database_accession.tsv that yields no xrefs at all should fail the build, naming that file.

    This is the check that would have caught issue #954 the release it appeared. The SDF's ~197,000
    xrefs meant any whole-output guard stayed satisfied while this input silently contributed zero
    rows for an entire release, so it is counted and checked on its own.
    """
    with pytest.raises(ValueError, match="database_accession.tsv"):
        _run_make_chebi_relations(tmp_path, dbx_contents=DBX_HEADER)


# CONCORD PAIRS OUTSIDE THE ID LISTS


@pytest.mark.unit
def test_filter_pairs_to_known_ids():
    """Pairs whose CHEBI/MESH/UMLS member is not in the id lists should be dropped and counted by that prefix."""
    pairs = [
        ["MESH:D000928", "CHEBI:35469"],  # Antidepressive Agents <-> antidepressant: both roles, neither listed
        ["MESH:D014867", "CHEBI:15377"],  # water: both listed
        ["PUBCHEM.COMPOUND:962", "MESH:D014867"],  # PUBCHEM is not an enforced prefix
        ["UMLS:C0003289", "MESH:D014867"],  # UMLS side not listed
    ]
    known = {"MESH:D014867", "CHEBI:15377"}
    kept, dropped = filter_pairs_to_known_ids(pairs, known, {"CHEBI", "MESH", "UMLS"})
    assert kept == [["MESH:D014867", "CHEBI:15377"], ["PUBCHEM.COMPOUND:962", "MESH:D014867"]]
    assert dropped == {"MESH": 1, "UMLS": 1}

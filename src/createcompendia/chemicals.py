import ast
import gzip
import logging
from collections import Counter, defaultdict

import jsonlines
import requests

import src.datahandlers.mesh as mesh
import src.datahandlers.umls as umls
from src.babel_utils import (
    get_prefixes,
    get_user_agent,
    glom,
    read_identifier_file,
    remove_overused_xrefs,
    write_compendium,
)
from src.categories import (
    CHEMICAL_ENTITY,
    CHEMICAL_MIXTURE,
    COMPLEX_MOLECULAR_MIXTURE,
    DRUG,
    MOLECULAR_MIXTURE,
    POLYPEPTIDE,
    SMALL_MOLECULE,
)
from src.datahandlers.unichem import UNICHEM_REFERENCE_TSV_HEADER, UNICHEM_STRUCT_TSV_HEADER
from src.datahandlers.unichem import data_sources as unichem_data_sources
from src.datahandlers.unii import UNII_ORGANISM_COLUMNS, read_unii_records
from src.metadata.provenance import write_combined_metadata, write_concord_metadata
from src.predicates import (
    CHEMROF_CHARGE,
    CHEMROF_FORMULA,
    CHEMROF_INCHI,
    CHEMROF_INCHI_KEY,
    CHEMROF_MASS,
    CHEMROF_MONOISOTOPIC_MASS,
    CHEMROF_SMILES,
    HAS_ROLE,
)
from src.prefixes import (
    CHEBI,
    CHEMBLCOMPOUND,
    DRUGBANK,
    DRUGCENTRAL,
    GTOPDB,
    INCHIKEY,
    KEGGCOMPOUND,
    MESH,
    PUBCHEMCOMPOUND,
    RXCUI,
    UMLS,
    UNII,
)
from src.properties import HAS_ALTERNATIVE_ID, Property
from src.sdfreader import read_sdf
from src.ubergraph import UberGraph
from src.util import (
    Text,
    ensure_parent_dir,
    get_config,
    get_logger,
    get_memory_usage_summary,
    open_maybe_gzipped,
)

logger = get_logger(__name__)

# The ChEBI SDF data-item tags make_chebi_relations() asks read_sdf() for, normalized the way
# normalize_sdf_tag() normalizes them: lowercased, spaces stripped, underscores kept.
#
# ChEBI renames these between releases and the parser silently omits a tag it does not recognize, so
# a rename empties the corresponding ingest without any error. That is how babel-1.18 shipped with
# no ChEBI secondary IDs at all: `Secondary ChEBI ID` had become `SECONDARY_ID`, and at the same time
# `PubChem Database Links` had split into separate Compound and Substance tags. Both of our keys
# stopped matching anything. `check_chebi_sdf_keys()` below now fails the build instead.
#
# Re-audit these against a new SDF with docs/sources/CHEBI/sdf_tags/audit_sdf_tags.py.
CHEBI_SDF_KEY_SECONDARY_ID = "secondary_id"
CHEBI_SDF_KEY_KEGG = "keggcompounddatabaselinks"
CHEBI_SDF_KEY_PUBCHEM = "pubchemcompounddatabaselinks"

# The SDF's structure and physical-property tags, mapped to the predicate each is written under.
# These are emitted as Property rows into the structure property file; they take no part in concord
# building or clique building.
#
# Named after ChemROF rather than ChEBI's own obo/chebi/ annotation properties because that is what
# UberGraph now publishes these values under -- see src/predicates.py and issue #1086 -- so a
# producer can later be switched from this SDF to UberGraph without changing the property file.
CHEBI_SDF_STRUCTURE_PROPERTIES = {
    "smiles": CHEMROF_SMILES,
    "inchi": CHEMROF_INCHI,
    "inchikey": CHEMROF_INCHI_KEY,
    "formula": CHEMROF_FORMULA,
    "mass": CHEMROF_MASS,
    "monoisotopic_mass": CHEMROF_MONOISOTOPIC_MASS,
    "charge": CHEMROF_CHARGE,
}

# Structure tags that are NOT covered by the per-key "produced no rows" guard below.
#
# That guard exists to catch a value-format change emptying an input silently, and it is only sound
# for a tag ChEBI publishes for nearly every entry. The other six are on 94-100% of entries in the
# 2026-06-29 SDF (181,048 to 192,445 of them), so zero rows really does mean something broke.
# CHARGE is on 15,613 -- 8% -- because most ChEBI entries are uncharged and simply omit it. Guarding
# it would be a check that can fire on legitimate data, so it is deliberately left out; a *rename*
# of the CHARGE tag is still caught by check_chebi_sdf_keys(), which is the failure that actually
# recurs here.
CHEBI_SDF_SPARSE_STRUCTURE_KEYS = frozenset({"charge"})

# chebiid is what read_sdf() keys its entries by, and chebiname is carried purely as a canary: it
# costs nothing to watch, and a rename that trips it says "ChEBI has reworked this file, re-audit all
# of it" well before the rename lands on a tag we do consume. check_chebi_sdf_keys() fails the build
# on any key in this set, canaries included -- that is deliberate. To stop watching one, delete it
# from here rather than softening the check.
CHEBI_SDF_KEYS = frozenset(
    {
        "chebiid",
        "chebiname",
        CHEBI_SDF_KEY_SECONDARY_ID,
        CHEBI_SDF_KEY_KEGG,
        CHEBI_SDF_KEY_PUBCHEM,
    }
    | CHEBI_SDF_STRUCTURE_PROPERTIES.keys()
)

# How to read database_accession.tsv. `source_id` names the database ChEBI recorded a value from; for
# MANUAL_X_REF, CITATION and REGISTRY_NUMBER that database is also the identifier's namespace (it is
# source.tsv's `prefix` column: kegg.compound, pubmed, reaxys).
#
# CAS is the exception. A CAS registry number is a shared identifier many databases redistribute, so
# there the namespace is fixed by `type` and source_id records only who supplied it -- `498-15-7`
# arrives under KEGG COMPOUND, ChemIDplus and NIST alike, and 27% of CAS values are shared across
# sources against 2 of 106,179 for CITATION.
#
# So a source_id match alone does not identify an accession, and the three constants below have to be
# applied together. See docs/sources/CHEBI/README.md, whose audit script regenerates that evidence.

# Sources whose MANUAL_X_REF rows we take, matched on source.tsv's `name` column rather than on the
# current id numbers (45 and 68) so that a renumbering raises instead of silently emptying this
# ingest -- the same failure mode as the SDF tag renames.
CHEBI_DBX_SOURCE_NAMES = {
    "KEGG COMPOUND": KEGGCOMPOUND,
    "PubChem Compound": PUBCHEMCOMPOUND,
}

# The type whose accession_number is the source database's own identifier. Restricting to it is what
# keeps shared-namespace rows out: `17  7  498-15-7  CAS  1  45` is a CAS registry number ChEBI
# sourced *from* KEGG COMPOUND, not a KEGG accession, and taking source_id at face value would emit
# 10,615 such CAS numbers as KEGG/PubChem CURIEs.
#
# Ingesting those rows as CAS: xrefs in their own right is issue #956, not an oversight here.
CHEBI_DBX_ACCESSION_TYPE = "MANUAL_X_REF"

# Curation states we accept, resolved by name against status.tsv. ChEBI also publishes SUBMITTED
# rows -- a depositor's unreviewed claim -- which we exclude: 793 of the KEGG COMPOUND rows and 30 of
# the 55 PubChem Compound ones, so the exclusion costs little and these feed glom() as equivalences.
#
# Whether SUBMITTED is in fact reviewed by some other route is issue #957; if it turns out to be
# trustworthy, adding it here is the whole change.
CHEBI_DBX_ACCEPTED_STATUSES = frozenset({"CHECKED", "OK"})


def get_type_from_smiles(smiles):
    if "." in smiles:
        return MOLECULAR_MIXTURE
    else:
        return SMALL_MOLECULE


def write_umls_ids(mrsty, outfile):
    groups = [
        "A1.4.1.1.1.1",  # antibiotic
        "A1.4.1.1.3.2",  # Hormone
        "A1.4.1.1.3.4",  # Vitamin
        "A1.4.1.1.3.5",  # Immunologic Factor
        "A1.4.1.1.4",  # Indicator, Reagent, or Diagnostic Aid
        "A1.4.1.2",  # Chemical Viewed Structurally
        "A1.4.1.2.1",  # Organic Chemical
        "A1.4.1.2.1.5",  # Nucleic Acid, Nucleoside, or Nucleotide
        "A1.4.1.2.2",  # Inorganic Chemical
        "A1.4.1.2.3",  # Element, Ion, or Isotope
        "A1.3.3",  # Clinical Drug
    ]
    # Leaving out these ones:
    exclude_umls_sty_trees = {
        "A1.4.1.1.3.6",  # Receptor
        "A1.4.1.1.3.3",  # Enzyme
        "A1.4.1.2.1.7",  # Amino Acid, Peptide, or Protein
    }
    umlsmap = {a: CHEMICAL_ENTITY for a in groups}
    umlsmap["A1.3.3"] = DRUG
    umls.write_umls_ids(mrsty, umlsmap, outfile, blocklist_umls_semantic_type_tree=exclude_umls_sty_trees)


def write_rxnorm_ids(infile, outfile):
    groups = [
        "A1.4.1.1.1.1",  # antibiotic
        "A1.4.1.1.3.2",  # Hormone
        "A1.4.1.1.3.3",  # Enzyme
        "A1.4.1.1.3.4",  # Vitamin
        "A1.4.1.1.3.5",  # Immunologic Factor
        "A1.4.1.1.4",  # Indicator, Reagent, or Diagnostic Aid
        "A1.4.1.2",  # Chemical Viewed Structurally
        "A1.4.1.2.1",  # Organic Chemical
        "A1.4.1.2.1.5",  # Nucleic Acid, Nucleoside, or Nucleotide
        "A1.4.1.2.2",  # Inorganic Chemical
        "A1.4.1.2.3",  # Element, Ion, or Isotope
        "A1.4",  # Substance
        "A1.3.3",  # Clinical Drug
        "A1.4.1.1.1",  # Pharmacologic Substance
    ]
    # Leaving out these ones:
    filter_types = [
        "A1.4.1.1.3.6",  # Receptor
        "A1.4.1.2.1.7",
    ]  # Amino Acid, Peptide, or Protein
    umlsmap = {a: CHEMICAL_ENTITY for a in groups}
    umlsmap["A1.3.3"] = DRUG
    umlsmap["A1.4.1.1.1"] = DRUG
    umls.write_rxnorm_ids(umlsmap, filter_types, infile, outfile, prefix=RXCUI, styfile="RXNSTY.RRF")


def build_chemical_umls_relationships(mrconso, idfile, outfile, metadata_yaml):
    umls.build_sets(
        mrconso,
        idfile,
        outfile,
        {"MSH": MESH, "DRUGBANK": DRUGBANK, "RXNORM": RXCUI},
        provenance_metadata_yaml=metadata_yaml,
    )


def build_chemical_rxnorm_relationships(conso, idfile, outfile, metadata_yaml):
    umls.build_sets(
        conso,
        idfile,
        outfile,
        {"MSH": MESH, "DRUGBANK": DRUGBANK},
        cui_prefix=RXCUI,
        provenance_metadata_yaml=metadata_yaml,
    )


def write_pubchem_ids(labelfile, smilesfile, outfile):
    # Trying to be memory efficient here.  We could just ingest the whole smilesfile which would make this code easier
    # but since they're already sorted, let's give it a shot
    with (
        open(labelfile) as inlabels,
        gzip.open(smilesfile, "rt", encoding="utf-8") as insmiles,
        open(outfile, "w") as outf,
    ):
        sn = -1
        flag_file_ended = False
        for labelline in inlabels:
            x = labelline.split("\t")[0]
            pn = int(x.split(":")[-1])
            while not flag_file_ended and sn < pn:
                line = insmiles.readline()
                if line == "":
                    # Get this: a blank line in readline() means that we've reached the end-of-file.
                    # (A '\n' would indicate that we've just read a blank line.)
                    flag_file_ended = True
                    break
                smiline = line.strip().split("\t")
                if len(smiline) != 2:
                    raise RuntimeError(f"Could not parse line from {smilesfile}: '{line}'")
                sn = int(smiline[0])

            if sn == pn:
                # We have a smiles for this id
                stype = get_type_from_smiles(smiline[1])
                outf.write(f"{x}\t{stype}\n")
            else:
                # sn > pn, we went past it.  No smiles for that
                print("no smiles:", x, pn, sn)
                outf.write(f"{x}\t{CHEMICAL_ENTITY}\n")


def write_mesh_ids(outfile):
    # MeSH D tree — chemical-related subtrees.
    # Included as CHEMICAL_ENTITY (via the D01–D26 base range):
    #   D01  Inorganic Chemicals
    #   D02  Organic Chemicals
    #   D03  Heterocyclic Compounds
    #   D04  Polycyclic Compounds
    #   D06  Hormones, Hormone Substitutes, and Hormone Antagonists
    #   D07  (no terms currently assigned in MeSH, but covered by the D01–D26 range)
    #   D08.211  Coenzymes (e.g. NAD, Coenzyme A, FAD) — non-protein small molecules
    #   D09  Carbohydrates
    #   D10  Lipids
    #   D11  (no terms currently assigned in MeSH, but covered by the D01–D26 range)
    #   D12  Amino Acids, Peptides, and Proteins (partially — see POLYPEPTIDE below)
    #   D14–D19  (no terms currently assigned in MeSH, but covered by the D01–D26 range)
    #   D21–D22  (no terms currently assigned in MeSH, but covered by the D01–D26 range)
    #   D23  Biological Factors
    #   D24  (no terms currently assigned in MeSH, but covered by the D01–D26 range)
    #   D25  Biomedical and Dental Materials
    #   D26  Pharmaceutical Preparations
    #
    # Included as POLYPEPTIDE:
    #   D12.125  Amino Acids
    #   D12.644  Peptides
    #
    # Included as CHEMICAL_ENTITY (via D01–D26 base range, no override needed):
    #   D13  Nucleic Acids, Nucleotides, and Nucleosides — nucleotides such as NAD also
    #        appear in D08.211 (Coenzymes); CHEMICAL_ENTITY is the correct type for both.
    #
    # Included as COMPLEX_MOLECULAR_MIXTURE:
    #   D20  Complex Mixtures
    #
    # EXCLUDED — protein subtrees (handled by protein.write_mesh_ids instead):
    #   D05.500  Multiprotein Complexes
    #   D05.875  Protein Aggregates
    #   D08.244  Cytochromes
    #   D08.622  Enzyme Precursors
    #   D08.811  Enzymes
    #   D12.776  Proteins
    #
    # INCLUDED as CHEMICAL_ENTITY for now — TODO: assign a more specific Biolink type
    # when the Biolink Model gains a suitable type for non-protein macromolecules:
    #   D05.374  Micelles
    #   D05.750  Polymers
    #   D05.937  Smart Materials
    #
    # D27 (Chemical Actions and Uses) is implicitly excluded by the range D01–D26.
    #
    # TODO: The MeSH tree assignments for chemicals and proteins are currently defined
    # independently in chemicals.write_mesh_ids() and protein.write_mesh_ids(). These
    # should be unified into a shared mapping (e.g. in config.yaml or a dedicated module)
    # so both compendia are derived from the same source of truth. This would also make it
    # easier to handle edge cases like SCR_Chemical terms mapped to non-protein descriptors
    # that are nonetheless proteins (e.g. scorpion venom toxins under D23 Biological Factors).
    meshmap = {f"D{str(i).zfill(2)}": CHEMICAL_ENTITY for i in range(1, 27)}
    # D05 protein subtrees → excluded (protein compendium handles these)
    meshmap["D05.500"] = "EXCLUDE"
    meshmap["D05.875"] = "EXCLUDE"
    # D05.374 Micelles, D05.750 Polymers, D05.937 Smart Materials inherit CHEMICAL_ENTITY
    # TODO: assign a more specific Biolink type for these non-protein macromolecules
    # D08 protein subtrees → excluded (protein compendium handles these)
    meshmap["D08.811"] = "EXCLUDE"
    meshmap["D08.622"] = "EXCLUDE"
    meshmap["D08.244"] = "EXCLUDE"
    # D08.211 Coenzymes inherits CHEMICAL_ENTITY from the D01–D26 base range above
    meshmap["D12.776"] = "EXCLUDE"
    meshmap["D12.125"] = POLYPEPTIDE
    meshmap["D12.644"] = POLYPEPTIDE
    # D13 (Nucleic Acids, Nucleotides, and Nucleosides) inherits CHEMICAL_ENTITY from the
    # D01–D26 base range. No override needed — nucleotides like NAD (D009243) appear in
    # both D08.211 (Coenzymes) and D13; both correctly map to CHEMICAL_ENTITY.
    meshmap["D20"] = COMPLEX_MOLECULAR_MIXTURE
    # Also add anything from SCR_Chemical, if it doesn't have a tree map.
    # SCR terms don't have tree numbers, so we need to separately exclude SCRs
    # mapped to descriptors under excluded trees (proteins, macromolecules, enzymes).
    excluded_trees = [treenum for treenum, category in meshmap.items() if category == "EXCLUDE"]
    mesh.write_ids(
        meshmap,
        outfile,
        order=["EXCLUDE", POLYPEPTIDE, COMPLEX_MOLECULAR_MIXTURE, CHEMICAL_ENTITY],
        extra_vocab={"SCR_Chemical": CHEMICAL_ENTITY},
        scr_exclude_trees=excluded_trees,
    )


# def write_obo_ids(irisandtypes,outfile,exclude=[]):
#    order = [CHEMICAL_SUBSTANCE]
#    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])


def write_chebi_ids(outfile):
    # We're not using obo.write_obo_ids here because we need to 1) grab smiles as well and 2) figure out the types
    chemical_entity_id = f"{CHEBI}:24431"
    racimate_id = f"{CHEBI}:60911"
    mixture_id = f"{CHEBI}:60004"
    peptide_id = f"{CHEBI}:16670"
    uber = UberGraph()
    uberres_chems = uber.get_subclasses_and_smiles(chemical_entity_id)
    uberres_racimates = set([x["descendent"] for x in uber.get_subclasses_of(racimate_id)])  # no smiles for this one
    uberres_mixtures = set([x["descendent"] for x in uber.get_subclasses_of(mixture_id)])  # no smiles for this one
    uberres_peptides = set([x["descendent"] for x in uber.get_subclasses_of(peptide_id)])  # no smiles for this one
    with open(outfile, "w") as idfile:
        for k in uberres_chems:
            desc = k["descendent"]
            if not desc.startswith("CHEBI"):
                continue
            if desc in uberres_racimates:
                ctype = MOLECULAR_MIXTURE
            elif desc in uberres_peptides:
                ctype = POLYPEPTIDE
            elif desc in uberres_mixtures:
                ctype = CHEMICAL_MIXTURE
            elif "SMILES" in k:
                # Is it a mixture?
                ctype = get_type_from_smiles(k["SMILES"])
            else:
                # What is it?
                ctype = CHEMICAL_ENTITY
            idfile.write(f"{k['descendent']}\t{ctype}\n")


def write_unii_ids(infile, outfile):
    """UNII contains a bunch of junk like leaves.   We are going to try to clean it a bit to get things
    that are actually chemicals.  In biolink 2.0 we cn revisit exactly what happens here."""
    with open(outfile, "w") as outf:
        for row in read_unii_records(infile):
            # Whole organisms / crude organism-derived substances (a plant or an eye of newt or
            # something) are not chemicals; skip them. UNII_ORGANISM_COLUMNS is the shared
            # definition, also used by the DrugBank food-and-extract retype (issue #828).
            if not any(row.get(col) for col in UNII_ORGANISM_COLUMNS):
                outf.write(f"{UNII}:{row['UNII']}\t{CHEMICAL_ENTITY}\n")


def write_drugbank_ids(infile, outfile):
    """We don't have a good drugbank source, so we're going to dig through unichem and get out drugbank ids."""
    # doublecheck so that we know we're getting the right value
    drugbank_id = "2"
    assert unichem_data_sources[drugbank_id] == DRUGBANK
    written = set()
    with open(infile) as inf, open(outfile, "w") as outf:
        header_line = inf.readline()
        assert header_line == UNICHEM_REFERENCE_TSV_HEADER, f"Incorrect header line in {infile}: {header_line}"
        for line in inf:
            x = line.rstrip().split("\t")
            if x[1] == drugbank_id:
                if x[2] in written:
                    continue
                dbid = f"{DRUGBANK}:{x[2]}"
                outf.write(f"{dbid}\t{CHEMICAL_ENTITY}\n")
                written.add(x[2])


def write_chemical_ids_from_labels_and_smiles(labelfile, smifile, outfile):
    smiles = {}
    with open(smifile) as inf:
        for line in inf:
            x = line.strip().split("\t")
            smiles[x[0]] = x[1]
    with open(labelfile) as inf, open(outfile, "w") as outf:
        for line in inf:
            hmdbid = line.split("\t")[0]
            if hmdbid in smiles:
                ctype = get_type_from_smiles(smiles[hmdbid])
            else:
                ctype = CHEMICAL_ENTITY
            outf.write(f"{hmdbid}\t{ctype}\n")


def parse_smifile(infile, outfile, smicol, idcol, pref, stripquotes=False):
    idcol_index = None
    smicol_index = None
    with open(infile) as inf, open(outfile, "w") as outf:
        for line in inf:
            if line.startswith('"# GtoPdb Version'):
                # Version line! Skip.
                continue
            if line.startswith('"Ligand ID"'):
                # Header line! Check, then skip.
                header = line.strip().split("\t")
                # print("header: ", header)
                assert header == [
                    '"Ligand ID"',
                    '"Name"',
                    '"Species"',
                    '"Type"',
                    '"Approved"',
                    '"Withdrawn"',
                    '"Labelled"',
                    '"Radioactive"',
                    '"PubChem SID"',
                    '"PubChem CID"',
                    '"UniProt ID"',
                    '"Ensembl ID"',
                    '"ChEMBL ID"',
                    '"Ligand Subunit IDs"',
                    '"Ligand Subunit Name"',
                    '"Ligand Subunit UniProt IDs"',
                    '"Ligand Subunit Ensembl IDs"',
                    '"IUPAC name"',
                    '"INN"',
                    '"Synonyms"',
                    '"SMILES"',
                    '"InChIKey"',
                    '"InChI"',
                    '"GtoImmuPdb"',
                    '"GtoMPdb"',
                    '"Antibacterial"',
                ], f"SMIFile {infile} has a modified header, please update."
                try:
                    idcol_index = header.index(idcol)
                except ValueError:
                    logging.error(f"Could not find ID column '{idcol}' in header {header} for {infile}")
                    exit(1)
                try:
                    smicol_index = header.index(smicol)
                except ValueError:
                    logging.error(f"Could not find SMILES column '{smicol}' in header {header} for {infile}")
                    exit(1)
                continue
            x = line.split("\t")
            if stripquotes:
                x = [xi[1:-1] for xi in x]
            if idcol_index is None:
                logging.error(f"Could not find ID column '{idcol}' in {infile}")
                exit(1)
            if smicol_index is None:
                logging.error(f"Could not find SMILES column '{smicol}' in {infile}")
                exit(1)
            smi = x[smicol_index]
            dcid = f"{pref}:{x[idcol_index]}"
            ctype = get_type_from_smiles(smi)
            outf.write(f"{dcid}\t{ctype}\n")


def write_drugcentral_ids(infile, outfile):
    smicol = 1
    idcol = 0
    with open(infile) as inf, open(outfile, "w") as outf:
        for line in inf:
            x = line.strip().split("\t")
            if x[smicol] == "None":
                outf.write(f"{x[idcol]}\t{CHEMICAL_ENTITY}\n")
            else:
                outf.write(f"{x[idcol]}\t{get_type_from_smiles(x[smicol])}\n")


def write_gtopdb_ids(infile, outfile):
    parse_smifile(infile, outfile, '"SMILES"', '"Ligand ID"', GTOPDB, stripquotes=True)


def write_unichem_concords(structfile, reffile, outdir):
    inchikeys = read_inchikeys(structfile)
    concfiles = {}
    row_counts = {}
    double_prefix_warned = set()  # sources where we already logged the strip-warning
    for num, name in unichem_data_sources.items():
        concname = f"{outdir}/UNICHEM_{name}"
        print(concname)
        concfiles[num] = open(concname, "w")
        row_counts[num] = 0
    try:
        with open(reffile) as inf:
            header_line = inf.readline()
            assert header_line == UNICHEM_REFERENCE_TSV_HEADER, f"Incorrect header line in {reffile}: {header_line}"
            for line in inf:
                x = line.rstrip().split("\t")
                src_id = x[1]
                compound_id = x[2]
                expected_prefix = unichem_data_sources[src_id]
                outf = concfiles[src_id]
                assert x[3] == "1", (  # Only '1' (current) assignments should be in this file
                    f"Expected assignment '1' (current) but got {x[3]!r} for src_id={src_id!r}, "
                    f"compound_id={compound_id!r}; filter_unichem should have excluded non-current rows "
                    f"(see https://chembl.gitbook.io/unichem/definitions/what-is-an-assignment)"
                )

                # Guard against UniChem embedding the prefix inside the compound ID.
                # e.g. CHEBI source stores "CHEBI:12345" instead of bare "12345".
                if ":" in compound_id:
                    embedded_prefix, bare_id = compound_id.split(":", 1)
                    if embedded_prefix == expected_prefix:
                        # Double prefix — strip the embedded one and warn once per source.
                        if src_id not in double_prefix_warned:
                            logger.warning(
                                f"UniChem source {src_id} ({expected_prefix}): compound ID already contains "
                                f"the prefix (e.g. {compound_id!r}). Stripping embedded prefix. "
                                f"Consider reporting this to UniChem."
                            )
                            double_prefix_warned.add(src_id)
                        compound_id = bare_id
                    else:
                        raise ValueError(
                            f"UniChem source {src_id} ({expected_prefix}): compound ID {compound_id!r} "
                            f"contains an unexpected embedded prefix {embedded_prefix!r}. "
                            f"Expected either a bare ID or one prefixed with {expected_prefix!r}. "
                            f"Update unichem_data_sources in src/datahandlers/unichem.py if this source "
                            f"has changed its identifier scheme."
                        )

                outf.write(f"{expected_prefix}:{compound_id}\toio:equivalent\t{inchikeys[x[0]]}\n")
                row_counts[src_id] += 1
    finally:
        for outf in concfiles.values():
            outf.close()

    empty_sources = [(num, unichem_data_sources[num]) for num, count in row_counts.items() if count == 0]
    if empty_sources:
        descriptions = ", ".join(f"{name!r} (source ID {num!r})" for num, name in empty_sources)
        raise RuntimeError(
            f"UniChem reference file produced no entries for the following sources: {descriptions}. "
            f"These sources may have been removed or renumbered in the current UniChem release. "
            f"To fix: remove them from unichem_data_sources in src/datahandlers/unichem.py and "
            f"from unichem_datasources (and chemical_labels/chemical_ids if present) in config.yaml, "
            f"then rerun this step."
        )


def read_inchikeys(struct_file):
    # struct header [0'uci', 1'standardinchi', 2'standardinchikey'],
    inchikeys = {}
    with gzip.open(struct_file, "rt") as inf:
        header_line = inf.readline()
        assert header_line == UNICHEM_STRUCT_TSV_HEADER, f"Unexpected header line in {struct_file}: {header_line}"
        for sline in inf:
            line = sline.rstrip().split("\t")
            if len(line) == 0:
                continue
            uci = line[0]
            inchikeys[uci] = f"{INCHIKEY}:{line[2]}"
    return inchikeys


def combine_unichem(concordances, output):
    PREFIXES_TO_REMOVE_OVERUSED_XREFS = [UNII, KEGGCOMPOUND, DRUGCENTRAL]

    dicts = {}
    for infile in concordances:
        print(infile)
        print("loading", infile)
        pairs = []

        # We will want to only remove overused xrefs for specific prefixes.
        # UniChem files should only have a single prefix in the first column,
        # but out of paranoia we'll double-check that.
        prefixes_in_file = set()

        with open(infile) as inf:
            for line in inf:
                x = line.strip().split("\t")
                pairs.append([x[0], x[2]])
                # Get the prefix from the first row to determine if we need to remove overused xrefs
                prefixes_in_file.add(Text.get_prefix(x[0]))

        # Was there exactly one prefix in the first column?
        if len(prefixes_in_file) == 0:
            raise RuntimeError(
                f"No prefixes found in {infile} (file may be empty or have no valid CURIE in column 1). All UNICHEM files should have exactly one prefix."
            )
        if len(prefixes_in_file) > 1:
            raise RuntimeError(
                f"Multiple prefixes found in {infile}: {prefixes_in_file}. All UNICHEM files should have exactly one prefix."
            )
        prefix_to_check = prefixes_in_file.pop()

        # Only remove overused xrefs for specific prefixes
        newpairs = pairs
        if prefix_to_check in PREFIXES_TO_REMOVE_OVERUSED_XREFS:
            newpairs = remove_overused_xrefs(pairs)
        setpairs = [set(x) for x in newpairs]
        glom(dicts, setpairs, unique_prefixes=[INCHIKEY])
    chem_sets = set([frozenset(x) for x in dicts.values()])
    with jsonlines.open(output, mode="w") as writer:
        for chemset in chem_sets:
            writer.write(list(chemset))


def read_partial_unichem(unichem_partial):
    chem_sets = {}
    with jsonlines.open(unichem_partial) as reader:
        for chemlist in reader:
            chemset = set(chemlist)
            for element in chemset:
                chem_sets[element] = chemset
    return chem_sets


def is_cas(thing):
    # The last digit in a CAS is a checksum. We could use, but are not atm.
    x = thing.split("-")
    if len(x) != 3:
        return False
    if len(x[-1]) != 1:
        return False
    for xi in x:
        if not xi.isnumeric():
            return False
    return True


def make_pubchem_cas_concord(pubchemsynonyms, outfile, metadata_yaml):
    with open(pubchemsynonyms) as inf, open(outfile, "w") as outf:
        for line in inf:
            x = line.strip().split("\t")
            if is_cas(x[1]):
                outf.write(f"{x[0]}\txref\tCAS:{x[1]}\n")

    write_concord_metadata(
        metadata_yaml,
        name="make_pubchem_cas_concord()",
        description="make_pubchem_cas_concord() creates xrefs from PUBCHEM identifiers in the PubChem synonyms file "
        + f"({pubchemsynonyms}) to Chemical Abstracts Service (CAS) identifiers.",
        concord_filename=outfile,
    )


def make_pubchem_mesh_concord(pubcheminput, meshlabels, outfile, metadata_yaml):
    mesh_label_to_id = {}
    # Meshlabels has all kinds of stuff. e.g. these are both in there:
    # MESH:D014867    Water
    # MESH:M0022883   Water
    # but we only want the ones that are MESH:D... or MESH:C....
    with open(meshlabels) as inf:
        for line in inf:
            x = line.strip().split("\t")
            if x[0].split(":")[-1][0] in ["C", "D"]:
                mesh_label_to_id[x[1]] = x[0]
    # The pubchem - mesh pairs are supposed to be ordered in this file such that the
    # first mapping is the 'best' i.e. the one most frequently reported.
    # We will only use the first one
    used_pubchem = set()
    with open(pubcheminput) as inf, open(outfile, "w") as outf:
        for line in inf:
            x = line.strip().split("\t")  # x[0] = puchemid (no prefix), x[1] = mesh label
            if x[0] in used_pubchem:
                continue
            try:
                mesh_id = mesh_label_to_id[x[1]]
            except Exception:
                print(f"no mesh for label {x[1]}")
                continue
            outf.write(f"{PUBCHEMCOMPOUND}:{x[0]}\txref\t{mesh_id}\n")
            used_pubchem.add(x[0])

    write_concord_metadata(
        metadata_yaml,
        name="make_pubchem_mesh_concord()",
        description=f"make_pubchem_mesh_concord() loads MeSH labels from {meshlabels}, then creates xrefs from PubChem "
        + f"identifiers in the PubChem input file ({pubcheminput}) to those MeSH identifiers using the labels as keys.",
        concord_filename=outfile,
    )


def build_drugcentral_relations(infile, outfile, metadata_yaml):
    prefixmap = {
        "CHEBI": CHEBI,
        "ChEMBL_ID": CHEMBLCOMPOUND,
        "DRUGBANK_ID": DRUGBANK,
        "IUPHAR_LIGAND_ID": GTOPDB,
        "MESH_DESCRIPTOR_UI": MESH,
        "PUBCHEM_CID": PUBCHEMCOMPOUND,
        "UMLSCUI": UMLS,
        "UNII": UNII,
    }
    external_id_col = 1
    external_ns_col = 2
    drugcentral_id_col = 3
    with open(infile) as inf, open(outfile, "w") as outf:
        for line in inf:
            parts = line.strip().split("\t")
            # print(parts)
            if len(parts) < 4:
                continue
            external_ns = parts[external_ns_col]
            if external_ns not in prefixmap:
                continue
            # print('ok')
            outf.write(
                f"{DRUGCENTRAL}:{parts[drugcentral_id_col]}\txref\t{prefixmap[external_ns]}:{parts[external_id_col]}\n"
            )

    write_concord_metadata(
        metadata_yaml,
        name="build_drugcentral_relations()",
        description=f"Build xrefs from DrugCentral ({infile}) to {DRUGCENTRAL} using the prefix map {prefixmap}.",
        concord_filename=outfile,
    )


def make_gtopdb_relations(infile, outfile, metadata_yaml):
    with open(infile) as inf, open(outfile, "w") as outf:
        h = inf.readline()
        # We might have a header/version line. If so, skip to the next line.
        if h.startswith('"# GtoPdb Version'):
            h = inf.readline()
        h = h.strip().split("\t")
        gid_index = h.index('"Ligand ID"')
        inchi_index = h.index('"InChIKey"')
        for line in inf:
            x = line.strip().split("\t")
            if x[inchi_index] == '""':
                continue
            gid = f"{GTOPDB}:{x[gid_index][1:-1]}"
            inchi = f"{INCHIKEY}:{x[inchi_index][1:-1]}"
            outf.write(f"{gid}\txref\t{inchi}\n")

    write_concord_metadata(
        metadata_yaml,
        name="make_gtopdb_relations()",
        description=f"Transform Ligand ID/InChIKey mappings from {infile} into a concord.",
        concord_filename=outfile,
    )


def split_chebi_sdf_values(value_lines):
    """
    Split the value lines of a ChEBI SDF data item into individual values.

    ChEBI packs multiple values for one tag as a semicolon-delimited list, so a tag whose value is
    "C00001;C00002" is two KEGG accessions, not one. Splitting matters for correctness, not just
    completeness: joining an unsplit value onto a prefix produces a CURIE like
    `KEGG.COMPOUND:C00001;C00002` that matches nothing downstream.

    read_sdf() always hands back a *list* of the lines under a tag, so this iterates lines as well
    as splitting each one. In the babel-1.18 SDF no tag we read spans more than one line -- only
    `DEFINITION`, which we don't read, does (5 entries) -- but the older code here concatenated
    multiple lines, so the behaviour is kept rather than assumed away.

    :param value_lines: The list of raw value lines read for one tag.
    :return: A list of individual values, stripped, with empties dropped.
    """
    values = []
    for line in value_lines:
        for value in line.split(";"):
            value = value.strip()
            if value:
                values.append(value)
    return values


def read_chebi_lookup_ids(lookup_tsv, wanted_names=None):
    """
    Map id -> name for the wanted rows of one of ChEBI's id/name lookup tables.

    database_accession.tsv refers to both its source and its curation status by number only;
    source.tsv and status.tsv are the lookup tables. Resolving by name means a ChEBI renumbering
    fails here rather than quietly changing which rows we accept, which is exactly how the SDF tag
    renames went unnoticed.

    Note that on database_accession.tsv a source_id identifies the *target* database only on
    MANUAL_X_REF rows -- on rows of other types it records where ChEBI sourced the value instead,
    which is why callers must pair the source with CHEBI_DBX_ACCESSION_TYPE rather than matching on
    the source alone.

    :param lookup_tsv: Path to a ChEBI lookup table whose first two columns are `id` and `name`
        (source.tsv, status.tsv).
    :param wanted_names: The names to look up, or None for every row (in which case there is nothing
        to be missing, so no completeness check runs -- used for labelling, not for matching).
    :return: {id -> name}, restricted to wanted_names.
    :raise ValueError: If the header is not id/name, or any wanted name is absent from the file.
    """
    ids_by_name = {}
    # Gzip-aware so the documented audit invocation, which points straight at the FTP downloads,
    # works on the .tsv.gz files as well as the pipeline's decompressed copies.
    with open_maybe_gzipped(lookup_tsv) as inf:
        header = inf.readline().rstrip("\n").split("\t")
        if header[:2] != ["id", "name"]:
            raise ValueError(
                f"Unexpected columns in ChEBI lookup file {lookup_tsv}: {header}. "
                f"Expected it to start with 'id' and 'name'."
            )
        for line in inf:
            row = line.rstrip("\n").split("\t")
            if len(row) < 2:
                continue
            if wanted_names is None or row[1] in wanted_names:
                ids_by_name[row[0]] = row[1]

    missing = set() if wanted_names is None else set(wanted_names) - set(ids_by_name.values())
    if missing:
        raise ValueError(
            f"ChEBI lookup file {lookup_tsv} does not list these expected names: {sorted(missing)}. "
            f"ChEBI has probably renamed them; update the matching constant in chemicals.py."
        )
    return ids_by_name


def check_chebi_sdf_keys(chebi_sdf_dat, sdf):
    """
    Fail if any tag make_chebi_relations() reads is absent from every entry of the ChEBI SDF.

    This is the ChEBI SDF's equivalent of checking a CSV header. read_sdf() matches tags by exact
    string and drops anything it doesn't recognize, so when ChEBI renames a tag the ingest doesn't
    error -- it just produces nothing for that field, for every compound, and the build completes
    looking healthy. babel-1.18 shipped that way: `Secondary ChEBI ID` had become `SECONDARY_ID`,
    so every ChEBI secondary identifier silently stopped normalizing.

    Checking key presence rather than merely "did we write a non-zero number of rows" means the
    error names the tag that vanished, which is the part that takes the time to work out.

    :param chebi_sdf_dat: The parsed SDF, as returned by read_sdf().
    :param sdf: The SDF filename, for the error message.
    :raise ValueError: If any key in CHEBI_SDF_KEYS appears in no entry at all.
    """
    keys_seen = set()
    for props in chebi_sdf_dat.values():
        keys_seen.update(props.keys())

    missing = CHEBI_SDF_KEYS - keys_seen
    if missing:
        raise ValueError(
            f"ChEBI SDF file {sdf} contains no entries carrying these expected tags: "
            f"{sorted(missing)}. ChEBI has probably renamed them -- re-audit the file's tags with "
            f"docs/sources/CHEBI/sdf_tags/audit_sdf_tags.py and update CHEBI_SDF_KEYS to match."
        )


# The root whose descendants we collect ChEBI role assertions for. The same root write_chebi_ids()
# walks, so the property file covers exactly the ChEBI terms the chemical pipeline knows about.
#
# Note that this deliberately does *not* cover the role terms themselves: CHEBI:50906 "role" is a
# separate ChEBI root, not a descendant of CHEBI:24431, so roles are the values here and never the
# subjects. Normalizing role terms as entities in their own right (biolink:ChemicalRole) is #101.
CHEBI_ROLE_ROOT = f"{CHEBI}:24431"


def make_chebi_roles(outfile_gz):
    """
    Write ChEBI's has_role (RO:0000087) assertions to a property file.

    These are annotations on a chemical, not equivalence assertions, so they are written as
    Property rows and take no part in concord or clique building. The values are role CURIEs that
    Babel does not currently normalize as entities -- see issue #101.

    No metadata YAML is written, matching the other property file this module produces.
    write_concord_metadata() reads its subject as a three-column TSV to count prefix pairs, which a
    gzipped JSONL property file is not; a property file's provenance is the `source` field carried
    on every row.

    :param outfile_gz: The gzipped JSONL property file to write.
    :raise ValueError: If UberGraph returns no role assertions at all.
    """
    uber = UberGraph()
    role_pairs = uber.get_roles(CHEBI_ROLE_ROOT)

    # Guard the input, the way check_chebi_sdf_keys() and make_chebi_relations()'s per-input counts
    # guard theirs. A SPARQL query against a predicate that has been renamed returns an empty result
    # set rather than an error -- which is exactly how the CHEBIP:smiles lookup in
    # get_subclasses_and_smiles() went quiet for every one of ~194,000 terms (issue #1086). An empty
    # result here means the query is broken, not that ChEBI has stopped curating roles.
    if not role_pairs:
        raise ValueError(
            f"UberGraph returned no {HAS_ROLE} assertions for descendants of {CHEBI_ROLE_ROOT}. "
            f"ChEBI curates tens of thousands of these, so this is a broken query rather than an "
            f"empty source -- check whether the predicate or the graph names have changed."
        )

    ensure_parent_dir(outfile_gz)
    # Sorted so re-runs diff cleanly.
    with gzip.open(outfile_gz, "wt") as outf:
        for term, role in sorted(set(role_pairs)):
            outf.write(
                Property(
                    curie=term,
                    predicate=HAS_ROLE,
                    value=role,
                    source=f"Asserted as a ChEBI role ({HAS_ROLE}) of {term}, read from UberGraph",
                ).to_json_line()
            )

    logger.info(f"make_chebi_roles() wrote {len(set(role_pairs))} role assertions to {outfile_gz}.")


def make_chebi_relations(sdf, dbx, dbx_source, dbx_status, outfile, propfile_gz, structfile_gz, metadata_yaml):
    """CHEBI contains relations both about chemicals with and without inchikeys.  You might think that because
    everything is based on unichem, we could avoid the with structures part, but history has shown that we lose
    links in that case, so we will use both the structured and unstructured chemical entries.

    Writes three files: the xref concord, a property file of ChEBI secondary identifiers, and a
    property file of the SDF's structure and physical-property values (SMILES, InChI, InChIKey,
    formula, mass, monoisotopic mass, charge).

    The structure properties go in their own file rather than into propfile_gz on purpose. That file
    is an input to the chemical_compendia rule, which loads every property in it into an in-memory
    PropertyList inside write_compendium(); nothing consumes structure properties yet, so folding
    ~1.2M more of them into a rule already sized at 512G would buy nothing. When something does
    consume them, adding this file to that rule's inputs is a one-line change.

    :param sdf: ChEBI_complete.sdf.
    :param dbx: database_accession.tsv.
    :param dbx_source: source.tsv, for resolving dbx source_id to a database name.
    :param dbx_status: status.tsv, for resolving dbx status_id to a curation state.
    :param outfile: The xref concord to write.
    :param propfile_gz: The gzipped JSONL property file for ChEBI secondary identifiers.
    :param structfile_gz: The gzipped JSONL property file for the SDF's structure properties.
    :param metadata_yaml: The concord metadata YAML to write.
    """
    # THE SDF and XREF stuff are handled in the same function because knowing what we found in the SDF impacts
    # what we want to get out of the xrefs. But the function is quite unwieldy
    # READ SDF
    chebi_sdf_dat = read_sdf(sdf, CHEBI_SDF_KEYS)
    check_chebi_sdf_keys(chebi_sdf_dat, sdf)
    # CHEBIs in the sdf by definition have structure (the sdf is a structure file)
    structured_chebi = set(chebi_sdf_dat.keys())
    # READ xrefs
    dbx_prefixes_by_source_id = {
        source_id: CHEBI_DBX_SOURCE_NAMES[name]
        for source_id, name in read_chebi_lookup_ids(dbx_source, set(CHEBI_DBX_SOURCE_NAMES)).items()
    }
    dbx_accepted_status_ids = set(read_chebi_lookup_ids(dbx_status, CHEBI_DBX_ACCEPTED_STATUSES))
    with open(dbx) as inf:
        dbxdata = inf.read()
    kk = CHEBI_SDF_KEY_KEGG
    pk = CHEBI_SDF_KEY_PUBCHEM
    secondary_chebi_id = CHEBI_SDF_KEY_SECONDARY_ID

    # What if we don't have a propfile directory?
    ensure_parent_dir(propfile_gz)
    ensure_parent_dir(structfile_gz)

    # Output rows counted per source key rather than in aggregate. A total-count check does not
    # protect an individual input: the SDF's ~181,000 PubChem xrefs could vanish entirely and KEGG's
    # ~16,000 would keep it quiet -- which is exactly the shape of the bug this function is being
    # fixed for. Each key is therefore checked on its own below.
    counts = Counter()
    # database_accession.tsv is checked alongside the SDF's tags. It is the input that spent a
    # release contributing nothing (issue #954) precisely because no check covered it on its own --
    # the SDF's ~197,000 xrefs kept any whole-output guard quiet.
    dbx_key = "database_accession.tsv"

    with open(outfile, "w") as outf, gzip.open(propfile_gz, "wt") as propf, gzip.open(structfile_gz, "wt") as structf:
        # Write SDF structured things
        for cid, props in chebi_sdf_dat.items():
            for sdf_key, predicate in CHEBI_SDF_STRUCTURE_PROPERTIES.items():
                if sdf_key not in props:
                    continue
                # Deliberately NOT split_chebi_sdf_values(). That splits on ";", which is a
                # separator for a multi-valued *xref* tag but a legitimate character inside an
                # InChI: a multi-component InChI uses it to separate per-component layers, as in
                # `InChI=1S/2ClH.Ca/h2*1H;/q;;+2/p-2` for calcium chloride. Splitting here would
                # shred 3,997 of the 181,048 InChI values in the 2026-06-29 SDF into fragments that
                # are not InChIs at all. Every tag in this map is single-valued and, in that file,
                # single-line; the lines are joined rather than indexed so a future wrapped value
                # is concatenated instead of silently truncated to its first line.
                value = "".join(line.strip() for line in props[sdf_key])
                if not value:
                    continue
                structf.write(
                    Property(
                        curie=cid,
                        predicate=predicate,
                        value=value,
                        source=f"Read from the {sdf_key} data item of the ChEBI SDF file ({sdf})",
                    ).to_json_line()
                )
                counts[sdf_key] += 1
            if secondary_chebi_id in props:
                # SECONDARY_ID holds already-prefixed CURIEs, semicolon-delimited on one line, e.g.
                # CHEBI:421707 "abacavir" carries
                # "CHEBI:193608;CHEBI:441792;CHEBI:2360;CHEBI:525912;CHEBI:520984".
                for secondary_id in split_chebi_sdf_values(props[secondary_chebi_id]):
                    propf.write(
                        Property(
                            curie=cid,
                            predicate=HAS_ALTERNATIVE_ID,
                            value=secondary_id,
                            source=f"Listed as a CHEBI secondary ID in the ChEBI SDF file ({sdf})",
                        ).to_json_line()
                    )
                    counts[secondary_chebi_id] += 1
            if kk in props:
                for kegg_id in split_chebi_sdf_values(props[kk]):
                    outf.write(f"{cid}\txref\t{KEGGCOMPOUND}:{kegg_id}\n")
                    counts[kk] += 1
            if pk in props:
                # Bare compound IDs, semicolon-delimited. This used to be a single "PubChem Database
                # Links" tag holding "SID: nnn CID: nnn" pairs, which is why older code here parsed
                # on those labels; ChEBI now splits substances and compounds into separate tags.
                # The SDF's ~191,000 PubChem *substance* xrefs are deliberately left alone -- they
                # are submitter-deposited records and so a much weaker equivalence assertion than a
                # compound. See docs/sources/CHEBI/README.md if we ever want them.
                for pubchem_id in split_chebi_sdf_values(props[pk]):
                    outf.write(f"{cid}\txref\t{PUBCHEMCOMPOUND}:{pubchem_id}\n")
                    counts[pk] += 1
        # DO THE xref stuff
        # database_accession.tsv columns: id, compound_id, accession_number, type, status_id,
        # source_id. Type, source and status must all match: on a CAS row the accession belongs to
        # CAS rather than to the source that supplied it, and SUBMITTED rows are unreviewed depositor
        # claims. See CHEBI_DBX_SOURCE_NAMES above.
        lines = dbxdata.split("\n")
        for line in lines[1:]:
            x = line.strip().split("\t")
            if len(x) < 6:
                continue
            if x[3] != CHEBI_DBX_ACCESSION_TYPE:
                continue
            if x[4] not in dbx_accepted_status_ids:
                continue
            prefix = dbx_prefixes_by_source_id.get(x[5])
            if prefix is None:
                continue
            cid = f"{CHEBI}:{x[1]}"
            if cid in structured_chebi:
                continue
            outf.write(f"{cid}\txref\t{prefix}:{x[2]}\n")
            counts[dbx_key] += 1

    # Backstop to check_chebi_sdf_keys(): that catches a renamed SDF tag, this catches the failures
    # a tag name cannot show -- a changed value format, a truncated download, a parse bug. ChEBI
    # publishes every one of these in every release, so any one coming out empty means a silently
    # broken build. Counted per input rather than in aggregate: both bugs this function has shipped
    # were one input going quiet while the others kept a whole-output guard satisfied. The structure
    # tags are checked on the same terms, except the sparse ones -- see
    # CHEBI_SDF_SPARSE_STRUCTURE_KEYS for why CHARGE is exempt.
    #
    # The dbx count is of rows actually *written*, so it is zero either when that file is broken or,
    # in principle, when every one of its rows was skipped as already-structured. The second case is
    # not distinguished, deliberately: it needs all ~18,000 of its accessions to be for SDF entries,
    # which has never been close to true, and a build where it became true is one worth stopping for.
    #
    # Both files have already been written and closed by this point, so the empty file exists on
    # disk when we raise; Snakemake deletes a failed job's outputs, but a direct call leaves it
    # behind. Hence "wrote an empty ..." rather than "refusing to write".
    checked_keys = (
        secondary_chebi_id,
        kk,
        pk,
        dbx_key,
        *(CHEBI_SDF_STRUCTURE_PROPERTIES.keys() - CHEBI_SDF_SPARSE_STRUCTURE_KEYS),
    )
    empty_keys = sorted(key for key in checked_keys if counts[key] == 0)
    if empty_keys:
        raise ValueError(
            f"ChEBI ingest from {sdf} and {dbx} produced no rows at all for: {empty_keys}. The SDF "
            f"tags themselves are present, so this is not a rename -- look for a changed value "
            f"format, a changed column layout, or a truncated download. Wrote empty output to "
            f"{outfile}, {propfile_gz} and {structfile_gz}; delete them and re-run once the cause "
            f"is fixed."
        )
    logger.info(f"make_chebi_relations() wrote {dict(counts)}.")

    write_concord_metadata(
        metadata_yaml,
        name="make_chebi_relations()",
        description=f"make_chebi_relations() creates xrefs from the ChEBI database ({sdf}) to {PUBCHEMCOMPOUND} and {KEGGCOMPOUND}.",
        concord_filename=outfile,
    )


def get_mesh_relationships(mesh_id_file, cas_out, unii_out, cas_metadata, unii_metadata):
    meshes = set()
    with open(mesh_id_file) as inf:
        for line in inf:
            x = line.split("\t")
            meshes.add(x[0])
    regis = mesh.pull_mesh_registry()
    with open(cas_out, "w") as casout, open(unii_out, "w") as uniiout:
        for meshid, reg in regis:
            if meshid not in meshes:
                continue
            if reg.startswith("EC"):
                continue
            if reg.startswith("txid"):
                # is a taxon
                continue
            if is_cas(reg):
                casout.write(f"{meshid}\txref\tCAS:{reg}\n")
            else:
                # is a unii?
                uniiout.write(f"{meshid}\txref\tUNII:{reg}\n")

    write_concord_metadata(
        cas_metadata,
        name="get_mesh_relationships()",
        sources=[
            {
                "type": "MeSH Registry",
                "name": "MeSH Registry",
            }
        ],
        description=f"get_mesh_relationships() iterates through the MeSH registry, filters it to the MeSH IDs "
        f"in {mesh_id_file}, then writes out CAS mappings to {cas_out}",
        concord_filename=cas_out,
    )

    write_concord_metadata(
        unii_metadata,
        name="get_mesh_relationships()",
        sources=[
            {
                "type": "MeSH Registry",
                "name": "MeSH Registry",
            }
        ],
        description=f"get_mesh_relationships() iterates through the MeSH registry, filters it to the MeSH IDs "
        f"in {mesh_id_file}, then writes out non-CAS mappings (i.e. UNII mappings) to {unii_out}",
        concord_filename=unii_out,
    )


def get_wikipedia_relationships(outfile, config, metadata_yaml):
    url = "https://query.wikidata.org/sparql?format=json&query=SELECT ?chebi ?mesh WHERE { ?compound wdt:P683 ?chebi . ?compound wdt:P486 ?mesh. }"
    results = requests.get(url, headers={"User-Agent": get_user_agent()}).json()
    pairs = [
        (f"{MESH}:{r['mesh']['value']}", f"{CHEBI}:{r['chebi']['value']}")
        for r in results["results"]["bindings"]
        if not r["mesh"]["value"].startswith("M")
    ]
    # Wikidata is great, except when it sucks.   One thing it likes to do is to
    # have multiple CHEBIs for a concept, say ignoring stereochemistry or
    # the like.  No good.   It's easy enough to filter these out, but then
    # we wouldn't have the mesh associated with anything. A spot check makes it seem like
    # cases of this type usually also have a UNII.  So we can perhaps remove ugly pairs without
    # a problem. We leave them in at this point, and they will get filtered out on reading
    with open(outfile, "w") as outf:
        # m2c = defaultdict(list)
        for m, c in pairs:
            outf.write(f"{m}\txref\t{c}\n")

    write_concord_metadata(
        metadata_yaml,
        name="get_wikipedia_relationships()",
        sources=[
            {
                "type": "Wikidata",
                "name": "Wikidata SPARQL query",
            }
        ],
        description="Wikidata SPARQL query to find Wikidata entities with both CHEBI and MESH IDs, and build a concordance between them.",
        concord_filename=outfile,
    )


def build_untyped_compendia(
    concordances, identifiers, unichem_partial, untyped_concord, type_file, metadata_yaml, input_metadata_yamls
):
    """:concordances: a list of files from which to read relationships
    :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = read_partial_unichem(unichem_partial)
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=[INCHIKEY])
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print("loading", infile)
        pairs = []
        with open(infile) as inf:
            for line in inf:
                x = line.strip().split("\t")
                pairs.append([x[0], x[2]])
        p = False
        if DRUGCENTRAL in [n.split(":")[0] for n in pairs[0]]:
            p = True
            i = "DrugCentral:4970"
        if p:
            print("before filtering:")
            for pair in pairs:
                if i in pair:
                    print(pair)
        newpairs = remove_overused_xrefs(pairs)
        setpairs = [set(x) for x in newpairs]
        if p:
            print("after filtering:")
            for pair in newpairs:
                if i in pair:
                    print(pair)
        glom(dicts, setpairs, unique_prefixes=[INCHIKEY])
        if p:
            print("after glomming:")
            print(dicts[i])
    with open(type_file, "w") as outf:
        for x, y in types.items():
            outf.write(f"{x}\t{y}\n")
    untyped_sets = set([frozenset(x) for x in dicts.values()])
    with open(untyped_concord, "w") as outf:
        for s in untyped_sets:
            outf.write(f"{set(s)}\n")

    # Build the metadata file by combining the input metadata_yamls.
    write_combined_metadata(
        filename=metadata_yaml,
        typ="untyped_compendium",
        name="chemicals.build_untyped_compendia()",
        description=f'Generate an untyped compendium from concordances {concordances}, identifiers {identifiers}, " +'
        f"unichem_partial {unichem_partial}, untyped_concord {untyped_concord}, and type file {type_file}.",
        # sources=None, url='', counts=None,
        combined_from_filenames=input_metadata_yamls,
    )


def build_compendia(
    type_file,
    untyped_compendia_file,
    properties_jsonl_gz_files,
    metadata_yamls,
    icrdf_filename,
    food_type_files,
):
    types = {}
    with open(type_file) as inf:
        for line in inf:
            x = line.strip().split("\t")
            types[x[0]] = x[1]
    logger.info(f"Loaded {len(types)} types from {type_file}: {get_memory_usage_summary()}")

    # Food/extract evidence (issues #828, #935): each file in food_type_files is CURIE\tbiolink:Type.
    # Today the only producer is the DRUGBANK food-and-extract retype — foods to biolink:Food, extracts
    # (pollens/danders/...) to biolink:ComplexMolecularMixture. These CURIEs enter chemical cliques via
    # the UMLS/RXNORM concords carrying no Babel type of their own, so the per-identifier vote alone
    # would leave them as ChemicalEntity; create_typed_sets adds this evidence to the vote as an extra
    # candidate.
    food_types = {}
    for food_type_file in food_type_files:
        _, file_types = read_identifier_file(food_type_file)
        food_types.update(file_types)
    logger.info(f"Loaded {len(food_types)} food/extract types from {food_type_files}: {get_memory_usage_summary()}")

    # create_typed_sets() ranks this evidence with order.index(), so a type missing from
    # chemical_type_order is a ValueError tens of millions of cliques into the build. Check it here,
    # where it costs one pass over a few hundred CURIEs and fails in the first second instead.
    unrankable = set(food_types.values()) - set(get_config()["chemical_type_order"])
    if unrankable:
        raise ValueError(
            f"Food/extract evidence in {food_type_files} uses types absent from config.yaml's "
            f"chemical_type_order, so they cannot be ranked against a clique's voted type: {sorted(unrankable)}"
        )

    untyped_sets = set()
    with open(untyped_compendia_file) as inf:
        for line in inf:
            s = ast.literal_eval(line.strip())
            untyped_sets.add(frozenset(s))
    logger.info(f"Loaded {len(untyped_sets)} untyped sets from {untyped_compendia_file}: {get_memory_usage_summary()}")

    typed_sets = create_typed_sets(untyped_sets, types, food_types)
    logger.info(
        f"Created {len(typed_sets)} typed sets from {len(untyped_sets)} untyped sets: {get_memory_usage_summary()}"
    )

    for biotype, sets in typed_sets.items():
        baretype = biotype.split(":")[-1]
        if biotype == DRUG:
            write_compendium(
                metadata_yamls,
                sets,
                f"{baretype}.txt",
                biotype,
                {},
                extra_prefixes=[MESH, UNII],
                icrdf_filename=icrdf_filename,
                properties_jsonl_gz_files=properties_jsonl_gz_files,
            )
        else:
            write_compendium(
                metadata_yamls,
                sets,
                f"{baretype}.txt",
                biotype,
                {},
                extra_prefixes=[RXCUI],
                icrdf_filename=icrdf_filename,
                properties_jsonl_gz_files=properties_jsonl_gz_files,
            )


def create_typed_sets(eqsets, types, food_types):
    """
    Given a set of sets of equivalent identifiers, we want to type each one into
    being a subclass of ChemicalEntity.

    :param eqsets: A list of lists of identifiers (should NOT be a list of LabeledIDs, but a list of strings).
    :param types: A dictionary of known types for each identifier. (Some identifiers don't have known types.)
    :param food_types: {CURIE -> biolink_type} of food/extract evidence (issues #828, #935): a DrugBank
        material whose UNII's NCIt class says "food", or whose name says "extract". These CURIEs carry no
        Babel type of their own, so the evidence is added to the clique's type vote as an extra
        candidate -- *not* as an override. The most preferred of the voted and the food/extract types
        wins, per ``order``, which is what keeps glucose a SmallMolecule: DrugBank's structureless
        "Dextrose, unspecified form" is a food, but the clique it gloms into votes SmallMolecule, and
        SmallMolecule is preferred.
    """
    # Most preferred first; see config.yaml: chemical_type_order for the ranking's rationale. Read once
    # here rather than per clique -- this function iterates tens of millions of them.
    order = get_config()["chemical_type_order"]
    # This loop runs once per chemical clique (tens of millions), and food_types holds a few hundred
    # CURIEs, so test membership with a set intersection rather than a per-member dict lookup.
    food_curies = frozenset(food_types)
    typed_sets = defaultdict(set)

    def evidence_for(ids):
        """The food/extract evidence carried by ``ids``, if any (issues #828, #935)."""
        if food_curies.isdisjoint(ids):
            return frozenset()
        return frozenset(food_types[c] for c in ids if c in food_types)

    def assign(biolink_type, ids, food_evidence):
        """Type a clique as the most preferred of its voted type and any food/extract evidence on it."""
        typed_sets[min({biolink_type} | food_evidence, key=order.index) if food_evidence else biolink_type].add(ids)

    # logging.warning(f"create_typed_sets: eqsets={eqsets}, types=...")
    for equivalent_ids in eqsets:
        # The evidence joins the vote below rather than overriding it. Recomputed per output clique in
        # the split branch, since a split sends the evidence CURIE to only one of the two halves.
        food_evidence = evidence_for(equivalent_ids)
        # logging.warning(f"Processing equivalent_ids={equivalent_ids}.")
        # prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found = False
        for prefix in [PUBCHEMCOMPOUND]:
            if prefix in prefixes and not found:
                # I only want to accept the type if all pubchems agree on it.
                pctypes = set()
                for x in prefixes[prefix]:
                    if x in types:
                        pctypes.add(types[x])
                    else:
                        # logging.warning(f"No type found for {x}, skipping.")
                        pass

                if len(pctypes) == 1:
                    assign(list(pctypes)[0], equivalent_ids, food_evidence)
                    found = True
                elif pctypes == {SMALL_MOLECULE, MOLECULAR_MIXTURE}:
                    # This is a common case (8,178 cases in 2022oct13) which occurs in cases where the InChI for
                    # e.g. water (SMILES: O) and hydron;hydroxide ([H+].[OH-]) are identical, causing them to be
                    # merged. (They may also be merged if we combine two identifiers into a single clique that is
                    # linked to two PubChem entries.)
                    #
                    # The comprehensive solution would be to use SMILES or molecular formula or per-database
                    # type information to split these cliques. Instead, as a temporary solution, we will split
                    # everything we're _sure_ is a biolink:MolecularMixture into a separate clique, and leave all
                    # the other identifiers as a biolink:SmallMolecule.
                    #
                    # First reported in https://github.com/NCATSTranslator/Babel/issues/83
                    molecular_mixture_ids = set()
                    all_other_ids = set()
                    for eq_id in equivalent_ids:
                        if eq_id in types and types[eq_id] == MOLECULAR_MIXTURE:
                            molecular_mixture_ids.add(eq_id)
                        else:
                            all_other_ids.add(eq_id)

                    logging.info(
                        "Found a clique that that contains PUBCHEM types "
                        + "({'biolink:SmallMolecule', 'biolink:MolecularMixture'}). This clique will be split "
                        + f"into a biolink:MolecularMixture ({molecular_mixture_ids}) and "
                        + f"a biolink:SmallMolecule ({all_other_ids})"
                    )
                    # Each half votes on its own evidence: an extract CURIE that lands in the small
                    # molecule half must not retype the mixture half to ComplexMolecularMixture.
                    assign(MOLECULAR_MIXTURE, frozenset(molecular_mixture_ids), evidence_for(molecular_mixture_ids))
                    assign(SMALL_MOLECULE, frozenset(all_other_ids), evidence_for(all_other_ids))
                    found = True
                else:
                    logging.warning(
                        f"An unexpected number of PUBCHEM types found for {equivalent_ids} ({len(pctypes)}): {pctypes}"
                    )
        if not found:
            typecounts = defaultdict(int)
            for eid in equivalent_ids:
                if eid in types:
                    typecounts[types[eid]] += 1
            if len(typecounts) == 0:
                # print('how did we not get any types?')
                # print(equivalent_ids)
                # One thing that happens is that we can have PUBCHEMs that have been deleted, but are still in UNICHEM
                # then the pubchem doesn't get assigned a type, but still ends up in the compendium
                assign(CHEMICAL_ENTITY, equivalent_ids, food_evidence)
            elif len(typecounts) == 1:
                t = list(typecounts.keys())[0]
                assign(t, equivalent_ids, food_evidence)
            else:
                # First attempt is majority vote, and after that by most specific
                otypes = [(-c, order.index(t), t) for t, c in typecounts.items()]
                otypes.sort()
                t = otypes[0][2]
                assign(t, equivalent_ids, food_evidence)
    return typed_sets

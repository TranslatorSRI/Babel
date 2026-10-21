import src.datahandlers.mesh as mesh
import src.datahandlers.ncbitaxon as ncbitaxon
import src.datahandlers.umls as umls
from src.babel_utils import glom, read_identifier_file, write_compendium
from src.categories import ORGANISM_TAXON
from src.metadata.provenance import write_concord_metadata
from src.prefixes import MESH, NCBITAXON, UMLS
from src.util import get_logger

logger = get_logger(__name__)

NCBI_TAXONOMY_SOURCE = {
    "type": "NCBITaxon",
    "name": "NCBI Taxonomy merged.dmp and delnodes.dmp",
    "url": "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz",
}


def write_mesh_ids(outfile):
    # Get the B tree,
    # B01	Eukaryota
    # B02	Archaea
    # B03	Bacteria
    # B04	Viruses
    # B05	Organism Forms
    meshmap = {f"B{str(i).zfill(2)}": ORGANISM_TAXON for i in range(1, 6)}
    # Also add anything from SCR_Chemical, if it doesn't have a tree map
    mesh.write_ids(meshmap, outfile, order=[ORGANISM_TAXON], extra_vocab={"SCR_Organism": ORGANISM_TAXON})


def write_umls_ids(mrsty, outfile, *, mesh_ownership=None):
    """Select the UMLS CUIs the taxon pipeline claims by semantic type.

    ``mesh_ownership`` (the umls_mesh_ownership rule's output) makes a CUI follow its MeSH descriptor
    (umls.apply_mesh_ownership): a CUI another pipeline owns is dropped here and a CUI this pipeline owns is added.
    A plant or virus CUI whose descriptor is an anatomy A-tree or SCR_Chemical record leaves taxon this way
    (https://github.com/NCATSTranslator/Babel/issues/1123).
    """
    # UMLS categories that should be classified as taxa:
    # - A1.1.3: Eukaryote (https://uts.nlm.nih.gov/uts/umls/semantic-network/T204)
    # - A1.1.2: Bacterium (https://uts.nlm.nih.gov/uts/umls/semantic-network/T007)
    # - A1.1.3.3: Plant (https://uts.nlm.nih.gov/uts/umls/semantic-network/T002)
    # - A1.1.3.2: Fungus (https://uts.nlm.nih.gov/uts/umls/semantic-network/T004)
    # - A1.1.3.1.1.3: Fish (https://uts.nlm.nih.gov/uts/umls/semantic-network/T013)
    # - A1.1.3.1.1.2: Bird (https://uts.nlm.nih.gov/uts/umls/semantic-network/T012)
    # - A1.1.4: Virus (https://uts.nlm.nih.gov/uts/umls/semantic-network/T005)
    # - A1.1.3.1.1.4: Mammal (https://uts.nlm.nih.gov/uts/umls/semantic-network/T015)
    # - A1.1.3.1.1.5: Reptile (https://uts.nlm.nih.gov/uts/umls/semantic-network/T014)
    # - A1.1.3.1.1.1: Amphibian (https://uts.nlm.nih.gov/uts/umls/semantic-network/T011)
    # - A1.1.1: Archaeon (https://uts.nlm.nih.gov/uts/umls/semantic-network/T194)
    # - A1.1.3.1: Animal (https://uts.nlm.nih.gov/uts/umls/semantic-network/T008)
    # - A1.1: Organism (https://uts.nlm.nih.gov/uts/umls/semantic-network/T001)
    # - A1.1.3.1.1: Vertebrate (https://uts.nlm.nih.gov/uts/umls/semantic-network/T010)
    #
    # Not clear if these should be included, so left out for now:
    # - A1.1.3.1.1.4.1: Human (https://uts.nlm.nih.gov/uts/umls/semantic-network/T016)
    #   (presumably the human taxon is represented as _Homo sapiens_, which is http://id.nlm.nih.gov/mesh/D006801)

    umlsmap = {
        x: ORGANISM_TAXON
        for x in [
            "A1.1.3",
            "A1.1.2",
            "A1.1.3.3",
            "A1.1.3.2",
            "A1.1.3.1.1.3",
            "A1.1.3.1.1.2",
            "A1.1.4",
            "A1.1.3.1.1.4",
            "A1.1.3.1.1.5",
            "A1.1.3.1.1.1",
            "A1.1.1",
            "A1.1.3.1",
            "A1.1",
            "A1.1.3.1.1",
        ]
    }
    umls.write_umls_ids(
        mrsty,
        umlsmap,
        outfile,
        mesh_ownership=mesh_ownership,
        pipeline="taxon",
    )


def update_obsolete_ncbitaxon_ids(concord_file, taxdump):
    """
    Rewrite a concord file so that it only refers to current NCBITaxon IDs.

    MeSH and UMLS both cross-reference NCBI Taxonomy IDs that have since been merged into another taxon or
    deleted. Left alone, these become unlabeled NCBITaxon identifiers that compete with the current ID for
    the same MeSH or UMLS identifier (we only allow one of each prefix per clique), and whichever concord
    row glom() sees first wins. So we replace merged IDs with the ID they were merged into, and drop rows
    pointing at deleted IDs.

    NCBI often merges two species names into one taxon while MeSH and UMLS keep a record for each name, so
    a remapped row can point at a taxon that another record of the same prefix already links to directly
    (e.g. MESH:C000668355 "Collybia dryophila" -> merged NCBITaxon:71877 -> NCBITaxon:206318, which
    MESH:C000672803 "Gymnopus dryophilus" links to directly). Only one of them can join that clique, and the
    direct link is the better evidence, so we drop the remapped row rather than leave the choice to row order.

    :param concord_file: A concord file (CURIE1\tpredicate\tCURIE2) to rewrite in place.
    :param taxdump: taxdump.tar, from which merged.dmp and delnodes.dmp are read.
    :return: A dict of counts of remapped and dropped rows, for the concord metadata.
    """
    merged, deleted = ncbitaxon.read_obsolete_taxa(taxdump)

    with open(concord_file) as inf:
        rows = [tuple(line.rstrip("\n").split("\t")) for line in inf]

    def ncbitaxon_claim(row):
        """Return (prefix of the non-NCBITaxon CURIE, NCBITaxon CURIE) for a row linking to NCBITaxon, else None."""
        subject, _, obj = row
        if obj.startswith(f"{NCBITAXON}:"):
            return subject.split(":")[0], obj
        if subject.startswith(f"{NCBITAXON}:"):
            return obj.split(":")[0], subject
        return None

    dropped_deleted = sum(1 for row in rows if any(curie in deleted for curie in row))
    rows = [row for row in rows if not any(curie in deleted for curie in row)]
    direct_claims = {ncbitaxon_claim(row) for row in rows if not any(curie in merged for curie in row)}

    remapped = 0
    superseded = 0
    seen = set()
    with open(concord_file, "w") as outf:
        for row in rows:
            new_row = tuple(merged.get(curie, curie) for curie in row)
            if new_row != row:
                if ncbitaxon_claim(new_row) in direct_claims:
                    superseded += 1
                    continue
                remapped += 1
            # Two merged IDs can be remapped to the same current ID, so dedupe after remapping.
            if new_row in seen:
                continue
            seen.add(new_row)
            outf.write("\t".join(new_row) + "\n")

    logger.info(
        f"Updated obsolete NCBITaxon IDs in {concord_file}: remapped {remapped:,} merged, dropped {superseded:,} "
        f"merged that a direct link already claims, and dropped {dropped_deleted:,} deleted."
    )
    return {
        "obsolete_ncbitaxon": {
            "remapped_merged": remapped,
            "dropped_merged_superseded_by_direct": superseded,
            "dropped_deleted": dropped_deleted,
        }
    }


def build_taxon_umls_relationships(mrconso, idfile, taxdump, outfile, metadata_yaml):
    # The keys are UMLS source abbreviations (the MRCONSO SAB column): NCBI Taxonomy is "NCBI" in UMLS, not "NCBITaxon".
    other_prefixes = {"MSH": MESH, "NCBI": NCBITAXON}
    umls.build_sets(mrconso, idfile, outfile, other_prefixes)
    counts = update_obsolete_ncbitaxon_ids(outfile, taxdump)

    write_concord_metadata(
        metadata_yaml,
        name="build_taxon_umls_relationships()",
        description=f"umls.build_sets() using UMLS MRCONSO with prefixes: {other_prefixes}, "
        "with merged NCBITaxon IDs updated and deleted NCBITaxon IDs removed.",
        sources=[{"type": "UMLS", "name": "MRCONSO"}, NCBI_TAXONOMY_SOURCE],
        concord_filename=outfile,
        counts=counts,
    )


def build_relationships(outfile, mesh_ids, taxdump, metadata_yaml):
    regis = mesh.pull_mesh_registry()
    # with open(mesh_ids) as inf:
    # lines = inf.read().strip().split("\n")
    # all_mesh_taxa = set([x.split("\t")[0] for x in lines])
    with open(outfile, "w") as outf:
        for meshid, reg in regis:
            # The mesh->ncbi are in mesh as registration numbers that start with a "tx"
            if reg.startswith("txid"):
                ncbi_id = f"{NCBITAXON}:{reg[4:]}"
                outf.write(f"{meshid}\txref\t{ncbi_id}\n")
        # June 7, 2021.  We have previously found that not all mesh/ncbi links are in the mesh.nt
        # but as of today, it appears that they ARE all in there, so we are not hitting eutil any more (thank goodness)
        # left = list(all_mesh_taxa.difference( set([x[0] for x in regis]) ))
        # eutil.lookup(left)
    counts = update_obsolete_ncbitaxon_ids(outfile, taxdump)

    write_concord_metadata(
        metadata_yaml,
        name="build_relationships()",
        description="Builds relationships between MeSH and NCBI Taxon from the MeSH registry, "
        "with merged NCBITaxon IDs updated and deleted NCBITaxon IDs removed.",
        sources=[
            {
                "type": "MeSH",
                "name": "MeSH Registry",
                "url": "ftp://ftp.nlm.nih.gov/online/mesh/rdf/mesh.nt.gz",
            },
            NCBI_TAXONOMY_SOURCE,
        ],
        concord_filename=outfile,
        counts=counts,
    )


def build_compendia(concordances, metadata_yamls, identifiers, icrdf_filename, mesh_ownership=None):
    """:concordances: a list of files from which to read relationships
    :identifiers: a list of files from which to read identifiers and optional categories
    :mesh_ownership: the umls_mesh_ownership rule's file; concord pairs naming a CUI another pipeline owns are
        skipped (umls.read_cuis_owned_elsewhere). None skips nothing."""
    owned_elsewhere = umls.read_cuis_owned_elsewhere(mesh_ownership, "taxon")
    dicts = {}
    types = {}
    uniques = [NCBITAXON, MESH, UMLS]
    for ifile in identifiers:
        print("loading", ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=uniques)
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print("loading", infile)
        pairs = []
        with open(infile) as inf:
            for line in inf:
                x = line.strip().split("\t")
                if x[0] in owned_elsewhere or x[2] in owned_elsewhere:
                    continue
                pairs.append(set([x[0], x[2]]))
        glom(dicts, pairs, unique_prefixes=uniques)
    gene_sets = set([frozenset(x) for x in dicts.values()])
    baretype = ORGANISM_TAXON.split(":")[-1]
    # We need to use extra_prefixes since UMLS is not listed as an identifier prefix at
    # https://biolink.github.io/biolink-model/docs/OrganismTaxon.html
    write_compendium(metadata_yamls, gene_sets, f"{baretype}.txt", ORGANISM_TAXON, {}, icrdf_filename=icrdf_filename)

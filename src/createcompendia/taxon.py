import logging

import src.datahandlers.mesh as mesh
import src.datahandlers.umls as umls
from src.babel_utils import glom, read_identifier_file, write_compendium
from src.categories import ORGANISM_TAXON
from src.metadata.provenance import write_concord_metadata
from src.prefixes import MESH, NCBITAXON, UMLS
from src.util import LoggingUtil, get_config

logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)


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


def build_taxon_umls_relationships(mrconso, idfile, outfile, metadata_yaml):
    umls.build_sets(
        mrconso, idfile, outfile, {"MSH": MESH, "NCBITaxon": NCBITAXON}, provenance_metadata_yaml=metadata_yaml
    )


def build_relationships(
    outfile, mesh_ids, metadata_yaml, *, mesh_desc_xml, mesh_supp_xml, mesh_labels, mesh_xml_sources
):
    """Write MeSH-NCBITaxon xrefs from the MeSH registry numbers that are NCBI Taxon IDs ("txid...").

    The mesh_* arguments are the files pull_mesh_registry() and mesh_registry_sources() read.
    """
    regis = mesh.pull_mesh_registry(mesh_desc_xml, mesh_supp_xml, mesh_labels)
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

    write_concord_metadata(
        metadata_yaml,
        name="build_relationships()",
        description="Builds relationships between MeSH and NCBI Taxon from the MeSH registry.",
        sources=mesh.mesh_registry_sources(get_config()["mesh_year"], mesh_xml_sources),
        concord_filename=outfile,
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

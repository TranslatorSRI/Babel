"""ChemicalRole compendium: chemical roles from ChEBI, MeSH and UMLS.

Each vocabulary draws its own line between what a chemical is and what it does, and this compendium takes the
"does" side of each line, which the chemicals compendium leaves out:

- ChEBI: everything under CHEBI:50906 (role). ChEBI's role and chemical entity branches are disjoint.
- MeSH: descriptors in D27 (Chemical Actions and Uses) and in no tree another compendium takes
  (mesh.MESH_ENTITY_TREES), e.g. "Antidepressive Agents".
- UMLS: substance-typed CUIs whose MeSH atoms all point at those descriptors. Most are linked to their descriptor
  by a UMLS<->MeSH concord; CUIs that only carry a descriptor's entry terms stay UMLS-only cliques.

Wikidata's MeSH<->ChEBI cross-references join the MeSH and ChEBI roles. Biolink's ChemicalRole lists only CHEBI
as an id_prefix, so MESH and UMLS are extra prefixes: CHEBI is preferred when a clique has one, and cliques
without a ChEBI role are kept with a MeSH or UMLS identifier.
"""

from collections import defaultdict

import src.datahandlers.mesh as mesh
import src.datahandlers.umls as umls
from src.babel_utils import glom, read_identifier_file, write_compendium
from src.categories import CHEMICAL_ROLE
from src.createcompendia.chemicals import get_wikidata_mesh_chebi_pairs
from src.metadata.provenance import write_concord_metadata, write_metadata
from src.prefixes import CHEBI, MESH, UMLS
from src.ubergraph import UberGraph

CHEBI_ROLE = f"{CHEBI}:50906"

# UMLS semantic type tree numbers. A MeSH-role CUI is only a ChemicalRole if UMLS calls it a substance: the others
# are effects ("Serotonin Effect", Physiologic Function), findings or devices that MeSH files as entry terms under
# the agent class, and process or leftover_umls takes them. Protein's semantic types stay with protein, matching
# the trees chemicals.write_umls_ids() excludes.
UMLS_SUBSTANCE_TREE = "A1.4"
UMLS_PROTEIN_TREES = {
    "A1.4.1.2.1.7",  # Amino Acid, Peptide, or Protein
    "A1.4.1.1.3.6",  # Receptor
    "A1.4.1.1.3.3",  # Enzyme
}


def write_chebi_ids(outfile, metadata_yaml):
    """Write every CHEBI descendant of CHEBI:50906 (including the root) as a ChemicalRole identifier."""
    descendants = UberGraph().get_subclasses_of(CHEBI_ROLE)
    role_ids = sorted({d["descendent"] for d in descendants if d["descendent"].startswith(f"{CHEBI}:")})
    if not role_ids:
        raise RuntimeError(f"UberGraph returned no CHEBI descendants of {CHEBI_ROLE}")
    with open(outfile, "w") as idfile:
        for curie in role_ids:
            idfile.write(f"{curie}\t{CHEMICAL_ROLE}\n")

    write_metadata(
        metadata_yaml,
        typ="transform",
        name="ChEBI roles",
        description=f"All ChEBI descendants of {CHEBI_ROLE} (role), retrieved from UberGraph",
        sources=[{"type": "UberGraph", "name": "ChEBI via UberGraph", "url": "https://ubergraph.apps.renci.org/"}],
        counts={"count_ids": len(role_ids)},
    )


def write_mesh_ids(outfile):
    """Write MeSH descriptors in D27 that are in no tree another compendium takes (mesh.MESH_ENTITY_TREES)."""
    meshmap = {tree: "EXCLUDE" for tree in mesh.MESH_ENTITY_TREES}
    meshmap[mesh.MESH_CHEMICAL_ACTIONS_AND_USES_TREE] = CHEMICAL_ROLE
    mesh.write_ids(meshmap, outfile, order=["EXCLUDE", CHEMICAL_ROLE])


def write_umls_ids(mrconso, mrsty, mesh_ids_file, outfile):
    """Write the substance-typed, non-protein CUIs whose MeSH atoms all point at this compendium's MeSH ids."""
    _, mesh_types = read_identifier_file(mesh_ids_file)
    mesh_role_cuis = umls.get_cuis_mapped_only_to_mesh(mrconso, set(mesh_types))
    semantic_type_trees = defaultdict(set)
    with open(mrsty) as inf:
        for line in inf:
            x = line.split("|")
            if x[0] in mesh_role_cuis:
                semantic_type_trees[x[0]].add(x[2])
    role_cuis = {
        cui
        for cui in mesh_role_cuis
        if any(t.startswith(UMLS_SUBSTANCE_TREE) for t in semantic_type_trees[cui])
        and not semantic_type_trees[cui] & UMLS_PROTEIN_TREES
    }
    with open(outfile, "w") as idfile:
        for cui in sorted(role_cuis):
            idfile.write(f"{UMLS}:{cui}\t{CHEMICAL_ROLE}\n")


def build_umls_relationships(mrconso, umls_ids_file, outfile, metadata_yaml):
    umls.build_sets(mrconso, umls_ids_file, outfile, {"MSH": MESH}, provenance_metadata_yaml=metadata_yaml)


def build_wikidata_relationships(chebi_ids_file, mesh_ids_file, outfile, metadata_yaml):
    """Write Wikidata MeSH<->ChEBI cross-references where both sides are in this compendium's id lists."""
    known_ids = set(read_identifier_file(chebi_ids_file)[1]) | set(read_identifier_file(mesh_ids_file)[1])
    with open(outfile, "w") as outf:
        for mesh_id, chebi_id in get_wikidata_mesh_chebi_pairs():
            if mesh_id in known_ids and chebi_id in known_ids:
                outf.write(f"{mesh_id}\txref\t{chebi_id}\n")

    write_concord_metadata(
        metadata_yaml,
        name="chemical_role.build_wikidata_relationships()",
        sources=[{"type": "Wikidata", "name": "Wikidata SPARQL query"}],
        description="Wikidata entities with both a MeSH and a ChEBI ID, limited to the ChemicalRole id lists.",
        concord_filename=outfile,
    )


def build_compendia(concordances, metadata_yamls, identifiers, icrdf_filename):
    """Build ChemicalRole.txt from the id lists and concords."""
    dicts = {}
    uniques = [CHEBI, MESH, UMLS]
    for ifile in identifiers:
        print("loading", ifile)
        new_identifiers, _ = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=uniques)
    for infile in concordances:
        print("loading", infile)
        pairs = []
        with open(infile) as inf:
            for line in inf:
                x = line.strip().split("\t")
                pairs.append({x[0], x[2]})
        glom(dicts, pairs, unique_prefixes=uniques)
    idsets = set([frozenset(x) for x in dicts.values()])
    baretype = CHEMICAL_ROLE.split(":")[-1]
    write_compendium(
        metadata_yamls,
        idsets,
        f"{baretype}.txt",
        CHEMICAL_ROLE,
        {},
        extra_prefixes=[MESH, UMLS],
        icrdf_filename=icrdf_filename,
    )

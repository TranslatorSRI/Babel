import warnings
from collections import defaultdict

import pyoxigraph

from src.babel_utils import make_local_name, pull_via_ftp
from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT
from src.prefixes import MESH

MESH_IRI_PREFIX = "http://id.nlm.nih.gov/mesh/"

# The D tree splits into chemicals classified by what they are (D01–D26) and by what they do (D27, Chemical
# Actions and Uses). A D27 descriptor is a chemical role, e.g. "Antidepressive Agents", unless it is also in a tree
# another compendium takes (MESH_ENTITY_TREES): MeSH files specific substances under D27 beside their action class
# (Cefiderocol, High Fructose Corn Syrup), and a few D27 descriptors are also diseases ("Metabolic Side Effects of
# Drugs and Substances", C25). Querying a tree number MeSH doesn't use returns nothing, so the ranges are generous.
MESH_CHEMICAL_TREES = [f"D{i:02}" for i in range(1, 27)]
MESH_CHEMICAL_ACTIONS_AND_USES_TREE = "D27"
MESH_ENTITY_TREES = (
    [f"A{i:02}" for i in range(1, 22)]  # anatomy
    + [f"B{i:02}" for i in range(1, 6)]  # organisms
    + [f"C{i:02}" for i in range(1, 27)]  # diseases
    + MESH_CHEMICAL_TREES  # chemicals and proteins
)


def get_mesh_id_from_iri(iri) -> str:
    """Extract a MeSH ID from a pyoxigraph IRI (e.g. <http://id.nlm.nih.gov/mesh/D009243>).

    Raises ValueError if the input is not a MeSH concept IRI.
    """
    s = str(iri)
    if s.startswith("<") and s.endswith(">"):
        s = s[1:-1]

    if not s.startswith(MESH_IRI_PREFIX):
        raise ValueError(f"Expected a MeSH IRI like <http://id.nlm.nih.gov/mesh/D009243>, got: '{s!r}'")
    return s[len(MESH_IRI_PREFIX) :]


def pull_mesh():
    pull_via_ftp("ftp.nlm.nih.gov", "/online/mesh/rdf", "mesh.nt.gz", decompress_data=True, outfilename="MESH/mesh.nt")


class Mesh:
    """Load the mesh rdf file for querying"""

    # Tracks how many Mesh instances currently hold mesh.nt in memory.
    # mesh.nt occupies ~2 GB in pyoxigraph's in-memory store; running two at the
    # same time on a 16 GB machine causes severe swapping or OOM failures.
    _active_instances: int = 0

    def __init__(self):
        if Mesh._active_instances > 0:
            warnings.warn(
                f"{Mesh._active_instances} Mesh instance(s) are already loaded in this "
                "process. Each instance holds mesh.nt in an in-memory pyoxigraph store "
                "(~2 GB); running multiple simultaneously can exhaust RAM on machines "
                "with ≤16 GB and cause severe slowdowns or OOM failures.",
                ResourceWarning,
                stacklevel=2,
            )
        ifname = make_local_name("mesh.nt", subpath="MESH")
        from datetime import datetime as dt

        print("loading mesh.nt")
        start = dt.now()
        self.m = pyoxigraph.Store()
        with open(ifname, "rb") as inf:
            self.m.bulk_load(input=inf, format=pyoxigraph.RdfFormat.N_TRIPLES)
        end = dt.now()
        print("loading complete")
        print(f"took {end - start}")
        Mesh._active_instances += 1  # only after successful load

    def __del__(self):
        if hasattr(self, "m"):  # only if __init__ completed successfully
            Mesh._active_instances = max(0, Mesh._active_instances - 1)

    def get_terms_in_tree(self, top_treenum):
        s = f"""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term
                WHERE {{ ?term meshv:treeNumber ?treenum .
                         ?treenum meshv:parentTreeNumber* mesh:{top_treenum}
                }}
                ORDER BY ?term
        """
        qres = self.m.query(s)
        meshes = []
        for row in qres:
            meshes.append(f"{MESH}:{get_mesh_id_from_iri(row['term'])}")
        return meshes

    def get_scr_terms_mapped_to_trees(self, top_treenums):
        """Get Supplementary Concept Record terms that are mapped to descriptors under any of the given tree numbers.

        SCR terms don't have tree numbers themselves, but they have meshv:mappedTo and/or
        meshv:preferredMappedTo relationships to descriptor terms that do. This method finds
        SCR terms whose mapped descriptors fall under the specified trees.

        Returns an empty set if top_treenums is empty."""
        if not top_treenums:
            return set()
        values_clause = " ".join(f"mesh:{t}" for t in top_treenums)
        s = f"""PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term
                WHERE {{ VALUES ?mappingPred {{ meshv:mappedTo meshv:preferredMappedTo }}
                         VALUES ?topTree {{ {values_clause} }}
                         ?term ?mappingPred ?descriptor .
                         ?descriptor meshv:treeNumber ?treenum .
                         ?treenum meshv:parentTreeNumber* ?topTree
                }}
                ORDER BY ?term
        """
        terms = set()
        for row in self.m.query(s):
            terms.add(f"{MESH}:{get_mesh_id_from_iri(row['term'])}")
        return terms

    def get_terms_with_type(self, termtype):
        s = f"""  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rdfns: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term
                WHERE {{ ?term rdfns:type meshv:{termtype} }}
                ORDER BY ?term
        """
        qres = self.m.query(s)
        meshes = []
        for row in qres:
            meshes.append(f"{MESH}:{get_mesh_id_from_iri(row['term'])}")
        return meshes

    def get_registry(self):
        """Based on stuff like
        <http://id.nlm.nih.gov/mesh/M0391958>	<http://id.nlm.nih.gov/mesh/vocab#registryNumber>	"8A1O1M485B" .
        <http://id.nlm.nih.gov/mesh/D000068877>	<http://id.nlm.nih.gov/mesh/vocab#preferredConcept>	<http://id.nlm.nih.gov/mesh/M0391958> ."""
        s = """   PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term ?reg
                WHERE {{ ?mthing meshv:registryNumber ?reg . 
                         ?term meshv:preferredConcept ?mthing }}
                ORDER BY ?term
        """
        qres = self.m.query(s)
        res = []
        for row in qres:
            label = str(row["reg"])[1:-1]  # strip quotes
            if label == "0":
                # wtf is this dumbness?
                continue
            meshid = f"{MESH}:{get_mesh_id_from_iri(row['term'])}"
            res.append((meshid, label))
        return res

    def get_tree_numbers(self, mesh_id: str) -> list[str]:
        """Return the tree-number strings for a MeSH descriptor.

        Accepts "D009243" or "MESH:D009243". Returns strings like
        ["D08.211.589", "D03.633.100.759.646.138.694"] sorted alphabetically.
        Returns an empty list for SCR terms, which have no tree numbers of their own.
        """
        raw_id = mesh_id.split(":")[-1]
        s = f"""PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?treenum
                WHERE {{ mesh:{raw_id} meshv:treeNumber ?treenum }}
                ORDER BY ?treenum
        """
        return [get_mesh_id_from_iri(row["treenum"]) for row in self.m.query(s)]

    def get_scr_mappings(self, scr_id: str) -> list[dict]:
        """Return mapping info for an SCR (Supplementary Concept Record) term.

        Accepts "C100843" or "MESH:C100843". Returns one dict per
        (predicate, descriptor) pair with keys:
          - "predicate":    "mappedTo" or "preferredMappedTo"
          - "descriptor":   CURIE, e.g. "MESH:D004338"
          - "label":        descriptor rdfs:label, or "" if absent
          - "tree_numbers": sorted list of tree-number strings for that descriptor
        """
        raw_id = scr_id.split(":")[-1]
        s = f"""PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                SELECT DISTINCT ?mappingPred ?descriptor ?descLabel ?treenum
                WHERE {{
                    VALUES ?mappingPred {{ meshv:mappedTo meshv:preferredMappedTo }}
                    mesh:{raw_id} ?mappingPred ?descriptor .
                    OPTIONAL {{ ?descriptor rdfs:label ?descLabel }}
                    OPTIONAL {{ ?descriptor meshv:treeNumber ?treenum }}
                }}
                ORDER BY ?mappingPred ?descriptor ?treenum
        """
        mappings: dict[tuple, dict] = {}
        for row in self.m.query(s):
            pred = str(row["mappingPred"])[:-1].split("#")[-1]
            desc_id = f"{MESH}:{get_mesh_id_from_iri(row['descriptor'])}"
            key = (pred, desc_id)
            if key not in mappings:
                label = ""
                try:
                    desc_label = row["descLabel"]
                    if desc_label is not None:
                        label = desc_label.value
                except (KeyError, AttributeError):
                    pass
                mappings[key] = {"predicate": pred, "descriptor": desc_id, "label": label, "tree_numbers": []}
            treenum_val = row["treenum"]
            if treenum_val is not None:
                tree_id = get_mesh_id_from_iri(treenum_val)
                if tree_id not in mappings[key]["tree_numbers"]:
                    mappings[key]["tree_numbers"].append(tree_id)
        for info in mappings.values():
            info["tree_numbers"].sort()
        return list(mappings.values())

    def print_tree_labels(self):
        s = """   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?label ?treenum
                WHERE { ?term meshv:treeNumber ?treenum .
                    ?term rdfs:label ?label
                }
                ORDER BY ?treenum
        """
        qres = self.m.query(s)
        with open("mesh_tree_labels", "w", encoding="utf8") as outf:
            for row in qres:
                outf.write(f"{get_mesh_id_from_iri(row['treenum'])}\t{row['label'].value}\n")

    def pull_mesh_labels(self):
        s = """   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term ?label
                WHERE { ?term rdfs:label ?label }
                ORDER BY ?term
        """
        ofname = make_local_name("labels", subpath="MESH")
        qres = self.m.query(s)
        with open(ofname, "w", encoding="utf8") as outf:
            for row in qres:
                outf.write(f"{MESH}:{get_mesh_id_from_iri(row['term'])}\t{row['label'].value}\n")


def pull_mesh_labels():
    m = Mesh()
    m.pull_mesh_labels()


def pull_mesh_registry():
    m = Mesh()
    return m.get_registry()


def write_ids(
    meshmap,
    outfile,
    order=[CELLULAR_COMPONENT, CELL, ANATOMICAL_ENTITY],
    extra_vocab={},
    scr_exclude_trees=None,
    scr_include_trees=None,
):
    """Write the mesh identifiers from a particular set of hierarchies to an output directory.
    This might be a mixed list of types (for instance anatomy and cell).  Also, the same term
    may appear in multiple trees, perhaps with different types.

    scr_exclude_trees: optional list of tree numbers. SCR terms (from extra_vocab) that are
    mapped to descriptors under these trees will be marked as EXCLUDE.
    scr_include_trees: optional list of tree numbers. If set, only SCR terms (from extra_vocab)
    that are mapped to descriptors under these trees will be kept; all other SCR terms will be
    removed. Cannot be used together with scr_exclude_trees."""
    if scr_exclude_trees is not None and scr_include_trees is not None:
        raise ValueError("scr_exclude_trees and scr_include_trees cannot both be set")
    m = Mesh()
    terms2type = defaultdict(set)
    for treenum, category in meshmap.items():
        mesh_terms = m.get_terms_in_tree(treenum)
        for mt in mesh_terms:
            terms2type[mt].add(category)
    if scr_include_trees:
        # Only add extra_vocab terms that are mapped to descriptors under the included trees.
        # This is the inverse of scr_exclude_trees: instead of adding all SCR terms and then
        # marking some as EXCLUDE, we only add SCR terms that match the included trees.
        included_scr_terms = m.get_scr_terms_mapped_to_trees(scr_include_trees)
        for k, v in extra_vocab.items():
            mesh_terms = m.get_terms_with_type(k)
            for mt in mesh_terms:
                if mt in included_scr_terms:
                    terms2type[mt].add(v)
    else:
        for k, v in extra_vocab.items():
            mesh_terms = m.get_terms_with_type(k)
            for mt in mesh_terms:
                terms2type[mt].add(v)
        if scr_exclude_trees:
            excluded_scr_terms = m.get_scr_terms_mapped_to_trees(scr_exclude_trees)
            for mt in excluded_scr_terms:
                terms2type[mt].add("EXCLUDE")
    with open(outfile, "w") as idfile:
        for term, typeset in terms2type.items():
            list_typeset = list(typeset)
            list_typeset.sort(key=lambda x: order.index(x))
            if list_typeset[0] == "EXCLUDE":
                continue
            idfile.write(f"{term}\t{list_typeset[0]}\n")


#    ifname = make_local_name('mesh.nt', subpath='MESH')
#    ofname = make_local_name('labels', subpath='MESH')
#    badlines = 0
#    with open(ofname, 'w') as outf, open(ifname,'r') as data:
#        for line in data:
#            if line.startswith('#'):
#                continue
#            triple = line[:-1].strip().split('\t')
#            try:
#                s,v,o = triple
#                if v == '<http://www.w3.org/2000/01/rdf-schema#label>':
#                    meshid = s[:-1].split('/')[-1]
#                    label = o.strip().split('"')[1]
#                    outf.write(f'MESH:{meshid}\t{label}\n')
#            except ValueError:
#                badlines += 1
#    print(f'{badlines} lines were bad')

if __name__ == "__main__":
    mesh = Mesh()
    mesh.print_tree_labels()
    # mesh.pull_mesh_labels()

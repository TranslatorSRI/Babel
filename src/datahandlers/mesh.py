import ftplib
import gzip
import hashlib
import os
import warnings
from collections import defaultdict
from io import BytesIO
from xml.etree import ElementTree

import pyoxigraph

from src.babel_utils import make_local_name, pull_via_ftp
from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT
from src.prefixes import MESH
from src.util import ensure_parent_dir, get_logger

logger = get_logger(__name__)

MESH_IRI_PREFIX = "http://id.nlm.nih.gov/mesh/"


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


MESH_FTP_SITE = "ftp.nlm.nih.gov"
MESH_RDF_DIR = "/online/mesh/rdf"


def mesh_rdf_location(mesh_year) -> tuple[str, str]:
    """Return the (FTP directory, filename) of the MeSH RDF N-Triples dump for ``mesh_year``.

    NLM publishes each MeSH release in its own year directory (e.g. /online/mesh/rdf/2026/mesh2026.nt.gz).
    The top-level /online/mesh/rdf/mesh.nt.gz stopped being updated after MeSH 2024, so we never use it.
    The per-year directory also has a mesh{year}-dups.nt, which is the same data before deduplication;
    mesh{year}.nt is the "combined, sorted, unique" file and is all we need.
    """
    return f"{MESH_RDF_DIR}/{mesh_year}", f"mesh{mesh_year}.nt.gz"


# The current MeSH year's XML lives in MESH_XML_CURRENT_DIR (updated daily); once a year is superseded, its
# final XML is archived under /online/mesh/{year}/xmlmesh. Filenames carry the year in both places.
MESH_XML_CURRENT_DIR = "/online/mesh/MESH_FILES/xmlmesh"

# (file prefix, XML record element, record UI element). Each is published as {prefix}{year}.gz and saved
# as babel_downloads/MESH/{prefix}.xml.gz.
MESH_XML_FILES = [
    ("desc", "DescriptorRecord", "DescriptorUI"),
    ("supp", "SupplementalRecord", "SupplementalRecordUI"),
]


def fetch_mesh_xml_dir(mesh_year) -> str:
    """Return the FTP directory holding desc{year}.gz and supp{year}.gz for ``mesh_year``.

    Lists MESH_XML_CURRENT_DIR on the NLM FTP site to see whether ``mesh_year`` is still the current year.
    """
    ftp = ftplib.FTP(MESH_FTP_SITE)
    try:
        ftp.login()
        current_files = ftp.nlst(MESH_XML_CURRENT_DIR)
        ftp.quit()
    finally:
        ftp.close()
    if any(os.path.basename(f) == f"desc{mesh_year}.gz" for f in current_files):
        return MESH_XML_CURRENT_DIR
    return f"/online/mesh/{mesh_year}/xmlmesh"


def normalize_mesh_nt(gz_path, out_path, mesh_year):
    """Decompress a per-year MeSH dump into out_path, rewriting its year-versioned IRIs to unversioned ones.

    The per-year dumps name records as <http://id.nlm.nih.gov/mesh/2026/D000001>, while everything that
    reads mesh.nt (the SPARQL in Mesh, get_mesh_id_from_iri()) expects <http://id.nlm.nih.gov/mesh/D000001>,
    which is how the old top-level dump and the id.nlm.nih.gov "current" IRIs name them. The vocabulary
    namespace (<http://id.nlm.nih.gov/mesh/vocab#>) is not versioned and passes through untouched.

    Raises ValueError if no line carries the expected versioned prefix: that means the dump is not for
    mesh_year or NLM has changed its IRI scheme, and the file would otherwise be silently unusable.
    """
    versioned = f"<{MESH_IRI_PREFIX}{mesh_year}/"
    unversioned = f"<{MESH_IRI_PREFIX}"
    rewritten_lines = 0
    with gzip.open(gz_path, "rt", encoding="utf-8") as inf, open(out_path, "w", encoding="utf-8") as outf:
        for line in inf:
            if versioned in line:
                line = line.replace(versioned, unversioned)
                rewritten_lines += 1
            outf.write(line)
    if rewritten_lines == 0:
        raise ValueError(f"{gz_path} contains no {versioned}...> IRIs; is it really the MeSH {mesh_year} RDF dump?")
    return rewritten_lines


def fetch_mesh_nt(mesh_year, out_path):
    """Download the MeSH RDF dump for ``mesh_year``, check it against NLM's SHA-1 and normalize it into out_path.

    The SHA-1 is computed while the dump downloads, so the ~120 MB file is read only once more, to decompress
    it (see normalize_mesh_nt()). Fails if NLM has no dump for that year; there is deliberately no fallback to
    the stale top-level file. The compressed download is kept beside out_path only until it is normalized.
    """
    ftpdir, ftpfile = mesh_rdf_location(mesh_year)
    url = f"ftp://{MESH_FTP_SITE}{ftpdir}/{ftpfile}"
    gz_path = f"{out_path}.gz"
    ensure_parent_dir(gz_path)
    sha1 = hashlib.sha1()
    sha1_file = BytesIO()
    ftp = ftplib.FTP(MESH_FTP_SITE)
    try:
        ftp.login()
        try:
            ftp.cwd(ftpdir)
            with open(gz_path, "wb") as outf:

                def write_chunk(chunk):
                    sha1.update(chunk)
                    outf.write(chunk)

                ftp.retrbinary(f"RETR {ftpfile}", write_chunk)
        except ftplib.error_perm as e:
            raise RuntimeError(
                f"Could not download MeSH {mesh_year} from {url} ({e}). "
                "Check that mesh_year in config.yaml names a year NLM has published."
            ) from e
        try:
            ftp.retrbinary(f"RETR {ftpfile}.sha1", sha1_file.write)
        except ftplib.error_perm as e:
            # The dump itself downloaded, so the year is published: NLM has moved or renamed its checksum file.
            raise RuntimeError(f"Downloaded {url}, but could not download its checksum file {url}.sha1 ({e}).") from e
        ftp.quit()
    finally:
        ftp.close()

    # NLM's .sha1 files read "<hash>  <filename>".
    expected_sha1 = sha1_file.getvalue().decode().split()[0]
    if sha1.hexdigest() != expected_sha1:
        os.remove(gz_path)
        raise RuntimeError(f"{url} downloaded with SHA-1 {sha1.hexdigest()}, but NLM publishes {expected_sha1}.")

    normalize_mesh_nt(gz_path, out_path, mesh_year)
    os.remove(gz_path)


def pull_mesh(mesh_year):
    """Download the MeSH RDF dump for ``mesh_year`` to babel_downloads/MESH/mesh.nt (see fetch_mesh_nt())."""
    fetch_mesh_nt(mesh_year, make_local_name("mesh.nt", subpath="MESH"))


def pull_mesh_xml(mesh_year, xml_sources_file):
    """Download the MeSH XML descriptor and SCR files for ``mesh_year`` to babel_downloads/MESH.

    Only needed for registry numbers (see pull_mesh_registry()). Fails if NLM has no XML for that year.
    Writes the URL of each file to xml_sources_file, one per line, for mesh_registry_sources(): whether
    they came from the daily-updated current directory or a year's archive is only known at download time.
    """
    xml_dir = fetch_mesh_xml_dir(mesh_year)
    urls = []
    for prefix, _, _ in MESH_XML_FILES:
        ftpfile = f"{prefix}{mesh_year}.gz"
        url = f"ftp://{MESH_FTP_SITE}{xml_dir}/{ftpfile}"
        try:
            pull_via_ftp(MESH_FTP_SITE, xml_dir, ftpfile, outfilename=f"MESH/{prefix}.xml.gz")
        except ftplib.error_perm as e:
            raise RuntimeError(
                f"Could not download MeSH {mesh_year} XML from {url} ({e}). "
                "Check that mesh_year in config.yaml names a year NLM has published."
            ) from e
        urls.append(url)
    with open(xml_sources_file, "w", encoding="utf-8") as outf:
        outf.writelines(f"{url}\n" for url in urls)


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


def read_mesh_xml_registry(xml_gz_path, record_tag, ui_tag):
    """Yield (MESH:ID, registry number) for each record's preferred concept in a gzipped MeSH XML file.

    record_tag/ui_tag are DescriptorRecord/DescriptorUI for desc{year}.gz and
    SupplementalRecord/SupplementalRecordUI for supp{year}.gz. Only the preferred concept's
    RegistryNumber is used (not RelatedRegistryNumber), and MeSH's "0" placeholder for
    "no registry number" is skipped.
    """
    with gzip.open(xml_gz_path, "rb") as inf:
        for _, elem in ElementTree.iterparse(inf, events=("end",)):
            if elem.tag != record_tag:
                continue
            ui = elem.findtext(ui_tag)
            if not ui:
                raise ValueError(f"{record_tag} without a {ui_tag} in {xml_gz_path}")
            for concept in elem.iterfind("ConceptList/Concept[@PreferredConceptYN='Y']"):
                for reg in concept.iterfind("RegistryNumberList/RegistryNumber"):
                    regnum = (reg.text or "").strip()
                    if regnum and regnum != "0":
                        yield f"{MESH}:{ui}", regnum
            elem.clear()


def pull_mesh_registry(desc_xml, supp_xml, mesh_labels):
    """Return sorted, unique (MESH:ID, registry number) pairs for the MeSH descriptors and SCRs in the RDF dump.

    Registry numbers are CAS numbers, UNIIs, EC numbers and NCBI Taxon IDs ("txid..."). They come from the
    MeSH XML (desc_xml and supp_xml, i.e. desc{year}.gz and supp{year}.gz) rather than the RDF dump because
    NLM's per-year RDF dumps stopped including meshv:registryNumber from MeSH 2025 onward.

    The current year's XML is updated daily while the RDF dump is a snapshot, so the XML can name records the
    dump does not have (100 SCRs on 2026-10-21). Pairs are kept only for records with a label in mesh_labels
    (babel_downloads/MESH/labels, read from the dump), so that a registry number never brings in a MeSH ID
    that has no label or type anywhere else in the build: a concord alone can put a CURIE into a compendium.

    Raises ValueError if none are found, since every caller (the chemical CAS/UNII concords and the
    MeSH-NCBITaxon concord) would otherwise silently come out empty.
    """
    with open(mesh_labels, encoding="utf-8") as inf:
        labeled_ids = {line.split("\t", 1)[0] for line in inf}
    registry = set()
    not_in_dump = set()
    for (_, record_tag, ui_tag), xml_gz_path in zip(MESH_XML_FILES, (desc_xml, supp_xml), strict=True):
        for meshid, regnum in read_mesh_xml_registry(xml_gz_path, record_tag, ui_tag):
            if meshid in labeled_ids:
                registry.add((meshid, regnum))
            else:
                not_in_dump.add(meshid)
    if not_in_dump:
        logger.info(
            f"Skipped registry numbers for {len(not_in_dump):,} MeSH records in the XML but not in {mesh_labels}, "
            f"e.g. {', '.join(sorted(not_in_dump)[:5])}."
        )
    if not registry:
        raise ValueError("No registry numbers found in the MeSH XML files; has the MeSH XML format changed?")
    return sorted(registry)


def mesh_registry_sources(mesh_year, xml_sources_file) -> list[dict]:
    """Return write_concord_metadata() sources for a concord built from pull_mesh_registry().

    The registry numbers come from the XML files named in xml_sources_file (written by pull_mesh_xml()), and
    are filtered to the records in the RDF dump for ``mesh_year``.
    """
    with open(xml_sources_file, encoding="utf-8") as inf:
        xml_urls = [line.strip() for line in inf if line.strip()]
    rdf_dir, rdf_file = mesh_rdf_location(mesh_year)
    return [{"type": "MeSH", "name": f"MeSH {mesh_year} XML (registry numbers)", "url": url} for url in xml_urls] + [
        {
            "type": "MeSH",
            "name": f"MeSH {mesh_year} RDF (records kept)",
            "url": f"ftp://{MESH_FTP_SITE}{rdf_dir}/{rdf_file}",
        }
    ]


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

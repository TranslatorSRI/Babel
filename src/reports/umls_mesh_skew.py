"""Report how many MeSH codes in UMLS have no record in the downloaded MeSH dump.

UMLS and MeSH are downloaded separately (umls_version and mesh_year in config.yaml). When the MeSH
dump is older than the MeSH release UMLS was built from, UMLS carries MSH codes -- mostly new
Supplementary Concept Records -- that MeSH itself knows nothing about. Those codes still reach the
compendia through UMLS, but without a MeSH label, type or tree position. This report makes that
version skew visible instead of leaving it to be discovered one missing chemical at a time.
"""

import json
from collections import Counter, defaultdict

from src.prefixes import MESH

MESH_IRI_PREFIX = "<http://id.nlm.nih.gov/mesh/"
RDFS_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"


def read_labeled_mesh_ids(mesh_nt_path) -> set[str]:
    """Return the bare MeSH IDs (e.g. D012711) that have an rdfs:label in a MeSH N-Triples dump.

    Every MeSH descriptor, SCR and qualifier has a label, so this is the set of records the dump knows
    about. Parsed line by line rather than through pyoxigraph: we only need one predicate, and this
    avoids holding the ~2 GB store in memory.
    """
    ids = set()
    with open(mesh_nt_path, encoding="utf-8") as inf:
        for line in inf:
            # N-Triples allows tabs or spaces between terms; NLM has used both.
            parts = line.split(maxsplit=2)
            if len(parts) < 3 or parts[1] != RDFS_LABEL or not parts[0].startswith(MESH_IRI_PREFIX):
                continue
            ids.add(parts[0][len(MESH_IRI_PREFIX) : -1])
    return ids


def read_umls_mesh_codes(mrconso_path) -> dict[str, set[str]]:
    """Return {MSH CODE: set of CUIs} for every MSH row in MRCONSO.RRF."""
    code_to_cuis = defaultdict(set)
    with open(mrconso_path, encoding="utf-8") as inf:
        for line in inf:
            x = line.split("|")
            # Column 12 (SAB) is the source, column 14 (CODE) is the source's own identifier.
            if x[11] == "MSH":
                code_to_cuis[x[13]].add(x[0])
    return code_to_cuis


def generate_umls_mesh_skew_report(mrconso_path, mesh_nt_path, umls_version, mesh_year, outfile, sample_size=50):
    """Write a JSON report of UMLS MSH codes that are absent from the MeSH dump.

    The report is informational: version skew is expected to some degree (UMLS and MeSH release on
    different schedules), so a large count is a prompt to check mesh_year, not a build failure.
    """
    mesh_ids = read_labeled_mesh_ids(mesh_nt_path)
    code_to_cuis = read_umls_mesh_codes(mrconso_path)

    missing = sorted(code for code in code_to_cuis if code not in mesh_ids)
    missing_set = set(missing)

    cui_to_codes = defaultdict(set)
    for code, cuis in code_to_cuis.items():
        for cui in cuis:
            cui_to_codes[cui].add(code)
    cuis_with_missing = {cui for cui, codes in cui_to_codes.items() if codes & missing_set}
    cuis_only_missing = {cui for cui in cuis_with_missing if cui_to_codes[cui] <= missing_set}

    report = {
        "umls_version": str(umls_version),
        "mesh_year": str(mesh_year),
        "mesh_records_in_dump": len(mesh_ids),
        "umls_msh_codes": len(code_to_cuis),
        "umls_msh_codes_missing_from_mesh": len(missing),
        # D = descriptor, C = Supplementary Concept Record, Q = qualifier, U = the MeSH category headings
        # UMLS adds itself (e.g. U000002 "Anatomy (MeSH Category)"), which are never in MeSH.
        "missing_by_record_type": dict(sorted(Counter(code[0] for code in missing).items())),
        "umls_cuis_with_msh_codes": len(cui_to_codes),
        "umls_cuis_with_any_missing_msh_code": len(cuis_with_missing),
        "umls_cuis_with_only_missing_msh_codes": len(cuis_only_missing),
        "missing_sample": [f"{MESH}:{code}" for code in missing[:sample_size]],
    }
    with open(outfile, "w", encoding="utf-8") as outf:
        json.dump(report, outf, indent=2)
        outf.write("\n")
    return report

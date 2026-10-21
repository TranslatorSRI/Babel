"""Report how many MeSH codes in UMLS have no record in the downloaded MeSH dump.

UMLS and MeSH are downloaded separately (umls_version and mesh_year in config.yaml). When the MeSH
dump is older than the MeSH release UMLS was built from, UMLS carries MSH codes -- mostly new
Supplementary Concept Records -- that MeSH itself knows nothing about. Those codes still reach the
compendia through UMLS, but without a MeSH label, type or tree position. This report makes that
version skew visible instead of leaving it to be discovered one missing chemical at a time.
"""

import json
from collections import Counter, defaultdict

from src.datahandlers.mesh import MESH_IRI_PREFIX, get_mesh_id_from_iri
from src.datahandlers.umls import check_mrconso_line
from src.prefixes import MESH

RDFS_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"

# The first letter of the "(MeSH Category)" headings UMLS adds to its MSH source (U000002 "Anatomy (MeSH
# Category)"). They are not MeSH records, so they are missing from every MeSH dump whatever its year.
MESH_CATEGORY_RECORD_TYPE = "U"


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
            if len(parts) < 3 or parts[1] != RDFS_LABEL or not parts[0].startswith(f"<{MESH_IRI_PREFIX}"):
                continue
            ids.add(get_mesh_id_from_iri(parts[0]))
    return ids


def read_umls_mesh_codes(mrconso_path) -> dict[str, set[str]]:
    """Return {MSH CODE: set of CUIs} for the MSH rows in MRCONSO.RRF that the build uses.

    Rows that umls.check_mrconso_line() rejects (non-English or SUPPRESS=O/E) are skipped, as they are
    everywhere else in Babel: an obsolete atom for a descriptor MeSH has deleted is missing from every MeSH
    dump whatever the versions, so counting it would shift the skew check's limit by a constant.
    UMLS 2026AA has no such MSH rows (its MSH rows are all SUPPRESS=N or Y), but nothing stops a later release.
    """
    code_to_cuis = defaultdict(set)
    with open(mrconso_path, encoding="utf-8") as inf:
        for line in inf:
            x = line.split("|")
            # Column 12 (SAB) is the source, column 14 (CODE) is the source's own identifier.
            if x[11] == "MSH" and check_mrconso_line(line):
                code_to_cuis[x[13]].add(x[0])
    return code_to_cuis


def generate_umls_mesh_skew_report(mrconso_path, mesh_nt_path, umls_version, mesh_year, outfile, sample_size=50):
    """Write a JSON report of UMLS MSH codes that are absent from the MeSH dump.

    Writing the report never fails. check_umls_mesh_skew() reads it and fails the build if the count
    is over config.yaml's umls_mesh_skew_max_missing_codes. The check is a separate rule so that the
    report survives when the check fails.
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

    missing_by_record_type = Counter(code[0] for code in missing)
    report = {
        "umls_version": str(umls_version),
        "mesh_year": str(mesh_year),
        "mesh_records_in_dump": len(mesh_ids),
        "umls_msh_codes": len(code_to_cuis),
        "umls_msh_codes_missing_from_mesh": len(missing),
        # What check_umls_mesh_skew() compares against its limit: the U headings are missing whatever the
        # versions, so counting them would only shift the limit by a constant.
        "umls_msh_codes_missing_from_mesh_excluding_categories": len(missing)
        - missing_by_record_type[MESH_CATEGORY_RECORD_TYPE],
        # D = descriptor, C = Supplementary Concept Record, Q = qualifier, U = the MeSH category headings
        # UMLS adds itself (e.g. U000002 "Anatomy (MeSH Category)"), which are never in MeSH.
        "missing_by_record_type": dict(sorted(missing_by_record_type.items())),
        "umls_cuis_with_msh_codes": len(cui_to_codes),
        "umls_cuis_with_any_missing_msh_code": len(cuis_with_missing),
        "umls_cuis_with_only_missing_msh_codes": len(cuis_only_missing),
        "missing_sample": [f"{MESH}:{code}" for code in missing[:sample_size]],
    }
    with open(outfile, "w", encoding="utf-8") as outf:
        json.dump(report, outf, indent=2)
        outf.write("\n")
    return report


def check_umls_mesh_skew(report_path, max_missing_codes, checked_file):
    """Fail the build when too many UMLS MSH codes have no record in the MeSH dump.

    A large count means mesh_year has fallen behind umls_version, and building on it costs more than the
    hours it would take to notice: every MeSH-owning pipeline would build its compendia from a MeSH dump that
    lacks the records UMLS points to, so their CUIs reach the compendia with no MeSH label, type or tree
    position, and "a CUI follows its MeSH descriptor" cannot route them. So rule umls_mesh_ownership, which
    all five of those pipelines wait for, waits for this check too.

    This runs as its own rule after generate_umls_mesh_skew_report() so that the JSON report survives a
    failure (Snakemake deletes the outputs of a job that raises) and can be read to see what went wrong.

    :param report_path: The JSON written by generate_umls_mesh_skew_report().
    :param max_missing_codes: config.yaml's umls_mesh_skew_max_missing_codes. Compared with the count that
        leaves out the U "(MeSH Category)" headings, which are missing whatever the versions.
    :param checked_file: Written (with a summary) only when the count is within the limit.
    :raises RuntimeError: if the count is over the limit.
    """
    with open(report_path, encoding="utf-8") as inf:
        report = json.load(inf)
    missing = report["umls_msh_codes_missing_from_mesh_excluding_categories"]
    summary = (
        f"{missing:,} MSH codes in UMLS {report['umls_version']} have no record in MeSH {report['mesh_year']} "
        f"(excluding the U category headings); the limit is {max_missing_codes:,}."
    )
    if missing > max_missing_codes:
        raise RuntimeError(
            f"{summary} mesh_year is probably behind umls_version: set mesh_year in config.yaml to the MeSH year "
            f"UMLS {report['umls_version']} was built from, if NLM has published it. If the skew is deliberate, "
            f"raise umls_mesh_skew_max_missing_codes instead. See {report_path} and docs/sources/MeSH/Ingestion.md."
        )
    with open(checked_file, "w", encoding="utf-8") as outf:
        outf.write(summary + "\n")

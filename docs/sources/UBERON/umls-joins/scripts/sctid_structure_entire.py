"""Classify UBERON's SNOMED CT xrefs as SNOMED "structure" or "entire" concepts.

SNOMED CT models anatomy as Structure/Entire/Part triples: "X structure" means X or any part of X,
"Entire X" means the whole of X. UBERON intends its SNOMED xrefs to point at the Entire concept
(obophenotype/uberon#3287). This script reads UBERON's SNOMED xrefs from the anatomy UBERON concord
(after config.yaml: anatomy_xref_prefixes renames SCTID to SNOMEDCT) and uses SNOMED's own
has_entire_anatomy_structure relationship from UMLS MRREL to say which half of the pair each xref
points at. For every xref to a structure concept it proposes the entire partner; that list is what
obophenotype/uberon#3786 asks UBERON to apply.

Usage (from the repository root, after `uv run snakemake anatomy`):

    uv run python docs/sources/UBERON/umls-joins/scripts/sctid_structure_entire.py

Writes docs/sources/UBERON/umls-joins/sctid-structure-entire.csv and, for review, every UBERON term with
more than one SNOMED xref to docs/sources/UBERON/umls-joins/multi-snomed-xrefs.csv (such a term joins all of
its SNOMED concepts into one clique, which is wrong when one is narrower, e.g. left and right). Prints the
summary quoted in docs/sources/UBERON/umls-joins/README.md.
"""

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path

from src.prefixes import SNOMEDCT, UBERON

REPO = Path(__file__).resolve().parents[5]


def read_uberon_snomed_xrefs(concord):
    """Return {UBERON CURIE: {SNOMED code}} from the anatomy UBERON concord."""
    xrefs = defaultdict(set)
    with open(concord) as inf:
        for line in inf:
            subject, _, target = line.rstrip("\n").split("\t")
            if subject.startswith(f"{UBERON}:") and target.startswith(f"{SNOMEDCT}:"):
                xrefs[subject].add(target.split(":", 1)[1])
    return xrefs


def read_labels(path):
    with open(path) as inf:
        return dict(line.rstrip("\n").split("\t", 1) for line in inf if "\t" in line)


def read_snomed(mrconso, mrrel):
    """Return (fully specified name by code, active codes, {structure code: {entire codes}}) for SNOMEDCT_US."""
    fsn, active, aui_to_code = {}, set(), {}
    with open(mrconso) as inf:
        for line in inf:
            x = line.split("|")
            if x[11] != "SNOMEDCT_US":
                continue
            aui_to_code[x[7]] = x[13]
            if x[12] == "FN":
                if x[16] == "N":
                    active.add(x[13])
                    fsn[x[13]] = x[14]
                else:
                    fsn.setdefault(x[13], x[14])
    entires = defaultdict(set)
    with open(mrrel) as inf:
        for line in inf:
            x = line.split("|")
            # RELA is the relationship of the second atom to the first: AUI2 has_entire_anatomy_structure AUI1,
            # so AUI2 is the structure and AUI1 the entire. Only active (SUPPRESS=N) relationships.
            if x[10] != "SNOMEDCT_US" or x[7] != "has_entire_anatomy_structure" or x[14] != "N":
                continue
            entire, structure = aui_to_code.get(x[1]), aui_to_code.get(x[5])
            if entire and structure:
                entires[structure].add(entire)
    return fsn, active, entires


def classify(code, fsn, active, entires, entire_codes):
    if code not in fsn:
        return "not in UMLS SNOMEDCT_US"
    if code not in active:
        return "inactive"
    if code in entires:
        return "structure"
    if code in entire_codes:
        return "entire"
    return "no structure/entire pair"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--concord", default=REPO / "babel_outputs/intermediate/anatomy/concords/UBERON")
    parser.add_argument("--labels", default=REPO / "babel_downloads/UBERON/labels")
    parser.add_argument("--mrconso", default=REPO / "babel_downloads/UMLS/MRCONSO.RRF")
    parser.add_argument("--mrrel", default=REPO / "babel_downloads/UMLS/MRREL.RRF")
    parser.add_argument("--out", default=REPO / "docs/sources/UBERON/umls-joins/sctid-structure-entire.csv")
    parser.add_argument("--multi-out", default=REPO / "docs/sources/UBERON/umls-joins/multi-snomed-xrefs.csv")
    args = parser.parse_args()

    xrefs = read_uberon_snomed_xrefs(args.concord)
    labels = read_labels(args.labels)
    fsn, active, entires = read_snomed(args.mrconso, args.mrrel)
    entire_codes = {e for es in entires.values() for e in es}
    code_to_uberons = defaultdict(set)
    for u, codes in xrefs.items():
        for c in codes:
            code_to_uberons[c].add(u)

    kinds = Counter(classify(c, fsn, active, entires, entire_codes) for codes in xrefs.values() for c in codes)
    rows, statuses = [], Counter()
    for u in sorted(xrefs):
        for s in sorted(xrefs[u]):
            if classify(s, fsn, active, entires, entire_codes) != "structure":
                continue
            es = sorted(entires[s])
            e = ";".join(es)
            if len(es) != 1:
                status = "ambiguous: several entire codes"
            elif es[0] in xrefs[u]:
                status = "remove: term already xrefs the entire code"
            elif code_to_uberons[es[0]]:
                status = "review: entire code already xrefd by " + ",".join(sorted(code_to_uberons[es[0]]))
            else:
                status = "replace"
            statuses[status.split(":")[0]] += 1
            rows.append([u, labels.get(u, ""), s, fsn.get(s, ""), e, "; ".join(fsn.get(x, "") for x in es), status])

    with open(args.out, "w", newline="") as outf:
        w = csv.writer(outf)
        w.writerow(
            ["uberon_id", "uberon_label", "structure_sctid", "structure_fsn", "entire_sctid", "entire_fsn", "status"]
        )
        w.writerows(rows)

    multi = [u for u in sorted(xrefs) if len(xrefs[u]) > 1]
    with open(args.multi_out, "w", newline="") as outf:
        w = csv.writer(outf)
        w.writerow(["uberon_id", "uberon_label", "sctid", "snomed_fsn", "kind"])
        for u in multi:
            for c in sorted(xrefs[u]):
                w.writerow([u, labels.get(u, ""), c, fsn.get(c, ""), classify(c, fsn, active, entires, entire_codes)])

    print(
        f"{sum(kinds.values())} UBERON SNOMED xrefs on {len(xrefs)} UBERON terms; {len(multi)} terms have more than one"
    )
    for k, v in kinds.most_common():
        print(f"  {k}: {v}")
    print(f"{len(rows)} xrefs to structure concepts, written to {args.out}")
    for k, v in statuses.most_common():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()

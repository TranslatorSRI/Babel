"""Census of likely-missed UBERON/UMLS joins in a baseline build, and how many a second build joins.

A candidate is a baseline clique holding UMLS CUIs but no UBERON term, one of whose CUIs has an English
MRCONSO string that, after normalization (lower-case; strip SNOMED's "Entire", "structure of",
"(body structure)" wrappers; collapse punctuation), equals the label or an exact synonym of exactly one
UBERON term that sits in a different baseline clique. This is a lexical heuristic for *finding* missed
joins, not a mapping: it is used only to measure recall, and it misses many (UMLS strings that do not
match UBERON wording) as well as producing some false matches.

Each candidate is bucketed by which cross-reference bridge exists between the two, read from the
anatomy concords of the comparison build:

- SCTID: the UBERON term's SNOMED xref (renamed by config.yaml: anatomy_xref_prefixes) is a SNOMEDCT_US
  code of the CUI.
- FMA: the UBERON term's FMA xref is an FMA code of the CUI.
- none: neither.

Usage (from the repository root):

    uv run python docs/sources/UBERON/umls-joins/scripts/missed_umls_links.py \\
        --before <baseline compendia dir> --after <comparison compendia dir>

Writes the full per-candidate record to data/uberon-umls/missed-umls-links.csv (regenerable, not
committed), a ranked sample to docs/sources/UBERON/umls-joins/missed-umls-links-sample.csv, and prints the
summary quoted in docs/sources/UBERON/umls-joins/README.md.
"""

import argparse
import csv
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path

from src.prefixes import FMA, SNOMEDCT, UBERON, UMLS

REPO = Path(__file__).resolve().parents[5]
COMPENDIA = ["AnatomicalEntity.txt", "GrossAnatomicalStructure.txt", "Cell.txt", "CellularComponent.txt"]
EXACT_SYNONYM = "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym"
STRIP = [
    r"\(body structure\)",
    r"\(cell structure\)",
    r"\(surface region\)",
    r"^entire ",
    r"^structure of ",
    r" structure$",
    r"^structure ",
    r", entire$",
    r", unspecified$",
    r"^the ",
]


def normalize(s):
    s = s.lower().strip()
    for _ in range(2):
        for pattern in STRIP:
            s = re.sub(pattern, "", s).strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return " ".join(s.split())


def read_cliques(directory):
    """Return ({CURIE: clique index}, [[CURIE, ...], ...]) from a compendia directory."""
    clique_of, cliques = {}, []
    for name in COMPENDIA:
        path = Path(directory) / name
        if not path.exists():
            continue
        with open(path) as inf:
            for line in inf:
                ids = [x["i"] for x in json.loads(line)["identifiers"]]
                for i in ids:
                    clique_of[i] = len(cliques)
                cliques.append(ids)
    return clique_of, cliques


def read_uberon_names(labels, synonyms):
    """Return ({normalized string: {UBERON CURIE}}, {UBERON CURIE: label})."""
    keys, label_of = defaultdict(set), {}
    with open(labels) as inf:
        for line in inf:
            curie, _, label = line.rstrip("\n").partition("\t")
            if curie.startswith(f"{UBERON}:"):
                label_of[curie] = label
                keys[normalize(label)].add(curie)
    with open(synonyms) as inf:
        for line in inf:
            if f'"{UBERON}:' not in line:
                continue
            row = json.loads(line)
            if row["curie"].startswith(f"{UBERON}:") and row["predicate"] == EXACT_SYNONYM:
                keys[normalize(row["synonym"])].add(row["curie"])
    return keys, label_of


def read_uberon_xrefs(concord):
    """Return {UBERON CURIE: {target CURIE}} for SNOMEDCT and FMA targets in the anatomy UBERON concord."""
    xrefs = defaultdict(set)
    with open(concord) as inf:
        for line in inf:
            subject, _, target = line.rstrip("\n").split("\t")
            if target.startswith((f"{SNOMEDCT}:", f"{FMA}:")):
                xrefs[subject].add(target)
    return xrefs


def read_mrconso(mrconso, cuis):
    """Return ({CUI: {English strings}}, {CUI: {SNOMEDCT/FMA CURIEs}}, {CUI: {sources}}) for the given CUIs."""
    sources = {"SNOMEDCT_US": SNOMEDCT, "FMA": FMA, "UWDA": FMA}
    strings, codes, sabs = defaultdict(set), defaultdict(set), defaultdict(set)
    with open(mrconso) as inf:
        for line in inf:
            x = line.split("|")
            if x[0] not in cuis or x[1] != "ENG":
                continue
            strings[x[0]].add(x[14])
            sabs[x[0]].add(x[11])
            if x[11] in sources:
                codes[x[0]].add(f"{sources[x[11]]}:{x[13]}")
    return strings, codes, sabs


def describe_unbridged(sources):
    """Say why a CUI with no bridge to UBERON has none, from the sources it comes from."""
    sources = sources - {"MTH", "CHV", "LCH", "SNMI", "RCD", "MDR"}
    if "SNOMEDCT_US" in sources:
        return "SNOMED CT concept"
    if sources & {"FMA", "UWDA"}:
        return "FMA/UWDA concept without SNOMED"
    if sources & {"LNC", "ICF", "ICF-CY", "SNOMEDCT_VET"}:
        return "only LOINC, ICF or veterinary SNOMED"
    return "other"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--before", required=True, help="Baseline compendia directory")
    parser.add_argument("--after", required=True, help="Comparison compendia directory")
    parser.add_argument("--uberon-concord", default=REPO / "babel_outputs/intermediate/anatomy/concords/UBERON")
    parser.add_argument("--labels", default=REPO / "babel_downloads/UBERON/labels")
    parser.add_argument("--synonyms", default=REPO / "babel_downloads/common/ubergraph/synonyms.jsonl")
    parser.add_argument("--mrconso", default=REPO / "babel_downloads/UMLS/MRCONSO.RRF")
    parser.add_argument("--out", default=REPO / "data/uberon-umls/missed-umls-links.csv")
    parser.add_argument("--sample-out", default=REPO / "docs/sources/UBERON/umls-joins/missed-umls-links-sample.csv")
    parser.add_argument("--sample-size", type=int, default=100)
    args = parser.parse_args()

    before_of, before = read_cliques(args.before)
    after_of, _ = read_cliques(args.after)
    keys, label_of = read_uberon_names(args.labels, args.synonyms)
    xrefs = read_uberon_xrefs(args.uberon_concord)

    orphan_cuis = set()
    for ids in before:
        if not any(i.startswith(f"{UBERON}:") for i in ids):
            orphan_cuis.update(i.split(":", 1)[1] for i in ids if i.startswith(f"{UMLS}:"))
    strings, codes, sabs = read_mrconso(args.mrconso, orphan_cuis)

    rows = []
    for cui in sorted(orphan_cuis):
        matches = set()
        for s in strings[cui]:
            matches |= keys.get(normalize(s), set())
        matches = {u for u in matches if u in before_of}
        if len(matches) != 1:
            continue
        uberon = matches.pop()
        umls = f"{UMLS}:{cui}"
        bridges = [
            name
            for name, prefix in (("SCTID", SNOMEDCT), ("FMA", FMA))
            if any(c.startswith(f"{prefix}:") and c in xrefs[uberon] for c in codes[cui])
        ]
        joined = uberon in after_of and after_of.get(uberon) == after_of.get(umls)
        rows.append([uberon, label_of.get(uberon, ""), umls, min(strings[cui]), "+".join(bridges) or "none", joined])

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    header = ["uberon_id", "uberon_label", "umls_id", "umls_string", "bridge", "joined_after"]
    with open(args.out, "w", newline="") as outf:
        w = csv.writer(outf)
        w.writerow(header)
        w.writerows(rows)

    # Sample: spread across buckets and joined/unjoined, so each kind of row is represented.
    groups = defaultdict(list)
    for r in rows:
        groups[(r[4], r[5])].append(r)
    rng = random.Random(0)
    per_group = max(1, args.sample_size // max(1, len(groups)))
    sample = []
    for key in sorted(groups, key=str):
        sample.extend(sorted(rng.sample(groups[key], min(per_group, len(groups[key])))))
    with open(args.sample_out, "w", newline="") as outf:
        w = csv.writer(outf)
        w.writerow(header)
        w.writerows(sample)

    counts = Counter((r[4], r[5]) for r in rows)
    print(f"{len(orphan_cuis)} UMLS CUIs in baseline cliques without UBERON; {len(rows)} candidates")
    print(f"joined after: {sum(1 for r in rows if r[5])}")
    for bridge in sorted({r[4] for r in rows}):
        print(f"  {bridge}: {counts[(bridge, True)]} joined, {counts[(bridge, False)]} not joined")
    unbridged = Counter(describe_unbridged(sabs[r[2].split(":", 1)[1]]) for r in rows if r[4] == "none")
    print("candidates with no bridge, by source:")
    for kind, n in unbridged.most_common():
        print(f"  {kind}: {n}")


if __name__ == "__main__":
    main()

"""Find DOID xrefs that join two distinct diseases into one clique.

`remove_overused_xrefs` catches a target claimed by two or more subjects, which is the shape an ICD
family code has. It cannot catch the other shape: a single DOID term asserting an xref to an
identifier that belongs to a *different* disease -- a syndrome equated with the lesion it produces,
or with a neighbouring condition. Claimed once, it looks exactly like a correct 1:1 mapping, and no
counting rule distinguishes them.

This finds them by asking where a clique would fall apart. For each clique holding two or more MONDO
identifiers' worth of disagreement -- in practice, each clique named in a supplied list of
(clique_leader, other_leader) pairs -- it rebuilds the clique, splits it by which side each member
lands on in a reference build, and reports every concord row that crosses the split, flagging
whether the target is overused.

It does not find the over-merged cliques itself: it takes its (clique_leader, other_leader) pairs
from a clique-diff CSV, so it only examines cliques something else already flagged. Generalising it
into a detector that runs per release is https://github.com/NCATSTranslator/Babel/issues/1066.

Run from the repo root against a disease build, giving it a clique-diff CSV to take its pairs from:

    uv run python docs/sources/DOID/scripts/find_cross_disease_xrefs.py \
        docs/sources/GARD/on-addition/clique-diff-regrouped.csv

Writes docs/sources/DOID/cross-disease-xrefs.csv.

A crossing row is not necessarily the wrong one: sometimes it is correct and the *other* endpoint is
in the wrong clique (DOID:8712 "neurofibromatosis" -> MESH:D017253 "Neurofibromatoses" is a right
xref sitting in the neurofibroma clique). Treat the output as a worklist, not a verdict.

Last result (2026-08-26, DOID 2026-08-18): 29 crossing rows across 11 over-merges, every one
asserted by the DOID concord; 4 of the over-merges involve an overused target (#1032's territory)
and 7 have only single non-overused xrefs (#1064's).
"""

import collections
import csv
import glob
import json
import sys
from pathlib import Path

from src.createcompendia.diseasephenotype import DEFAULT_BAD_XREFS, compute_cliques_for_impact_report
from src.util import get_config, get_repo_root

REPO = get_repo_root()
OUT_CSV = REPO / "docs/sources/DOID/cross-disease-xrefs.csv"
# Concords read for the "before" state, and for the edges that cross a split. GARD's are excluded so
# the reference build is the one the over-merges were found in.
CONCORDS = ["HP", "MP", "MONDO", "UMLS", "DOID", "EFO", "Manual"]


def _labels(prefixes):
    out = {}
    downloads = Path(get_config()["download_directory"])
    for prefix in prefixes:
        path = downloads / prefix / "labels"
        if path.exists():
            with open(path) as inf:
                for line in inf:
                    curie, _, label = line.rstrip("\n").partition("\t")
                    out[curie] = label
    return out


def main(pairs_csv, build_dir=None):
    config = get_config()
    intermediate = Path(config["intermediate_directory"])
    build = Path(build_dir or config["output_directory"])
    concords = [str(intermediate / f"disease/concords/{c}") for c in CONCORDS]
    ids = [f for f in sorted(glob.glob(str(intermediate / "disease/ids/*"))) if not f.endswith("/GARD")]

    before, _ = compute_cliques_for_impact_report(
        concords, ids, mondoclose=str(intermediate / "disease/concords/MONDO_close"), badxrefs=DEFAULT_BAD_XREFS
    )
    after = {}
    for name in ("Disease.txt", "PhenotypicFeature.txt"):
        with open(build / "compendia" / name) as inf:
            for line in inf:
                clique = json.loads(line)
                leader = clique["identifiers"][0]["i"]
                for identifier in clique["identifiers"]:
                    after[identifier["i"]] = leader

    # Every concord row, and how many subjects claim each target within its own concord.
    rows_by_concord = collections.defaultdict(list)
    for name in CONCORDS + ["MONDO_close"]:
        with open(intermediate / f"disease/concords/{name}") as inf:
            for line in inf:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3:
                    rows_by_concord[name].append((parts[0], parts[2]))
    subject_count = {n: collections.Counter(o for _, o in rs) for n, rs in rows_by_concord.items()}

    labels = _labels(["MONDO", "DOID", "MESH", "NCIT", "UMLS", "Orphanet", "HP", "EFO"])
    with open(pairs_csv) as inf:
        pairs = [(r["before_leader"], r["destination"]) for r in csv.DictReader(inf) if r["before_leader"] in before]

    findings = []
    for leader, destination in pairs:
        clique = set(before[leader])
        moved = {m for m in clique if after.get(m) == destination}
        if not moved or moved == clique:
            continue
        stayed = clique - moved
        for concord, rows in rows_by_concord.items():
            for subject, obj in rows:
                if {subject, obj} <= clique and ((subject in stayed) != (obj in stayed)):
                    claimants = subject_count[concord][obj]
                    findings.append(
                        {
                            "asserted_by": concord,
                            "subject": subject,
                            "subject_label": labels.get(subject, ""),
                            "object": obj,
                            "object_label": labels.get(obj, ""),
                            "object_claimed_by": claimants,
                            "shape": "overused target" if claimants > 1 else "single cross-disease xref",
                            "fuses": f"{labels.get(leader, leader)} + {labels.get(destination, destination)}",
                            "clique_leader": leader,
                            "other_leader": destination,
                        }
                    )

    findings = [dict(t) for t in {tuple(sorted(f.items())) for f in findings}]
    findings.sort(key=lambda f: (f["shape"], f["fuses"], f["subject"], f["object"]))
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as outf:
        writer = csv.DictWriter(outf, fieldnames=list(findings[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(findings)

    print(f"{len(pairs)} over-merges examined; {len(findings)} crossing rows written to {OUT_CSV.relative_to(REPO)}")
    print("  by asserting concord:", dict(collections.Counter(f["asserted_by"] for f in findings)))
    print("  by shape:", dict(collections.Counter(f["shape"] for f in findings)))
    fused = collections.defaultdict(set)
    for f in findings:
        fused[f["fuses"]].add(f["shape"])
    print(f"\n  over-merges involving an overused target: {sum('overused target' in v for v in fused.values())}")
    print(
        f"  over-merges with only single cross-disease xrefs: {sum('overused target' not in v for v in fused.values())}"
    )
    return len(findings)


if __name__ == "__main__":
    main(*sys.argv[1:])

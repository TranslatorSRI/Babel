"""Measure the GARD_label concord: what it links, and how often the same rule would be wrong.

The concord itself only ever runs on the GARD ids no other concord places, so its own rows cannot be
checked against anything. This script gets a precision number by running the *same rule* over the
GARD ids MONDO and DOID **do** place -- a held-out set the concord deliberately skips -- and asking
whether the label match lands in the clique those curated xrefs already chose. It also tabulates
what the emitted rows join, and the NCIt label overlap of the GARD:27000-28999 block.

It imports build_gard_label_concord() rather than re-implementing the match, so the measurement
cannot drift from the production rule (docs/sources/CLAUDE.md, "commit the check, not the
conclusion"). The precision pass reuses the same helpers the concord does.

Run from the repo root against a finished disease build:

    uv run python docs/sources/GARD/label-matches/scripts/gard_label_match_report.py

It writes two CSVs beside docs/sources/GARD/label-matches/README.md, which records the summary:
label-matches.csv (one row per emitted link) and label-mismatches.csv (the held-out disagreements,
which are upstream mappings worth a second look rather than a Babel defect -- see #1063).

Last result (2026-08-26, GARD Jun2026, MONDO/DOID/NCIt/UMLS 2026AA): 270 rows emitted; the same rule
over the 15,937 held-out GARD ids picks a target for 15,370 and disagrees with the curated clique 33
times (0.21%).
"""

import collections
import csv
import json
import sys
from pathlib import Path

from src.createcompendia.diseasephenotype import build_gard_label_concord, normalize_label_for_matching
from src.util import get_config, get_repo_root

REPO = get_repo_root()
OUT_CSV = REPO / "docs/sources/GARD/label-matches/label-matches.csv"
# The held-out disagreements: GARD ids whose label exactly names a clique other than the one MONDO's
# or DOID's own xref places them in. GARD_label never acts on these (guard 1 skips them), so they
# are not a Babel defect -- they are a list of upstream mappings worth a second look, which is what
# https://github.com/NCATSTranslator/Babel/issues/1063 sends to MONDO.
MISMATCH_CSV = REPO / "docs/sources/GARD/label-matches/label-mismatches.csv"
NCIT_BLOCK = range(27000, 29000)  # the contiguous GARD id block most of the unmapped terms fall in


def _labels(path):
    """A CURIE -> label dict from a two-column labels file."""
    with open(path) as inf:
        return dict(line.rstrip("\n").split("\t", 1) for line in inf if "\t" in line)


def _clique_index(compendia):
    """CURIE -> (leader, preferred label, biolink type, non-GARD member count, compendium).

    The member count excludes GARD identifiers so it reads the same whether this runs against a
    build that already has the GARD_label concord or one that does not -- it is the size of the
    clique a GARD term joins, not the size after it joined.
    """
    index = {}
    for path in compendia:
        for line in open(path):
            clique = json.loads(line)
            ids = clique["identifiers"]
            others = sum(1 for i in ids if not i["i"].startswith("GARD:"))
            entry = (ids[0]["i"], clique.get("preferred_name", ""), clique["type"], others, Path(path).name)
            for identifier in ids:
                index[identifier["i"]] = entry
    return index


def main(build_dir=None):
    config = get_config()
    build = Path(build_dir or config["output_directory"])
    downloads = Path(config["download_directory"])
    intermediate = Path(config["intermediate_directory"])
    concords = intermediate / "disease/concords"
    pool = config["disease_gard_label_match_prefixes"]

    # Rebuild the concord in a scratch file rather than reading the built one, so the report is of
    # the current code even if the build predates a change to it.
    scratch = REPO / "data/scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    other_concords = [str(concords / c) for c in config["disease_concords"] if c != "GARD_label"]
    build_gard_label_concord(
        str(downloads / "GARD/labels"),
        [str(intermediate / f"disease/ids/{p}") for p in pool],
        [str(downloads / f"{p}/labels") for p in pool],
        other_concords,
        str(scratch / "GARD_label.report"),
        str(scratch / "GARD_label.report.yaml"),
    )
    rows = [line.rstrip("\n").split("\t") for line in open(scratch / "GARD_label.report")]

    gard_labels = _labels(downloads / "GARD/labels")
    cliques = _clique_index([build / "compendia/Disease.txt", build / "compendia/PhenotypicFeature.txt"])
    all_labels = {}
    for prefix in pool:
        all_labels.update(_labels(downloads / f"{prefix}/labels"))

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as outf:
        # lineterminator="\n" overrides csv's default "\r\n", matching the impact report's writers
        # so the committed file uses LF and diffs cleanly.
        writer = csv.writer(outf, lineterminator="\n")
        writer.writerow(
            ["gard_id", "gard_label", "target", "target_label", "clique_leader", "clique_type", "clique_members_joined"]
        )
        for gard_id, _predicate, target in rows:
            leader, _leader_label, clique_type, size, _compendium = cliques.get(target, (target, "", "", 1, ""))
            writer.writerow(
                [gard_id, gard_labels[gard_id], target, all_labels.get(target, ""), leader, clique_type, size]
            )
    print(f"{len(rows)} emitted rows written to {OUT_CSV.relative_to(REPO)}")
    print("  target prefix:", dict(collections.Counter(t.split(":")[0] for _, _, t in rows).most_common()))
    sizes = sorted(cliques.get(t, (None, None, None, 1, None))[3] for _, _, t in rows)
    print(
        f"  clique joined: median {sizes[len(sizes) // 2]} non-GARD members, "
        f"{sizes.count(2)} of {len(sizes)} join exactly 2"
    )

    # Precision, on the held-out GARD ids MONDO/DOID place and the concord therefore skips.
    matchable = []
    for prefix in pool:
        with open(intermediate / f"disease/ids/{prefix}") as inf:
            known = {line.split("\t")[0] for line in inf}
        by_label = collections.defaultdict(set)
        for curie, label in _labels(downloads / f"{prefix}/labels").items():
            if curie in known and label.strip():
                by_label[normalize_label_for_matching(label)].add(curie)
        matchable.append(by_label)

    claimed = set()
    for concord in other_concords:
        with open(concord) as inf:
            for line in inf:
                claimed.update(c for c in line.rstrip("\n").split("\t") if c.startswith("GARD:"))

    right, wrong, undecided = 0, [], 0
    for gard_id in sorted(claimed & set(gard_labels) & set(cliques)):
        key = normalize_label_for_matching(gard_labels[gard_id])
        target = next((next(iter(m[key])) for m in matchable if key in m and len(m[key]) == 1), None)
        if target is None or target not in cliques:
            undecided += 1
        elif cliques[target][0] == cliques[gard_id][0]:
            right += 1
        else:
            wrong.append((gard_id, gard_labels[gard_id], cliques[gard_id], target, cliques[target]))
    decided = right + len(wrong)
    print(f"\nheld-out precision over {right + len(wrong) + undecided} GARD ids other concords place:")
    print(f"  rule picks a target for {decided}: correct {right}, wrong {len(wrong)} ({len(wrong) / decided:.2%})")
    for gard_id, label, placed, target, other in wrong[:8]:
        print(f'    {gard_id} "{label}" is on {placed[0]}; the label also names {target} ({other[0]})')

    with open(MISMATCH_CSV, "w", newline="") as outf:
        writer = csv.writer(outf, lineterminator="\n")
        writer.writerow(
            [
                "gard_id",
                "gard_label",
                "mapped_clique_leader",
                "mapped_clique_label",
                "label_matches",
                "label_matches_clique_leader",
                "label_matches_clique_label",
            ]
        )
        for gard_id, label, placed, target, other in wrong:
            writer.writerow([gard_id, label, placed[0], placed[1], target, other[0], other[1]])
    print(f"  full list written to {MISMATCH_CSV.relative_to(REPO)}")

    # The NCIt label overlap of the 27000-28999 block, stated as an observation: no source documents
    # where those registry terms came from.
    ncit_labels = {normalize_label_for_matching(v) for v in _labels(downloads / "NCIT/labels").values()}
    block = collections.Counter()
    for gard_id, label in gard_labels.items():
        key = "27000-28999" if int(gard_id.split(":")[1]) in NCIT_BLOCK else "outside the block"
        block[(key, normalize_label_for_matching(label) in ncit_labels)] += 1
    print("\nNCIt preferred-label overlap:")
    for key in ("27000-28999", "outside the block"):
        hit, miss = block[(key, True)], block[(key, False)]
        print(f"  {key:18s} {hit:5d}/{hit + miss:5d} = {hit / (hit + miss):.1%}")
    return len(rows)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)

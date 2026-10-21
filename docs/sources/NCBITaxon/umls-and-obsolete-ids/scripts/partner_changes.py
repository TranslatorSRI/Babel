"""Compare which NCBITaxon each MESH and UMLS identifier sits with in two OrganismTaxon builds.

The taxon compendium allows at most one NCBITaxon, MESH and UMLS identifier per clique, so a bad new
mapping shows up as an identifier losing (or swapping) its NCBITaxon partner rather than as a large
clique. This script classifies every MESH/UMLS identifier in the BEFORE build by what happened to its
NCBITaxon partner in the AFTER build, and writes:

- partner-changes.md: the counts, plus a sample of the regressions.
- partner-changes-regressions.csv: every identifier that lost a current NCBITaxon partner.

Usage (from the repository root):

    PYTHONPATH=. uv run python docs/sources/NCBITaxon/umls-and-obsolete-ids/scripts/partner_changes.py \\
        BEFORE/OrganismTaxon.txt "before label" AFTER/OrganismTaxon.txt "after label" \\
        babel_downloads/NCBITaxon/taxdump.tar docs/sources/NCBITaxon/umls-and-obsolete-ids
"""

import csv
import json
import random
import sys
from collections import Counter
from pathlib import Path

from src.datahandlers.ncbitaxon import read_obsolete_taxa
from src.prefixes import NCBITAXON

SAMPLE_SIZE = 10


def read_partners(compendium):
    """Return ({non-NCBITaxon CURIE: NCBITaxon partner or None}, {CURIE: clique}, {CURIE: label})."""
    partners, cliques, labels = {}, {}, {}
    with open(compendium) as inf:
        for line in inf:
            identifiers = json.loads(line)["identifiers"]
            clique = [x["i"] for x in identifiers]
            ncbi = [curie for curie in clique if curie.startswith(f"{NCBITAXON}:")]
            for x in identifiers:
                labels[x["i"]] = x.get("l", "")
                cliques[x["i"]] = clique
                if not x["i"].startswith(f"{NCBITAXON}:"):
                    partners[x["i"]] = ncbi[0] if ncbi else None
    return partners, cliques, labels


def classify(before, after, obsolete):
    if before == after:
        return "same partner"
    if before is None:
        return "gained an NCBITaxon"
    if before in obsolete:
        return "obsolete partner replaced by current" if after else "obsolete partner removed"
    return "current partner replaced (regression)" if after else "current partner lost (regression)"


def main(before_fn, before_label, after_fn, after_label, taxdump, outdir):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    merged, deleted = read_obsolete_taxa(taxdump)
    obsolete = set(merged) | deleted
    before, before_cliques, before_labels = read_partners(before_fn)
    after, after_cliques, after_labels = read_partners(after_fn)

    counts = Counter()
    regressions = []
    for curie, partner in sorted(before.items()):
        kind = classify(partner, after.get(curie), obsolete)
        counts[(curie.split(":")[0], kind)] += 1
        if kind.endswith("(regression)"):
            regressions.append(
                {
                    "curie": curie,
                    "label": before_labels.get(curie, ""),
                    "before_partner": partner,
                    "before_partner_label": before_labels.get(partner, ""),
                    "after_partner": after.get(curie) or "",
                    "after_partner_label": after_labels.get(after.get(curie), ""),
                    "before_partner_now_with": "; ".join(after_cliques.get(partner, [])),
                }
            )

    with open(outdir / "partner-changes-regressions.csv", "w", newline="") as outf:
        writer = csv.DictWriter(outf, fieldnames=list(regressions[0].keys()) if regressions else ["curie"])
        writer.writeheader()
        writer.writerows(regressions)

    lines = [
        "# NCBITaxon partner changes",
        "",
        f"- Before: {before_label}",
        f"- After: {after_label}",
        "",
        "| prefix | what happened to its NCBITaxon partner | identifiers |",
        "|---|---|---|",
    ]
    lines += [f"| {prefix} | {kind} | {n:,} |" for (prefix, kind), n in sorted(counts.items())]
    lines += [
        "",
        f"A random sample of {min(SAMPLE_SIZE, len(regressions))} of the {len(regressions):,} regressions "
        "(all of them are in `partner-changes-regressions.csv`):",
        "",
        "| identifier | before partner | after partner | before partner is now with |",
        "|---|---|---|---|",
    ]
    random.seed(0)
    for row in random.sample(regressions, min(SAMPLE_SIZE, len(regressions))):
        lines.append(
            f"| {row['curie']} {row['label']} | {row['before_partner']} {row['before_partner_label']} "
            f"| {row['after_partner']} {row['after_partner_label']} | {row['before_partner_now_with']} |"
        )
    (outdir / "partner-changes.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:7])

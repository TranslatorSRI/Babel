"""Count DOID GARD xrefs that disagree with MONDO about which MONDO term a GARD id belongs to.

This is the number behind "MONDO_GARD must precede DOID in disease_concords" (config.yaml): for each
DOID -> GARD row, look up the MONDO term MONDO_GARD maps that GARD id to, and the MONDO terms the
DOID term is equivalent to (DOID's own MONDO xrefs plus MONDO's exactMatch to the DOID term). A row
disagrees when both are known and the two differ. MONDO is a unique prefix, so such a pair can only
land with whichever concord glom() reads first.

Run from the repo root against a disease build:

    uv run python docs/sources/GARD/scripts/count_doid_mondo_gard_disagreements.py

Last result (2026-08-21, DOID 2026-08-18, MONDO via UberGraph): 148 disagreeing rows of 2,208.
"""

import collections
import sys
from pathlib import Path

from src.util import get_config


def main(concords=None):
    concords = Path(concords or f"{get_config()['intermediate_directory']}/disease/concords")

    def rows(name):
        return [line.rstrip("\n").split("\t") for line in open(concords / name)]

    mondo_for_gard = {o: s for s, _, o in rows("MONDO_GARD")}
    doid = rows("DOID")
    equivalents = collections.defaultdict(set)
    for s, _, o in doid:
        if o.startswith("MONDO:"):
            equivalents[s].add(o)
    for s, _, o in rows("MONDO"):
        if o.startswith("DOID:"):
            equivalents[o].add(s)

    gard_rows = [(s, o) for s, _, o in doid if o.startswith("GARD:")]
    disagree = [
        (s, o, mondo_for_gard[o], sorted(equivalents[s]))
        for s, o in gard_rows
        if o in mondo_for_gard and equivalents[s] and mondo_for_gard[o] not in equivalents[s]
    ]
    print(f"{len(disagree)} of {len(gard_rows)} DOID->GARD rows disagree with MONDO_GARD")
    for s, o, m, eq in disagree[:5]:
        print(f"  {s} -> {o}: MONDO_GARD says {m}, DOID is equivalent to {eq}")
    return len(disagree)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)

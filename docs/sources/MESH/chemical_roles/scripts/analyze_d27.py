#!/usr/bin/env python
"""Characterize how MeSH D27 (Chemical Actions and Uses) splits between chemicals and chemical roles.

The ChemicalRole compendium takes MeSH descriptors that are in D27 and in no tree another compendium takes
(``mesh.MESH_ENTITY_TREES``: A, B, C, D01–D26), and the substance-typed UMLS CUIs whose MeSH atoms all point at
them; the chemicals compendium leaves both out. This script regenerates
the evidence for that rule from the downloaded files, using the production predicates
(``chemicals.get_mesh_role_descriptors`` and ``umls.get_cuis_mapped_only_to_mesh``):

  1. how many D27 descriptors are roles versus also in an entity tree, with samples of each.
     The mixed ones are why "any D27 tree number" is not the rule: MeSH files specific substances under
     D27 next to their action class (Cefiderocol, High Fructose Corn Syrup);
  2. the SCRs mapped to D27 descriptors, which stay in chemicals because SCRs are substances (this is why
     D27 is not in chemicals' scr_exclude_trees);
  3. the UMLS CUIs whose MeSH atoms are all on role descriptors, by semantic type, and how many of them
     ``chemical_role.write_umls_ids`` keeps (substance-typed, non-protein).

Usage (from the repository root, with babel_downloads/MESH/mesh.nt and babel_downloads/UMLS/*.RRF present)::

    uv run python docs/sources/MESH/chemical_roles/scripts/analyze_d27.py
    uv run rumdl fmt docs/sources/MESH/chemical_roles

Writes README-generated.md plus the full CSVs next to this script's parent directory.
"""

import csv
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Allow running this script directly from a checkout without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from src.createcompendia.chemical_role import UMLS_PROTEIN_TREES, UMLS_SUBSTANCE_TREE  # noqa: E402
from src.createcompendia.chemicals import get_mesh_role_descriptors  # noqa: E402
from src.datahandlers import mesh, umls  # noqa: E402
from src.prefixes import MESH  # noqa: E402

OUT_DIR = Path(__file__).resolve().parents[1]
MESH_NT = Path("babel_downloads/MESH/mesh.nt")
MRCONSO = Path("babel_downloads/UMLS/MRCONSO.RRF")
MRSTY = Path("babel_downloads/UMLS/MRSTY.RRF")
SAMPLE_SIZE = 10

TRIPLE = re.compile(r"^<http://id\.nlm\.nih\.gov/mesh/([^>]+)>\t<([^>]+)>\t(.*) \.$")


def read_mesh_nt():
    """Stream mesh.nt for descriptor tree numbers, English labels and SCR mappings."""
    trees = defaultdict(set)
    labels = {}
    scr_mapped_to = defaultdict(set)
    with open(MESH_NT) as inf:
        for line in inf:
            m = TRIPLE.match(line.rstrip("\n"))
            if not m:
                continue
            subject, predicate, obj = m.groups()
            if predicate.endswith("vocab#treeNumber"):
                trees[f"{MESH}:{subject}"].add(obj.strip("<>").rsplit("/", 1)[1])
            elif predicate.endswith("rdf-schema#label") and obj.endswith("@en"):
                labels[f"{MESH}:{subject}"] = obj[1:-4]
            elif predicate.endswith("vocab#preferredMappedTo") or predicate.endswith("vocab#mappedTo"):
                # Qualified mappings look like D000117Q000031; the descriptor is the part before the Q.
                descriptor = obj.strip("<>").rsplit("/", 1)[1].split("Q")[0]
                scr_mapped_to[f"{MESH}:{subject}"].add(f"{MESH}:{descriptor}")
    return trees, labels, scr_mapped_to


def is_entity_tree(tree_number):
    return tree_number.split(".")[0] in mesh.MESH_ENTITY_TREES


def sample(rows):
    """A deterministic spread across the list rather than its head."""
    rows = sorted(rows)
    return sorted(random.Random(0).sample(rows, min(SAMPLE_SIZE, len(rows))))


def main():
    trees, labels, scr_mapped_to = read_mesh_nt()
    d27 = {d for d, ts in trees.items() if any(t.startswith(mesh.MESH_CHEMICAL_ACTIONS_AND_USES_TREE) for t in ts)}
    roles = get_mesh_role_descriptors()
    mixed = {d for d in d27 if any(is_entity_tree(t) for t in trees[d])}
    if roles != d27 - mixed:
        raise RuntimeError("get_mesh_role_descriptors() disagrees with the D27-minus-entity-trees split in mesh.nt")

    with open(OUT_DIR / "mesh_d27_descriptors.csv", "w", newline="") as outf:
        writer = csv.writer(outf)
        writer.writerow(["mesh_id", "label", "role", "tree_numbers"])
        for d in sorted(d27):
            writer.writerow([d, labels.get(d, ""), d in roles, " ".join(sorted(trees[d]))])

    d27_scrs = {c for c, ds in scr_mapped_to.items() if ds & d27}
    role_only_scrs = {c for c, ds in scr_mapped_to.items() if ds and ds <= roles}

    role_cuis = umls.get_cuis_mapped_only_to_mesh(str(MRCONSO), roles)
    cui_names = {}
    with open(MRCONSO) as inf:
        for line in inf:
            x = line.split("|")
            if x[0] in role_cuis and x[1] == "ENG" and x[2] == "P" and x[4] == "PF" and x[6] == "Y":
                cui_names.setdefault(x[0], x[14])
    cui_types = defaultdict(set)
    cui_trees = defaultdict(set)
    with open(MRSTY) as inf:
        for line in inf:
            x = line.split("|")
            if x[0] in role_cuis:
                cui_types[x[0]].add(x[3])
                cui_trees[x[0]].add(x[2])
    kept = {
        c
        for c in role_cuis
        if any(t.startswith(UMLS_SUBSTANCE_TREE) for t in cui_trees[c]) and not cui_trees[c] & UMLS_PROTEIN_TREES
    }
    dropped = role_cuis - kept
    with open(OUT_DIR / "umls_role_cuis.csv", "w", newline="") as outf:
        writer = csv.writer(outf)
        writer.writerow(["cui", "name", "semantic_types", "in_chemical_role"])
        for c in sorted(role_cuis):
            writer.writerow([c, cui_names.get(c, ""), "; ".join(sorted(cui_types[c])), c in kept])

    def fmt_descriptor(d):
        return f"- `{d}` {labels.get(d, '')} ({', '.join(sorted(trees[d]))})"

    lines = [
        "# MeSH D27 split (generated)",
        "",
        "Generated by `scripts/analyze_d27.py`; do not edit by hand. Samples are a seeded spread across each list.",
        "",
        "## Descriptors",
        "",
        f"- D27 descriptors: {len(d27)}",
        f"- Roles (D27 and no A, B, C or D01–D26 tree), in ChemicalRole: {len(roles)}",
        f"- Also in an entity tree, left to that compendium: {len(mixed)}",
        "",
        "Full list: `mesh_d27_descriptors.csv`.",
        "",
        "### Sample roles",
        "",
        *[fmt_descriptor(d) for d in sample(roles)],
        "",
        "### Sample descriptors also in an entity tree",
        "",
        *[fmt_descriptor(d) for d in sample(mixed)],
        "",
        "## SCRs mapped to D27 descriptors",
        "",
        f"{len(d27_scrs)} SCRs map to a D27 descriptor; {len(role_only_scrs)} map only to role descriptors. "
        "They stay in chemicals: an SCR is a substance filed under its class, not the class itself.",
        "",
        *[f"- `{c}` {labels.get(c, '')} -> {', '.join(sorted(scr_mapped_to[c]))}" for c in sample(d27_scrs)],
        "",
        "## UMLS CUIs on role descriptors",
        "",
        f"{len(role_cuis)} CUIs have MeSH atoms only on role descriptors. Chemicals leaves all of them out. "
        f"ChemicalRole takes the {len(kept)} that are substance-typed and not protein-typed; the other "
        f"{len(dropped)} are effects, findings or proteins, left to process, protein or leftover UMLS. "
        "Full list: `umls_role_cuis.csv`.",
        "",
        "Semantic types of the CUIs ChemicalRole takes:",
        "",
        *[f"- {t}: {n}" for t, n in Counter(t for c in kept for t in cui_types[c]).most_common()],
        "",
        "Semantic types of the CUIs it leaves out:",
        "",
        *[f"- {t}: {n}" for t, n in Counter(t for c in dropped for t in cui_types[c]).most_common()],
        "",
        "Sample taken:",
        "",
        *[f"- `UMLS:{c}` {cui_names.get(c, '')} ({'; '.join(sorted(cui_types[c]))})" for c in sample(kept)],
        "",
        "Sample left out:",
        "",
        *[f"- `UMLS:{c}` {cui_names.get(c, '')} ({'; '.join(sorted(cui_types[c]))})" for c in sample(dropped)],
        "",
    ]
    (OUT_DIR / "README-generated.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()

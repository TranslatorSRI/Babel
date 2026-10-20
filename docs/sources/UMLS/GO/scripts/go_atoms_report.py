"""Measure how UMLS's GO atoms feed the process/activity concord and UMLS synonyms, over a finished build.

Replays the process/activity clique build (and the anatomy one, which also joins UMLS to GO) over a build's
`intermediate/` directory, with the UMLS-GO concord regenerated three ways from MRCONSO:

- all: every non-suppressed GO atom (the behavior before GO_PREFERRED_TTYS);
- preferred: only GO_PREFERRED_TTYS atoms (what build_process_umls_relationships() does);
- fallback: preferred, plus a GO term's other atoms when none of its preferred atoms is in a process CUI
  (considered and rejected; see docs/sources/UMLS/GO.md).

It then counts, for two rules about which GO atoms pull_umls() turns into UMLS synonyms, how many strings each rule
drops from CUIs that do or do not end up in a clique with a GO term, and which wrong-term strings the shipped rule
(umls.is_other_go_term_atom()) still keeps. "GO's own names" are GO labels plus the GO synonyms in UberGraph's
synonyms.jsonl, which is where SynonymFactory gets them. Finally it checks that no CUI takes its UMLS label from an
atom that the shipped rule skips.

Usage (from the repository root):

    uv run python docs/sources/UMLS/GO/scripts/go_atoms_report.py \\
        --mrconso babel_downloads/UMLS/MRCONSO.RRF --intermediate data/2026jul22/intermediate \\
        --go-labels babel_downloads/GO/labels --ubergraph-synonyms babel_downloads/common/ubergraph/synonyms.jsonl \\
        > docs/sources/UMLS/GO/go_atoms_report.json
"""

import argparse
import contextlib
import glob
import io
import json
import os
import sys
import tempfile
from collections import defaultdict

from src.createcompendia import anatomy, processactivitypathway
from src.datahandlers.umls import (
    DEFAULT_ACCEPTABLE_TTYS,
    GO_PREFERRED_TTYS,
    build_sets,
    check_mrconso_line,
    is_other_go_term_atom,
    read_umls_priority,
)


def read_go_atoms(mrconso):
    """For CUIs with GO atoms, return (cui -> [(tty, go_code, string)] for its GO atoms, cui -> set of its non-GO
    strings, cui -> the GO code of the atom pull_umls() picks as its label, or "" for a non-GO atom)."""
    priority = read_umls_priority()
    go_atoms = defaultdict(list)
    other_strings = defaultdict(set)
    # The atom pull_umls() would pick as the CUI's label: the lowest (priority, term, go_code).
    label_atom = {}
    with open(mrconso) as f:
        for line in f:
            if not check_mrconso_line(line):
                continue
            x = line.split("|")
            go_code = x[13] if x[11] == "GO" else ""
            if go_code:
                go_atoms[x[0]].append((x[12], go_code, x[14]))
            else:
                other_strings[x[0]].add(x[14])
            atom = (priority.get((x[11], x[12], x[16]), 1000000), x[14], go_code)
            if x[0] not in label_atom or atom < label_atom[x[0]]:
                label_atom[x[0]] = atom
    return (
        go_atoms,
        {cui: other_strings[cui] for cui in go_atoms},
        {cui: label_atom[cui][2] for cui in go_atoms},
    )


def process_cliques(intermediate, umls_concord):
    """Replay the process/activity clique build with the given UMLS concord file, which must be named "UMLS" (the
    pipeline recognizes its UMLS concord by name)."""
    concords = [f"{intermediate}/process/concords/{source}" for source in ("GO", "RHEA")] + [umls_concord]
    dicts, _ = processactivitypathway.compute_cliques_for_impact_report(
        concords, sorted(glob.glob(f"{intermediate}/process/ids/*"))
    )
    return dicts


def go_by_cui(dicts):
    """Map each UMLS CUI in a clique that contains a GO term to that clique's GO terms."""
    result = defaultdict(set)
    for clique in {frozenset(c) for c in dicts.values()}:
        gos = {c for c in clique if c.startswith("GO:")}
        if gos:
            for c in clique:
                if c.startswith("UMLS:"):
                    result[c[len("UMLS:") :]] |= gos
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mrconso", required=True)
    parser.add_argument("--intermediate", required=True, help="A finished build's intermediate/ directory")
    parser.add_argument("--go-labels", required=True)
    parser.add_argument("--ubergraph-synonyms", required=True)
    args = parser.parse_args()
    inter = args.intermediate

    go_atoms, other_strings, label_go_code = read_go_atoms(args.mrconso)
    process_ids = f"{inter}/process/ids/UMLS"
    with open(process_ids) as f:
        process_cuis = {line.split("\t")[0][len("UMLS:") :] for line in f}

    report = {"CUIs with GO atoms": len(go_atoms)}
    with contextlib.redirect_stdout(io.StringIO()):
        anat, _ = anatomy.compute_cliques_for_impact_report(
            sorted(glob.glob(f"{inter}/anatomy/concords/[A-Z]*")), sorted(glob.glob(f"{inter}/anatomy/ids/*"))
        )
    anatomy_go = go_by_cui(anat)

    joined = {}
    with tempfile.TemporaryDirectory() as tmp:
        variants = {"all": DEFAULT_ACCEPTABLE_TTYS, "preferred": {**DEFAULT_ACCEPTABLE_TTYS, "GO": GO_PREFERRED_TTYS}}
        concords = {}
        for name in ("all", "preferred", "fallback"):
            os.mkdir(os.path.join(tmp, name))
            concords[name] = os.path.join(tmp, name, "UMLS")
        for name, ttys in variants.items():
            build_sets(args.mrconso, process_ids, concords[name], {"GO": "GO"}, acceptable_ttys=ttys)

        # fallback = preferred + every process-CUI atom of a GO term that has no preferred atom in a process CUI.
        with open(concords["preferred"]) as f:
            preferred_pairs = {tuple(line.rstrip("\n").split("\t")) for line in f}
        covered = {go for _, _, go in preferred_pairs}
        restored = sorted(
            {
                (f"UMLS:{cui}", "eq", code)
                for cui in process_cuis & go_atoms.keys()
                for _, code, _ in go_atoms[cui]
                if code not in covered
            }
        )
        with open(concords["fallback"], "w") as f:
            f.writelines("\t".join(p) + "\n" for p in sorted(preferred_pairs) + restored)

        for name, path in concords.items():
            with open(path) as f:
                report[f"{name}: process UMLS-GO concord pairs"] = sum(1 for _ in f)
            with contextlib.redirect_stdout(io.StringIO()):
                cui_go = go_by_cui(process_cliques(inter, path))
            for cui, gos in anatomy_go.items():
                cui_go[cui] |= gos
            joined[name] = cui_go
            report[f"{name}: CUIs with GO atoms that end up in a clique with GO"] = len(cui_go.keys() & go_atoms.keys())

    restored_joined = [(cui, code) for cui, _, code in restored if cui[len("UMLS:") :] in joined["fallback"]]
    report["fallback: pairs added to preferred"] = len(restored)
    report["fallback: added pairs whose CUI ends up in a clique with GO"] = len(restored_joined)
    report["fallback: examples of added joins"] = [
        f"{cui} {code} " + "; ".join(f"{t} {s!r}" for t, c, s in go_atoms[cui[len("UMLS:") :]] if c == code)
        # Ten examples spread across the list, rather than the lowest CUIs.
        for cui, code in restored_joined[:: max(1, len(restored_joined) // 10)][:10]
    ]

    # GO's own names, lower-cased: labels and UberGraph synonyms.
    go_names = defaultdict(set)
    with open(args.go_labels) as f:
        for line in f:
            curie, _, label = line.rstrip("\n").partition("\t")
            go_names[curie].add(label.lower())
    with open(args.ubergraph_synonyms) as f:
        for line in f:
            if '"GO:' in line:
                row = json.loads(line)
                go_names[row["curie"]].add(row["synonym"].lower())
    is_go_name = defaultdict(bool)
    for names in go_names.values():
        for name in names:
            is_go_name[name] = True

    def drop_all(cui):
        return {s for _, _, s in go_atoms[cui]}

    def own_go_codes(cui):
        return {code for tty, code, _ in go_atoms[cui] if tty in GO_PREFERRED_TTYS}

    def kept_by_shipped_rule(cui):
        own = own_go_codes(cui)
        return {s for _, code, s in go_atoms[cui] if not is_other_go_term_atom(code, own)}

    def drop_other_go_terms(cui):
        return {s for _, _, s in go_atoms[cui]} - kept_by_shipped_rule(cui)

    cui_go = joined["preferred"]
    for rule_name, rule in (("drop every GO atom", drop_all), ("drop GO atoms of other GO terms", drop_other_go_terms)):
        counts = defaultdict(int)
        for cui in go_atoms:
            # A string that a non-GO atom of the same CUI also supplies is never lost.
            for s in rule(cui) - other_strings[cui]:
                if cui not in cui_go:
                    counts["dropped from CUIs not in a GO clique (names lost)"] += 1
                elif any(s.lower() in go_names[go] for go in cui_go[cui]):
                    counts["dropped from CUIs in a GO clique, already a name of that GO term"] += 1
                elif is_go_name[s.lower()]:
                    counts["dropped from CUIs in a GO clique, a name of a different GO term"] += 1
                else:
                    counts["dropped from CUIs in a GO clique, not a current GO name"] += 1
        report[f"synonyms, {rule_name}"] = dict(sorted(counts.items()))

    # Wrong-term strings the shipped rule keeps: MRCONSO files them under the CUI's own GO code, but current GO
    # names them only for another term, so nothing in MRCONSO alone can catch them.
    missed = sorted(
        (cui, s)
        for cui in go_atoms
        if cui in cui_go
        for s in kept_by_shipped_rule(cui) - other_strings[cui]
        if is_go_name[s.lower()] and not any(s.lower() in go_names[go] for go in cui_go[cui])
    )
    # A CUI with several GO preferred terms keeps all of their names, though it can join only one of them.
    multi = sum(1 for cui, _ in missed if len(own_go_codes(cui)) > 1)
    report["synonyms, drop GO atoms of other GO terms: kept on CUIs in a GO clique, a name of a different GO term"] = {
        "on CUIs with several GO preferred terms": multi,
        "on CUIs with one GO preferred term (GO has since moved the name)": len(missed) - multi,
    }
    report["synonyms, examples of those kept strings"] = [
        f"UMLS:{cui} (in a clique with {', '.join(sorted(cui_go[cui]))}): {s!r}"
        for cui, s in missed[:: max(1, len(missed) // 10)][:10]
    ]
    report["labels: CUIs whose UMLS label comes from a GO atom the shipped rule skips"] = sum(
        1 for cui in go_atoms if is_other_go_term_atom(label_go_code[cui], own_go_codes(cui))
    )

    json.dump(report, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()

# CLAUDE.md — src/tools/clique_diff/

`babel-clique-diff` compares the finished JSONL compendia of two builds. Full reference:
[`docs/tools/CliqueDiff.md`](../../../docs/tools/CliqueDiff.md).

The diff logic lives in [`src/model/compendium_diff.py`](../../model/compendium_diff.py); this
`cli.py` is a thin argparse/CSV wrapper over it — put new comparison logic in the model module,
not here.

**It is a whole-node job on a big pipeline.** `diff_builds()` holds every `--files` compendium from
*both* builds in memory at once, so the cost tracks the identifier count, not the change count: the
eight chemical outputs peaked at 202.9 GiB over 45 minutes, single-threaded. Splitting `--files`
across runs to save memory is not an option — it turns retypes into phantom `dropped` rows. See
[`docs/tools/CliqueDiff.md`](../../../docs/tools/CliqueDiff.md) ("Resource cost").

Reach for this tool, not `source-impact-report`, whenever a change can *restructure* existing
cliques (a disjointness policy, a concord/close-match change, any change that isn't "add a
source"): `source-impact-report`'s before/after is always "same inputs minus one source", so it
only ever sees added/expanded/merged cliques, never ones that split, lose members, or disappear.
See `docs/AddingNewSources.md` ("Build-vs-build clique diff") for the full when-to-use-which
guidance and a worked example.

# Clique diff: the #935 food-evidence vote and `chemical_type_order`

[PR #948](https://github.com/NCATSTranslator/Babel/pull/948) made DrugBank food/extract evidence a
*vote* ranked by `config.yaml: chemical_type_order` instead of a clique-level override
([#935](https://github.com/NCATSTranslator/Babel/issues/935); the story is in
[README.md](./README.md#the-override-that-had-to-become-a-vote)). The effect was measured before the
merge by *replaying* `create_typed_sets` over a finished build
([`replay_type_vote.txt`](./replay_type_vote.txt)), which can only see cliques the build already
produced. This page records the real build-vs-build
[`babel-clique-diff`](../../../tools/CliqueDiff.md) that closes that gap — the one the PR's own
"Still to do before merge" section asked for.

Artifacts in [`clique-diff/`](./clique-diff/), reduced from a 31,567-row, 9.2 MB CSV to under
100 KB by [`scripts/reduce_clique_diff.py`](./clique-diff/scripts/reduce_clique_diff.py), which
also prints every aggregate quoted below:

| file | rows | what it is |
|---|---|---|
| `clique-diff.summary.json` | — | the tool's own summary, verbatim |
| `food-moves.csv` | 8 of 8 | every clique that left `Food.txt` — complete, no cap |
| `drug-moves-top-100.csv` | 100 of 4,269 | ranked slice of the `Drug` demotions |
| `leader-churn-top-100.csv` | 100 of 27,290 | ranked slice of the PubChem leader flips |
| `reduce_clique_diff.txt` | — | the script's own output, capturing every aggregate below |

Every count on this page comes from one of those two files. The aggregates are computed over the
full CSV, which is *not* committed, so `reduce_clique_diff.txt` is what keeps them checkable now
that the CSV is gone — the same role [`replay_type_vote.txt`](./replay_type_vote.txt) plays for its
script.

## Headline: the intended retype, and nothing else moved

**No identifier was dropped, and no clique gained or lost a member.** Across all eight chemical
compendia the diff contains only two kinds of row — every change is a clique being retyped into a
different file, or a clique keeping its exact membership under a different preferred identifier:

| `destination_kind` | rows | meaning |
|---|---|---|
| `moved` | 4,277 | retyped into a different compendium file |
| `leader_changed` | 27,290 | same members, different preferred identifier |
| `kept` | 0 | — |
| `regrouped` | 0 | no member redistributed to another clique |
| `dropped` | 0 | **no identifier left the output** |

`before_size == after_size == member_count` on all 31,567 rows, and all 31,567 `before_leader`
values are distinct.

Clique counts per compendium:

| compendium | before | after | diff |
|---|---|---|---|
| `SmallMolecule.txt` | 112,039,192 | 112,039,197 | +5 |
| `MolecularMixture.txt` | 11,625,299 | 11,625,301 | +2 |
| `ChemicalEntity.txt` | 358,496 | 362,764 | +4,268 |
| `Drug.txt` | 266,598 | 262,329 | −4,269 |
| `ComplexMolecularMixture.txt` | 571 | 573 | +2 |
| `ChemicalMixture.txt` | 490 | 490 | 0 |
| `Food.txt` | 293 | 285 | −8 |
| `Polypeptide.txt` | 5 | 5 | 0 |

Every one of those deltas is accounted for by the 4,277 `moved` rows, with nothing left over:

- `Drug` −4,269 = 4,268 cliques becoming `ChemicalEntity` + 1 becoming `ComplexMolecularMixture`;
- `Food` −8 = 5 → `SmallMolecule`, 2 → `MolecularMixture`, 1 → `ComplexMolecularMixture`;
- so `ComplexMolecularMixture` +2 = 1 from `Drug` + 1 from `Food`, and `SmallMolecule` +5 and
  `MolecularMixture` +2 are the Food arrivals.

`ChemicalMixture` and `Polypeptide` are untouched. **This is the completeness check**: a retype that
also lost identifiers, split a clique, or disturbed an unrelated compendium would show up here as a
`dropped`, `regrouped`, or unreconciled count, and none of it did.

## The eight `Food.txt` cliques: the replay was exactly right

[`replay_type_vote.txt`](./replay_type_vote.txt) predicted, from `babel-1.18`'s intermediates alone,
that 8 of 293 `Food.txt` cliques would leave — 5 to `SmallMolecule`, 2 to `MolecularMixture`, 1 to
`ComplexMolecularMixture` — holding 4+7+7+20+14+14+24+12 = **102 identifiers**, with nothing
dropped. The real build moved **8 cliques and 102 members**, into exactly those types, with each
clique's leader and label unchanged and `dropped_member_count` zero. Same cliques, same counts.

That is worth recording beyond this PR: `docs/sources/CLAUDE.md` recommends replaying a pipeline
function over a finished build's `intermediate/` instead of rebuilding, and this is the first time
that technique's prediction has been checked against a real build. It matched member-for-member.

All eight are in [`clique-diff/food-moves.csv`](./clique-diff/food-moves.csv). They are the
`babel-1.18` mis-typings the vote exists to fix:

| clique leader | label | now typed |
|---|---|---|
| [`CHEBI:17234`](http://purl.obolibrary.org/obo/CHEBI_17234) | "glucose" | `biolink:SmallMolecule` |
| [`CHEBI:27300`](http://purl.obolibrary.org/obo/CHEBI_27300) | "vitamin D" | `biolink:SmallMolecule` |
| [`CHEBI:27013`](http://purl.obolibrary.org/obo/CHEBI_27013) | "tocopherol" | `biolink:SmallMolecule` |
| [`CHEBI:140618`](http://purl.obolibrary.org/obo/CHEBI_140618) | "castor oil" | `biolink:SmallMolecule` |
| [`CHEBI:15444`](http://purl.obolibrary.org/obo/CHEBI_15444) | "(1->4)-alpha-D-glucan" | `biolink:SmallMolecule` |
| [`CHEBI:25681`](http://purl.obolibrary.org/obo/CHEBI_25681) | "omega-3 fatty acid" | `biolink:MolecularMixture` |
| [`CHEBI:36009`](http://purl.obolibrary.org/obo/CHEBI_36009) | "omega-6 fatty acid" | `biolink:MolecularMixture` |
| [`DRUGBANK:DB10514`](https://go.drugbank.com/drugs/DB10514) | "Cantaloupe" | `biolink:ComplexMolecularMixture` |

The D-glucose clique is the reported bug and behaves as intended: it is a `SmallMolecule` again and
still holds [`DRUGBANK:DB09341`](https://go.drugbank.com/drugs/DB09341) "Dextrose, unspecified
form", the structureless food row that had dragged the whole clique to `Food`. Cantaloupe is the
one that leaves via the extract path rather than a structural vote.

The remaining 285 cliques stay `biolink:Food`, including the ones that should — inulin is both a
food and a GFR diagnostic.

## The 4,269 `Drug` demotions: the type order doing its job

The larger group is not the food change at all; it is the other half of #948, which moved the type
precedence into `config.yaml` and reordered it. `create_typed_sets` breaks a tied type vote with
`(-count, order.index(type))` (`src/createcompendia/chemicals.py`), so putting `biolink:Drug` below
`biolink:ChemicalEntity` flips every clique whose vote was tied between the two. The PR anticipated
this ("expect thousands rather than millions… worth eyeballing the `Drug` demotions specifically").

What moved is uniform and unstructural — these are RxNorm formulation stubs, not molecules:

| leader prefixes | rows |
|---|---|
| `RXCUI` → `UMLS` | 4,242 |
| `UMLS` → `MESH` | 23 |
| `RXCUI` → `MESH` | 4 |

4,263 of the 4,269 cliques have exactly two members and six have three — small enough that the CSV's
five-member `example_members` sample is the *whole* membership, so this is exhaustive rather than a
sample: across all 4,269 cliques the only prefixes present are `UMLS` (4,269), `RXCUI` (4,246),
`MESH` (27) and `CAS` (2). No CHEBI, PubChem, DRUGBANK, CHEMBL or InChIKey member anywhere.
`config.yaml`'s own rationale is why this is the wanted direction: `biolink:Drug` "comes almost
entirely from RxNorm drug formulations — a level of detail neither Babel nor Translator needs", and
ranking it above `ChemicalEntity` would put `RXCUI` CURIEs ahead of real chemical entities.

Two rows are worth an eye, both in
[`drug-moves-top-100.csv`](./clique-diff/drug-moves-top-100.csv) (which ranks the rare destination
first, so both sort to the top):

- **The lone non-`ChemicalEntity` destination.** `UMLS:C0058773` / `MESH:D022422`
  "Diphtheria-Tetanus Vaccine" went to `ComplexMolecularMixture` while all 4,268 siblings went to
  `ChemicalEntity`. Defensible for a vaccine, and it is the only one.
- **One clique's leader is now nameless.** `RXCUI:2711790` → `MESH:C000731746`, which has no label,
  although `UMLS:C6003442` "Symbravo" sits in the same clique. It is the single row in the whole
  diff with an empty `destination_label`, and a consequence of prefix priority rather than of this
  change; `choose_preferred_name()` may still recover the name downstream.

**The accepted `Drug`-below-`Food` tradeoff is still unexercised.** `chemical_type_order` ranks
`Food` above `Drug`, so a clique that votes `Drug` *and* carries food evidence would be typed
`Food` — mildly wrong, and accepted rather than special-cased (see
[README.md](./README.md#the-override-that-had-to-become-a-vote)). In this build no row anywhere
moves *into* `Food.txt`: `Food` only loses cliques. The premise behind
`test_food_evidence_beats_a_drug_vote` therefore now holds against a real build, not just a replay.

## The 27,290 leader flips this PR did not cause

The rest of the diff — 16,250 `MolecularMixture` and 11,040 `SmallMolecule` rows — is cliques whose
membership and Biolink type are *byte-identical* on both sides, but whose preferred identifier
changed. They are not attributable to #948, and they are not a property of these two builds so much
as of any two runs:

- every flip is `PUBCHEM.COMPOUND` → `PUBCHEM.COMPOUND` (27,290 of 27,290);
- both leaders carry the **same label** in 27,225 of them;
- the direction is a coin flip — neither the numerically nor the lexicographically smaller CID wins.

`chemical_type_order` cannot reorder two identifiers of the same prefix inside an unchanged,
same-typed clique, and both sides were built from the same intermediates. The cause is
`pubchemsort()` in `src/node.py`, which keys a dict on the *label*
(`pclabels[lid.label.upper()] = lid.identifier`): when a clique holds two PubChem CIDs with the same
label — precisely the case that function exists to handle — every CID but the last-visited one is
discarded before any sorting happens, and "last" is the iteration order of a `frozenset` of strings,
which Python randomises per process.

For contrast, the disease/phenotype and anatomy clique diffs committed elsewhere in this repo report
`leader_changed_count: 0` across ~615,000 cliques between them. The churn is specific to
PubChem-led cliques. It is filed as
[#1027](https://github.com/NCATSTranslator/Babel/issues/1027); the ~27k figure here is what a fix
should reduce to roughly zero, and
[`leader-churn-top-100.csv`](./clique-diff/leader-churn-top-100.csv) is the evidence, ranked so the
65 flips that *also* changed the displayed label come first.

## What was compared

Both sides are the same build directory; only the chemical compendium step differs.
`2026jul15/` was copied wholesale (all 126 GB written in one 18-minute window), while in
`2026jul21/` every non-chemical compendium keeps its original mtime and only the eight chemical
files were rewritten — by a re-run of the chemical rules under #948, over unchanged
`intermediate/chemicals`. The diff's own result is the confirmation: with zero membership changes in
either direction, no upstream data can have moved between them.

All eight `chemical_outputs` must be passed in one run. A retype is only visible as `moved` when
both the before- and after-type's files are compared; leaving one out would turn these 4,277 real
retypes into phantom `dropped` rows.

## Reproducing

```bash
srun --mem=1400G --time "24:0:0" /usr/bin/time -v \
uv run babel-clique-diff \
    --before ../pr-948-diff/2026jul15 --after ../pr-948-diff/2026jul21 \
    --files SmallMolecule.txt MolecularMixture.txt Polypeptide.txt ComplexMolecularMixture.txt \
            ChemicalMixture.txt ChemicalEntity.txt Drug.txt Food.txt \
    --before-label "babel-2026jul15" --after-label "babel-2026jul21" \
    --note "isolates PR #935: food evidence votes instead of forcing, + chemical_type_order" \
    --out-csv ../pr-948-diff/clique-diff.csv --out-json ../pr-948-diff/clique-diff.summary.json

uv run python docs/sources/DRUGBANK/food-and-extracts/clique-diff/scripts/reduce_clique_diff.py \
    ../pr-948-diff/clique-diff.csv --out-dir docs/sources/DRUGBANK/food-and-extracts/clique-diff
```

That run took 45m30s and peaked at 202.9 GiB — `--mem=1400G` was about seven times more than it
needed, and `--mem=256G` is the right ask. See
[`docs/tools/CliqueDiff.md`](../../../tools/CliqueDiff.md) ("Resource cost") for the sizing rule the
run produced.

The full 9.2 MB CSV is not committed; re-run the command above to regenerate it, or read the
reductions in [`clique-diff/`](./clique-diff/), which carry every number quoted on this page.

## Related work

- [#828](https://github.com/NCATSTranslator/Babel/issues/828) /
  [#918](https://github.com/NCATSTranslator/Babel/pull/918) — the original DrugBank food/extract
  retype, and the clique-level override this diff verifies the removal of.
- [#935](https://github.com/NCATSTranslator/Babel/issues/935) /
  [#948](https://github.com/NCATSTranslator/Babel/pull/948) — the vote and `chemical_type_order`.
- [#1027](https://github.com/NCATSTranslator/Babel/issues/1027) — the PubChem leader churn this diff
  measured, and [#1028](https://github.com/NCATSTranslator/Babel/issues/1028) — the 203 GiB peak the
  run cost.
- [#894](https://github.com/NCATSTranslator/Babel/issues/894) /
  [#901](https://github.com/NCATSTranslator/Babel/pull/901) — the other "two builds of the same data
  disagree" family, in `build_sets()` rather than in leader selection.
- [`docs/sources/MP/disjointness.md`](../../MP/disjointness.md) and
  [`docs/sources/EMAPA/clique-diff.md`](../../EMAPA/clique-diff.md) — the other two committed
  build-vs-build diffs, and the reconciliation pattern this page follows.

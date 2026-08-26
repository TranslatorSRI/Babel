# Clique diff: disease before and after adding GARD

The [source-impact report](./impact-report.md) models what *adding* GARD contributes, but by
construction it only walks after-cliques that contain a GARD CURIE. It therefore cannot report
before-cliques that split, lose members, or disappear (see
[#895](https://github.com/NCATSTranslator/Babel/issues/895)) — and this addition does restructure
existing cliques, so that gap is load-bearing here. This page records the build-vs-build
[`babel-clique-diff`](../../tools/CliqueDiff.md) that closes it.

Artifacts in [`on-addition/`](./on-addition/): `clique-diff.summary.json`, and
`clique-diff-regrouped.csv` — every one of the 29 rows that is not a plain `kept`, which is the
reduction worth reading. The full per-row CSV is 16,133 rows, almost all `kept`, and is not
committed.

Three diffs are recorded here. The first is the whole PR against `main`, which is the number a
reviewer wants; the other two decompose it, in the order the work happened. Diff 1 is the GARD
**ingest** — `main` against this branch with its ids file, labels and the `MONDO_GARD` concord, and
no `GARD_label`. Diff 2 isolates `GARD_label` on top of it.

## Diff 0: the whole PR

`main` at `dd9b07cf` against this branch at `1438ddee`, both built from the same cached
`babel_downloads/` and the same disease `ids/`, with only the code and configuration differing.
Summary in [`whole-pr/clique-diff.summary.json`](./whole-pr/clique-diff.summary.json).

| compendium | before | after | diff |
| --- | ---: | ---: | ---: |
| `Disease.txt` | 365,087 | 365,080 | **−7** |
| `PhenotypicFeature.txt` | 75,477 | 75,477 | 0 |

**Adding 16,214 identifiers makes the disease compendium seven cliques smaller.** That is the whole
argument for the two concords: GARD's terms merge into cliques that already exist rather than
forming new ones beside them, and the 265 `GARD_label` merges more than cancel the 258 cliques the
ingest adds.

| destination_kind | rows | members | meaning |
| --- | ---: | ---: | --- |
| `kept` | 16,353 | 109,340 | an existing clique gained GARD identifiers and kept its leader |
| `regrouped` | 29 | 248 | members moved to a different leader in the same compendium |
| `leader_changed` | 0 | 0 | no clique's preferred identifier was reassigned |
| `moved` | 0 | 0 | no member retyped into a different compendium file |
| `dropped` | 0 | 0 | **no identifier disappeared from the compendia** |

Checked directly as well as through the diff, since `babel-clique-diff` cannot see a CURIE absent
from both sides: the two compendia hold 746,552 identifiers on `main` and 763,060 on this branch,
the 16,508 added are **all** `GARD:`, and **not one `main` identifier is missing**.

The 29 `regrouped` rows are byte-for-byte the same 29 as diff 1 below. `GARD_label` restructures
nothing of its own — every one of its 265 rows attaches a single-identifier GARD clique to an
existing one — so all of this PR's clique restructuring comes from the ingest and `MONDO_GARD`, and
is analysed there.

Two other properties worth stating because they are easy to assume rather than check:

- **Every non-DOID disease concord is byte-identical between the two builds** (`MONDO`,
  `MONDO_close`, `HP`, `MP`, `UMLS`, `EFO`, `Manual`), including after re-running the UberGraph walk
  on `main`'s code. `DOID` has the same 39,264 rows on both sides and differs only in how its GARD
  targets are spelled. So the diff isolates the intended inputs and nothing else.
- **`extra_prefixes` scoping changes nothing in this build.** Scoping the list to `biolink:Disease`
  closes a hole rather than fixing an active leak: `ICD10CM` appears 2,012 times in `Disease.txt`
  and zero times in `PhenotypicFeature.txt` on *both* sides, so no identifier was going through it
  today. See [`README.md`](README.md), "Biolink registration".

## Diff 1: adding the GARD ingest

### Headline: GARD joins existing cliques rather than forming its own

| compendium | before | after | diff |
| --- | ---: | ---: | ---: |
| `Disease.txt` | 365,087 | 365,345 | **+258** |
| `PhenotypicFeature.txt` | 75,477 | 75,477 | 0 |

16,214 identifiers arrive and the disease compendium grows by 258 cliques (+0.07%), because 16,102
of the 16,379 cliques that end up holding a GARD identifier are cliques that already existed. Only
277 registry terms are new concepts to Babel.

That ratio is the whole point of the `MONDO_GARD` concord. Without it — reading only DOID's
xrefs — the same 16,214 identifiers produce **14,319** single-identifier cliques and grow
`Disease.txt` by 3.92%, every one of them a second clique for a concept MONDO already names. See
[`docs/AddingNewSources.md`](../../AddingNewSources.md) ("Prefer joining an existing clique") and
[`docs/sources/MONDO/README.md`](../MONDO/README.md).

`PhenotypicFeature.txt` is untouched, as expected: `disease_extra_prefixes` is a per-class allowlist
applied to `biolink:Disease` only, so no GARD CURIE can survive in a phenotype clique.

### Nothing is lost; 29 cliques are restructured

| destination_kind | rows | members | meaning |
| --- | ---: | ---: | --- |
| `kept` | 16,104 | — | member stayed under the same leader |
| `regrouped` | 29 | 248 | members moved to a different leader in the same compendium |
| `leader_changed` | 0 | 0 | no clique's preferred identifier was reassigned |
| `moved` | 0 | 0 | no member retyped into a different compendium file |
| `dropped` | 0 | 0 | **no identifier disappeared from the compendia** |

The 29 `regrouped` rows are in
[`on-addition/clique-diff-regrouped.csv`](./on-addition/clique-diff-regrouped.csv) and fall into two
shapes.

#### Shape 1 — a stranded MONDO term rejoins its disease (19 rows)

A DOID-led clique gains exactly two members: a MONDO identifier and a GARD one. These are cliques
where MONDO and DOID describe the same disease but MONDO asserted no `skos:exactMatch` to it, so the
MONDO term sat alone in its own clique. GARD is the bridge: DOID xrefs the GARD id, MONDO xrefs the
same GARD id, and the two cliques join. The labels agree on both sides:

| before leader | joins | after size |
| --- | --- | ---: |
| [`DOID:0050465`](http://purl.obolibrary.org/obo/DOID_0050465) "Muir-Torre syndrome" (7) | [`MONDO:0008018`](http://purl.obolibrary.org/obo/MONDO_0008018) "Muir-Torre syndrome" | 9 |
| [`DOID:0070026`](http://purl.obolibrary.org/obo/DOID_0070026) "Revesz syndrome" (6) | [`MONDO:0009990`](http://purl.obolibrary.org/obo/MONDO_0009990) "Revesz syndrome" | 8 |
| [`DOID:5572`](http://purl.obolibrary.org/obo/DOID_5572) "Beckwith-Wiedemann syndrome" (8) | [`MONDO:0007534`](http://purl.obolibrary.org/obo/MONDO_0007534) "Beckwith-Wiedemann syndrome" | 10 |

This is a **fix**: before this PR each of these diseases had two Babel cliques, and Node
Normalization answered differently depending on which identifier you held.

#### Shape 2 — a pre-existing over-merge splits (10 rows)

A clique that had absorbed a second disease sheds it. `glom()` cannot hold two MONDO identifiers
(`DISEASE_UNIQUE_PREFIXES`), so where the new GARD edges give a better-supported home to members
that were only loosely attached, those members move. The largest:

| before leader | members move to | count |
| --- | --- | ---: |
| [`MONDO:0010029`](http://purl.obolibrary.org/obo/MONDO_0010029) "situs inversus" (34) | [`MONDO:0001734`](http://purl.obolibrary.org/obo/MONDO_0001734) "tuberous sclerosis" | 19 |
| [`MONDO:0016063`](http://purl.obolibrary.org/obo/MONDO_0016063) "Cowden disease" (30) | [`MONDO:0017623`](http://purl.obolibrary.org/obo/MONDO_0017623) "PTEN hamartoma tumor syndrome" | 15 |
| [`MONDO:0006365`](http://purl.obolibrary.org/obo/MONDO_0006365) "Peutz-Jeghers polyp" (18) | [`MONDO:0008280`](http://purl.obolibrary.org/obo/MONDO_0008280) "Peutz-Jeghers syndrome" | 12 |
| [`MONDO:0008145`](http://purl.obolibrary.org/obo/MONDO_0008145) "Ollier disease" (22) | [`MONDO:0013808`](http://purl.obolibrary.org/obo/MONDO_0013808) "Maffucci syndrome" | 7 |
| [`MONDO:0016755`](http://purl.obolibrary.org/obo/MONDO_0016755) "neurofibroma" (25) | [`MONDO:0021061`](http://purl.obolibrary.org/obo/MONDO_0021061) "neurofibromatosis" | 6 |

Each of these separates two things that should not have been one clique — a syndrome from the polyp
it produces, a hamartoma syndrome from Cowden disease, tuberous sclerosis from situs inversus. **The
over-merges predate this PR and ship today**; GARD's mappings are what pull them apart. They are the
strongest single argument for the `MONDO_GARD` concord, and the rows an SME should read first: the
full 29 are in the CSV with before/after sizes and example members.

#### Two splits that were wrong, and the filter that undoes them

An earlier run of this diff had 31 rows. The two no longer present were splits in the *wrong*
direction, caused by one GARD id being xrefed by two DOID terms: `GARD:625` "Autosomal recessive
Alport syndrome" is cited by both [`DOID:0110033`](http://purl.obolibrary.org/obo/DOID_0110033)
"Alport syndrome 2" and [`DOID:0051080`](http://purl.obolibrary.org/obo/DOID_0051080) "Alport
syndrome 3B", which pulled 3B out of
[`MONDO:0957811`](http://purl.obolibrary.org/obo/MONDO_0957811) "Alport syndrome 3b, autosomal
recessive" and into the general autosomal recessive clique; `GARD:7674` "Spinal muscular atrophy"
did the same to [`DOID:0060160`](http://purl.obolibrary.org/obo/DOID_0060160) "childhood spinal
muscular atrophy", pulling it out of
[`MONDO:0009673`](http://purl.obolibrary.org/obo/MONDO_0009673) "spinal muscular atrophy, type II".
Twelve of DOID's GARD targets are claimed by two or more DOID terms in this way. DOID's concord is
now overuse-filtered on GARD as well as ICD (`OVERUSE_FILTERED_CONCORDS["DOID"]` in
`src/createcompendia/diseasephenotype.py`), which drops all twelve from DOID's concord; MONDO's own
mapping still places every one of them, and both DOID terms stay in their MONDO cliques.

The only visible cost is five *retired* GARD ids — `GARD:7220`, `GARD:8609`, `GARD:9226`,
`GARD:9948` and `GARD:9971` — that the registry no longer publishes and that only DOID cited, each
from two terms. With no registry row and no MONDO mapping, nothing
else carries them, so they are no longer emitted; they had no label and there was no way to tell
which of their two DOID subjects they meant.

### The mistyped DOID xref this also fixes

[`DOID:0061030`](http://purl.obolibrary.org/obo/DOID_0061030) "hemophilia" writes its GARD xref as
`GARD:0418`, a typo for [`GARD:10418`](https://rarediseases.info.nih.gov/?gard_id=10418)
"Hemophilia" — MONDO independently maps
[`MONDO:0018660`](http://purl.obolibrary.org/obo/MONDO_0018660) "hemophilia" to `GARD:10418`, which
confirms the diagnosis. Unpadded, DOID's typo becomes `GARD:418` "Essential pentosuria", which
[`DOID:0111258`](http://purl.obolibrary.org/obo/DOID_0111258) "pentosuria" also xrefs.

The two cliques do not merge — both hold a MONDO identifier — but the contested id goes to whichever
concord claims it first. With DOID's concord alone, hemophilia's row came first, so the hemophilia
clique carried an identifier labelled "Essential pentosuria" while pentosuria got none. Two general
rules now settle it (see [`README.md`](README.md), "The one xref that is not padding"): `MONDO_GARD`
is glommed before `DOID` and maps pentosuria to `GARD:418`, and DOID's concord is overuse-filtered
on GARD, which drops both DOID rows. Reported upstream as
[DiseaseOntology#1620](https://github.com/DiseaseOntology/HumanDiseaseOntology/issues/1620).

**Neither standard artifact can see that bug**, which is worth recording as a property of the
tooling rather than of this change. The impact report only knows `GARD:418` joined *an* existing
clique — landing in the wrong one is not a category it has. And a main-vs-branch clique diff is
byte-identical with and without the fix, because `GARD:418` is a *new* identifier on both sides and
`babel-clique-diff` classifies *before*-clique members, none of which move. Diffing the branch
against itself with the entry disabled does show it, as one `regrouped` row:

| before clique | destination | kind | members |
|---|---|---|---|
| `MONDO:0018660` "hemophilia" (11) | `MONDO:0009846` "pentosuria" (12) | `regrouped` | 1 — `GARD:418` "Essential pentosuria" |

What surfaced it was reading the two cliques out of the finished compendia directly, which is the
rule [`AGENTS.md`](../../../AGENTS.md) states for clique-membership questions.

## Diff 2: adding the GARD_label concord

The ingest above leaves 277 registry terms that neither MONDO nor DOID maps as single-identifier
cliques. The `GARD_label` concord links 265 of them to an identically labelled identifier already in
the pipeline; [`label-matches/README.md`](label-matches/README.md) is the evidence for the match
rule, and [`label-matches/label-matches.csv`](label-matches/label-matches.csv) is the per-row
record, so only the summary is committed here:
[`label-matches/clique-diff.summary.json`](./label-matches/clique-diff.summary.json).

| compendium | before | after | diff |
| --- | ---: | ---: | ---: |
| `Disease.txt` | 365,345 | 365,080 | **−265** |
| `PhenotypicFeature.txt` | 75,477 | 75,477 | 0 |

| destination_kind | rows | meaning |
| --- | ---: | --- |
| `kept` | 263 | a clique gained a GARD member and kept its leader |
| `regrouped` | 265 | a GARD single-identifier clique merged into an existing clique |
| `leader_changed` | 0 | no clique's preferred identifier was reassigned |
| `moved` | 0 | no member retyped into a different compendium file |
| `dropped` | 0 | **no identifier disappeared from the compendia** |

263 rather than 265 destinations because two cliques gain two GARD ids each, where the registry
carries the same label twice. All 16,214 GARD identifiers still reach a compendium.

### The dropped-members run that produced guard 3

The first implementation of this concord reported **5 dropped members** on this diff, and that is
how `build_gard_label_concord()`'s third guard came to exist. Five registry terms matched an
identifier whose clique Babel types `biolink:PhenotypicFeature` — Cementoblastoma, Ileal Atresia,
Phocomelia of the Lower Limb, Chilblains, Myokymia. `disease_extra_prefixes` is a per-class
allowlist naming GARD for `biolink:Disease` only, so those GARD ids were not moved into
`PhenotypicFeature.txt`; they were dropped from the build, trading a working single-identifier
clique for a vanished identifier.

Nothing else would have caught it. The impact report cannot see a dropped identifier at all — a
CURIE absent from both sides is not a difference — and the concord looked correct on disk. Guard 3
skips those five targets, and the diff above is the result.

## What was compared

Both sides were built from the **same cached intermediates**
(`babel_outputs/intermediate/disease/`), with only the code and configuration under test changing:

| | before | after |
| --- | --- | --- |
| `whole-pr/` (diff 0) | `main` at `dd9b07cf`, rebuilt in a worktree from the same cached intermediates | this branch at `1438ddee` |
| `on-addition/` (diff 1) | `main` at `a3ae3e4d` — no GARD ingest, no `MONDO_GARD` concord, DOID concord built without GARD unpadding | this branch, before `GARD_label` |
| `label-matches/` (diff 2) | this branch with `GARD_label` removed from `disease_concords` | this branch |
| the isolating diff (not committed) | this branch, with the `DOID:0061030 GARD:418` concord row kept | this branch, row dropped |

Reproduce with:

```bash
uv run snakemake -c 4 babel_outputs/compendia/Disease.txt \
    --forcerun get_disease_obo_relationships get_disease_doid_relationships
uv run babel-clique-diff --before <before-dir> --after <after-dir> \
    --files Disease.txt PhenotypicFeature.txt \
    --out-csv clique-diff.csv --out-json clique-diff.summary.json
```

Put the target *before* `--forcerun`: `--forcerun` takes a list, so a target written after it is
swallowed as another rule name and Snakemake falls back to building the entire pipeline.

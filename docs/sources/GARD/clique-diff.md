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
| `Disease.txt` | 365,087 | 365,075 | **−12** |
| `PhenotypicFeature.txt` | 75,477 | 75,477 | 0 |

**Adding 16,214 identifiers makes the disease compendium twelve cliques smaller.** That is the whole
argument for the two concords: GARD's terms merge into cliques that already exist rather than
forming new ones beside them, and the 270 `GARD_label` merges more than cancel the 258 cliques the
ingest adds.

| destination_kind | rows | members | meaning |
| --- | ---: | ---: | --- |
| `kept` | 16,359 | — | an existing clique gained GARD identifiers and kept its leader |
| `regrouped` | 29 | 248 | members moved to a different leader in the same compendium |
| `leader_changed` | 0 | 0 | no clique's preferred identifier was reassigned |
| `moved` | 0 | 0 | no `main` member retyped into a different compendium file |
| `dropped` | 0 | 0 | **no identifier disappeared from the compendia** |

Checked directly as well as through the diff, since `babel-clique-diff` cannot see a CURIE absent
from both sides: the two compendia hold 746,552 identifiers on `main` and 763,061 on this branch,
the 16,509 added are **all** `GARD:`, and **not one `main` identifier is missing**. Six of those
GARD identifiers are in `PhenotypicFeature.txt`, where GARD names a concept HP also names; the rest
are in `Disease.txt`.

The 29 `regrouped` rows are byte-for-byte the same 29 as diff 1 below. `GARD_label` restructures no
pre-existing clique of its own — each of its 270 rows attaches a single-identifier GARD clique to an
existing one — so all of this PR's clique restructuring comes from the ingest and `MONDO_GARD`, and
is analysed there.

Two other properties worth stating because they are easy to assume rather than check:

- **Every non-DOID disease concord is byte-identical between the two builds** (`MONDO`,
  `MONDO_close`, `HP`, `MP`, `UMLS`, `EFO`, `Manual`), including after re-running the UberGraph walk
  on `main`'s code. `DOID` has the same 39,264 rows on both sides and differs only in how its GARD
  targets are spelled. So the diff isolates the intended inputs and nothing else.
- **The per-class allowlist changes nothing for `ICD10CM` and everything for six GARD identifiers.**
  Keying `extra_prefixes` by Biolink class stops a list argued for one class being granted to
  another; for `ICD10CM` that is a hole closed rather than a leak fixed, since it appears 2,012
  times in `Disease.txt` and zero times in `PhenotypicFeature.txt` on *both* sides. For GARD it is
  load-bearing: naming GARD under `biolink:PhenotypicFeature` as well is what lets six identifiers
  survive in the phenotype cliques they belong to instead of being deleted. See
  [`README.md`](README.md), "Biolink registration".

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

`PhenotypicFeature.txt` is untouched at this stage: the ingest's GARD identifiers all land in
disease cliques. Six reach phenotype cliques once `GARD_label` runs — see diff 2.

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

A clique that had absorbed a second disease sheds it. Worth being precise about how, because GARD
does not repair the bad cross-reference that caused the merge — it makes it **inert**. The
over-merge survives on `main` only because one side of it has no MONDO identifier to collide with;
once MONDO's GARD mappings give both sides one, `DISEASE_UNIQUE_PREFIXES` refuses the union and the
two diseases fall apart. The largest:

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

**They are improvements, not repairs, and two of them are visibly partial.** After the neurofibroma
split, `MONDO:0021061` "neurofibromatosis" takes only `DOID:8712` and one UMLS concept, while
`MESH:D017253` "Neurofibromatoses", `NCIT:C6727` "Neurofibromatosis" and `UMLS:C0162678`
"Neurofibromatoses" all stay behind in the *neurofibroma* clique — which is still wrong, just less
wrong than one fused clique. Peutz-Jeghers is cleaner but leaves `NCIT:C7755` "Peutz-Jeghers Polyp
of the Small Intestine" on the syndrome side. GARD moves what its own mappings reach and nothing
else; the bad cross-references are all still there.

#### What causes the ten over-merges

Traced by rebuilding `main`'s clique for each row and finding the edges that connect the members
that move to the members that stay. **Every crossing in all ten is asserted by the `DOID` concord**
— not one comes from MONDO, UMLS, HP, EFO or Manual. Beyond that they split into two kinds, and the
distinction decides what can be done about them:

**Five involve an overused xref target** — one identifier claimed by two or more DOID subjects,
which is what `remove_overused_xrefs` is for:

| target | claimed by | fuses |
| --- | --- | --- |
| `SNOMEDCT:157033002` | 2 DOID terms | situs inversus + tuberous sclerosis |
| `MESH:D006223` "Hamartoma Syndrome, Multiple" | 2 DOID terms | Cowden disease + PTEN hamartoma tumor syndrome |
| `MESH:D016715` "Proteus Syndrome" | 2 DOID terms | Cowden disease + Proteus syndrome |
| `MESH:D011546` "Pseudohypoaldosteronism" | **3** DOID terms | pseudohypoaldosteronism + its type IB1 subtype |

These are #1032's territory, and unfiltered only because `OVERUSE_FILTERED_CONCORDS["DOID"]` is
scoped to ICD and GARD.

**Five are a single DOID xref crossing a granularity boundary**, claimed by exactly one DOID term
each — so counting subjects reveals nothing and **the overuse filter can never catch them**:

- [`DOID:3852`](http://purl.obolibrary.org/obo/DOID_3852) "Peutz-Jeghers syndrome" xrefs
  `NCIT:C4733` "Peutz-Jeghers **Polyp**" and `UMLS:C0456487` — a syndrome asserted equal to the
  lesion it produces.
- [`DOID:4624`](http://purl.obolibrary.org/obo/DOID_4624) "Ollier disease" xrefs `NCIT:C3213` and
  `UMLS:C0024454` "**Maffucci** Syndrome" — two distinct conditions.
- [`DOID:8712`](http://purl.obolibrary.org/obo/DOID_8712) "neurofibromatosis" xrefs MeSH, NCIT and
  UMLS "Neurofibromato**ses**" while sitting in the neurofibro**ma** clique.
- [`DOID:10041`](http://purl.obolibrary.org/obo/DOID_10041) "dysplastic nevus syndrome" ↔
  `MESH:D004416`, pulling familial atypical multiple mole melanoma syndrome into cutaneous melanoma.
- [`DOID:0050787`](http://purl.obolibrary.org/obo/DOID_0050787) "juvenile polyposis syndrome" ↔
  `MESH:C537702` / `OMIM:174900`, pulling it into chromosome 10q23 deletion syndrome.

That second kind is a wrong equivalence asserted once, which no counting rule detects; it needs
either upstream correction or a bad-xrefs entry. Neither kind is fixed by this PR, and both would
re-fuse their cliques if MONDO stopped mapping the diseases to different GARD terms.

##### Worked example: why tuberous sclerosis was in the situs inversus clique

The first row is the one to read, because nothing about it is obvious from the CSV: tuberous
sclerosis has nothing to do with situs inversus, and the after-clique looks entirely sensible.

On `main`, `MONDO:0010029` "situs inversus" leads a 39-member clique holding *both* diseases. Every
edge between the two halves runs through a single identifier:

```text
DOID:13515 "tuberous sclerosis"     --xref--> SNOMEDCT:157033002
DOID:758   "visceral heterotaxy 5"  --xref--> SNOMEDCT:157033002
```

One SNOMED code claimed by two DOID terms. [`DOID:758`](http://purl.obolibrary.org/obo/DOID_758)
"visceral heterotaxy 5" genuinely belongs with situs inversus;
[`DOID:13515`](http://purl.obolibrary.org/obo/DOID_13515) "tuberous sclerosis" does not, and it
drags 18 tuberous-sclerosis identifiers in with it. Removing either edge separates the two
diseases; nothing else joins them.

This is exactly the failure `remove_overused_xrefs` exists for — and it is not filtered, because
`OVERUSE_FILTERED_CONCORDS["DOID"]` is scoped to `DOID_ICD_XREF_PREFIXES + [GARD]` and **SNOMEDCT is
not in that list**. Scoping is what keeps the filter from savaging DOID's 1:1 SNOMED and MeSH rows,
so widening it is a decision with its own evidence, tracked as
[#1032](https://github.com/NCATSTranslator/Babel/issues/1032).

GARD ends the merge without touching that. MONDO maps the two diseases to different registry terms,
and so does DOID, both correctly:

| | MONDO's `hasDbXref` | DOID's xref |
| --- | --- | --- |
| tuberous sclerosis | `MONDO:0001734` → `GARD:7830` "Tuberous sclerosis syndrome" | `DOID:13515` → `GARD:7830` |
| situs inversus | `MONDO:0010029` → `GARD:4883` "Situs inversus" | `DOID:758` → `GARD:4883` |

`MONDO_GARD` is glommed **before** `DOID` (the load-bearing order in `disease_concords`), so by the
time DOID's rows are read, `GARD:7830` already sits with `MONDO:0001734` and `GARD:4883` with
`MONDO:0010029`. The bad SNOMED edge would now have to union two cliques that each hold a MONDO
identifier, and `glom()` refuses it. The result is 21 members under `MONDO:0001734` and 16 under
`MONDO:0010029`.

Two consequences worth keeping in view. The repair is **incidental**: it depends on MONDO mapping
both diseases to GARD, and it would come back if MONDO dropped either mapping — #1032 is still the
real fix. And `SNOMEDCT:157033002` itself does not go away; `glom()` awards a contested identifier
to whichever concord claimed it first, and here that is the tuberous sclerosis clique.

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
cliques. The `GARD_label` concord links 270 of them to an identically labelled identifier already in
the pipeline; [`label-matches/README.md`](label-matches/README.md) is the evidence for the match
rule, and [`label-matches/label-matches.csv`](label-matches/label-matches.csv) is the per-row
record, so only the summary is committed here:
[`label-matches/clique-diff.summary.json`](./label-matches/clique-diff.summary.json).

| compendium | before | after | diff |
| --- | ---: | ---: | ---: |
| `Disease.txt` | 365,345 | 365,075 | **−270** |
| `PhenotypicFeature.txt` | 75,477 | 75,477 | 0 |

| destination_kind | compendium | rows | meaning |
| --- | --- | ---: | --- |
| `regrouped` | `Disease.txt` | 265 | a GARD single-identifier clique merged into an existing disease clique |
| `kept` | `Disease.txt` | 263 | a disease clique gained a GARD member and kept its leader |
| `moved` | `Disease.txt` | 5 | a GARD term retyped out of `Disease.txt` into a phenotype clique |
| `kept` | `PhenotypicFeature.txt` | 5 | the phenotype cliques those five joined |
| `dropped` | — | 0 | **no identifier disappeared** |
| `leader_changed` | — | 0 | no clique's preferred identifier was reassigned |

263 rather than 265 `kept` destinations because two cliques gain two GARD ids each, where the
registry carries the same label twice.

### The five `moved` rows, and the guard that used to prevent them

`disease_gard_ids` types every registry term `biolink:Disease`, but five of the matched terms name
concepts HP also names — Cementoblastoma, Ileal Atresia, Phocomelia of the Lower Limb, Chilblains,
Myokymia — and the clique type vote rightly follows HP. They therefore retype into
`PhenotypicFeature.txt`, which is what the `moved` column is for: a member changing compendium is
neither a merge nor a loss, and it is the one outcome the source-impact report cannot express.

The first version of this concord refused those five links, and an earlier run of this diff is why.
While `config.yaml`'s extra-prefixes allowlist named GARD for `biolink:Disease` only, joining an
HP-led clique **deleted** the GARD identifier rather than moving it, and the diff reported
`5 dropped members`. Nothing else would have caught it — the impact report cannot see a dropped
identifier at all, since a CURIE absent from both sides is not a difference — so a guard was added
that refused any target whose clique was not `biolink:Disease`.

That guard was the wrong fix and has been removed. GARD is unregistered for *every* Biolink class,
so its exemption was never one earned on disease grounds;
`disease_extra_prefixes_by_biolink_class` now names GARD under both classes, the identifier follows
its clique, and `dropped` is 0 without anything having to refuse a correct link. The removal also
deleted the most expensive thing in `build_gard_label_concord()`: the guard had to reglom every
other concord, because the clique that types those five forms through UMLS two hops from the target,
where no label or ids-file inspection reaches it.

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

# DOID mappings: an ICD code shared by two diseases is not an equivalence

DOID cross-references ICD-10, ICD-9 and ICD-O codes with `hasDbXref`, and every concord row is fed
to `glom()` as an equivalence assertion. An ICD code often names a disease *family*, and then one
code merges every subtype that cites it. Babel drops exactly those: `remove_overused_xrefs` is
scoped to DOID's ICD prefixes (`OVERUSE_FILTERED_CONCORDS` in
`src/createcompendia/diseasephenotype.py`), so an ICD code claimed by two or more DOID terms is
dropped and a code claimed by one is kept. See issue #1029.

## The mechanism

```text
MONDO:0019064 "hereditary spastic paraplegia"
  --oio:exactMatch--> DOID:2476            (grouping term <-> grouping term, correct)
DOID:2476             --xref--> ICD10:G11.4
DOID:0110764 "hereditary spastic paraplegia 11" --xref--> ICD10:G11.4
DOID:0110782 "hereditary spastic paraplegia 31" --xref--> ICD10:G11.4
... 60 DOID terms, all citing the same code
```

`ICD10:G11.4` is "Hereditary spastic paraplegia" — one code for the whole family. Because every
subtype carries it too, `glom()` merged 61 mutually-exclusive HSP subtypes into one 223-identifier
clique. The `ICD kept` column of the table below reproduces that at 222, the two differing by a
single member the prefix renames moved.

MONDO is not at fault. Its concord's most-shared target is claimed by just two subjects — i.e.
essentially 1:1 curated exact matches — and the `MONDO:0019064 -> DOID:2476` edge is correct. Every
many-to-one edge here is DOID's. This is the failure mode [`docs/sources/CLAUDE.md`](../CLAUDE.md)
documents under "An OBO `hasDbXref` is not an equivalence".

## Why the filter is scoped to ICD, and why it is a filter rather than an exclusion

Two instruments were available and neither is right on its own, which is why the filter takes an
argument naming the namespaces it may act on:

- **`remove_overused_xrefs` unscoped** — what MONDO/HP/EFO/MP get — **over-cleans everything else.**
  It drops 1,258 rows — 618 MeSH, 279 SNOMEDCT, 148 orphanet, 88 UMLS, 76 NCIT and 42 others —
  including correct ones.
  [`MESH:D010195`](http://id.nlm.nih.gov/mesh/D010195) "Pancreatitis" is claimed by both
  [`DOID:4989`](http://purl.obolibrary.org/obo/DOID_4989) "pancreatitis" (correct) and
  [`DOID:2913`](http://purl.obolibrary.org/obo/DOID_2913) "acute pancreatitis" (too narrow) — the
  filter discards both, losing a genuine equivalence to suppress an over-broad one.
- **A categorical prefix exclusion over-cleans ICD.** Only 298 of DOID's 5,139 distinct ICD targets
  are claimed twice; **4,841 of the 6,425 ICD rows are 1:1**, and many are plainly right
  ([`ICD10:A01.0`](https://icd.who.int/browse10/2019/en#/A01.0) "Typhoid fever" ->
  [`DOID:13258`](http://purl.obolibrary.org/obo/DOID_13258) "typhoid fever"). Dropping the
  namespace deleted all 4,841 to suppress the 1,584.

Scoping the overuse filter to the ICD prefixes takes the useful half of each: only ICD is policed,
and within ICD only the codes that demonstrably name more than one DOID term. The four merge engines
go and the 1:1 rows stay.

The argument for the categorical exclusion was that a 1:1 code today is an accident of granularity —
`ICD10:A01.0` would fuse every typhoid subtype DOID might add tomorrow. The scoped filter answers
that by construction: it recounts on every build, so the row goes the day a second subtype cites it.
Paying the cost of 4,841 deleted mappings today to insure against that is the worse trade.

Measured by rebuilding the DOID concord through `build_disease_doid_relationships()` and replaying
`compute_cliques_for_impact_report()` over a complete local `disease` intermediate set. All four
columns use the current prefix rename map (below), so they differ only in ICD treatment — only one
of them is a build that will ever be made. That is the point: the table prices the three
alternatives, which a before/after clique diff structurally cannot do.

| | ICD kept | overuse-filtered | **ICD overuse-filtered** | ICD excluded |
| --- | --- | --- | --- | --- |
| identifiers | 757,770 | 757,389 | **757,474** | 752,649 |
| cliques | 440,663 | 440,618 | 440,647 | 440,647 |
| largest clique | 307 | 83 | 99 | 92 |
| cliques with >=50 identifiers | 53 | 11 | 23 | 16 |
| cliques with >=20 identifiers | 834 | 599 | 747 | 609 |
| `DOID:2476` "hereditary spastic paraplegia" (`ICD10:G11.4`, 60 claimants) | 222 | 15 | 15 | 14 |
| `DOID:8500` "hereditary retinal dystrophy" (`ICD10:H35.5`, 107) | 307 | 19 | 19 | 18 |
| `DOID:0050564` "AD nonsyndromic deafness" (`ICD10:H90.3`, 134) | 288 | 6 | 6 | 6 |

The third column is what ships. Against the categorical exclusion it **keeps 4,825 more
identifiers** — very nearly the 4,841 1:1 rows — while fixing the same mega-cliques: every probe
collapses to the same size either way, and the largest clique in the build falls from 307 to 99.

The probes are anchored on **DOID**, not MONDO. `glom()`'s `DISEASE_UNIQUE_PREFIXES` refuses a merge
that would put two MONDO ids in one clique, so in the `ICD kept` column an inflated family
*fragments* and a MONDO probe lands in an arbitrary small piece of it — reading as though the
problem were absent. DOID is unrestricted, so a DOID probe measures the merge itself.

It does leave more large cliques than the exclusion (23 vs 16 at `>=50`, 747 vs 609 at `>=20`).
That residue is real and worth naming: a DOID ICD row that is 1:1 *within DOID* can still bridge
DOID to HP or EFO, which emit ICD codes of their own — the cross-source overlap counted in
[issue #1033](https://github.com/NCATSTranslator/Babel/issues/1033). The filter only sees one
concord at a time, so it cannot catch those; that is the ICD question that remains open, not this
one.

One caution on reading the table: the `>=50` and `>=20` rows are not a like-for-like ranking of
"which is cleaner", because the unscoped filter also breaks up MeSH/SNOMED merges this change
deliberately leaves alone.

## What is dropped, and what is kept

DOID's concord carries **6,425 ICD rows** (ICD10 3,687, ICD9 2,238, ICD0 495, ICD11 5) across 5,139
distinct codes. They all stay in the concord — the filter runs at glom time — so
[`mappings/icd-targets.csv`](mappings/icd-targets.csv) lists every one with both endpoints
labelled, and `babel-overused-xrefs` can still audit them.

**Dropped: 1,584 rows on 298 codes claimed by 2+ DOID terms.** These are the merge engines:

| target | label | DOID terms citing it |
| --- | --- | --- |
| `ICD10:H90.3` | Sensorineural hearing loss, bilateral | 134 |
| `ICD10:H35.5` | Hereditary retinal dystrophy | 107 |
| `ICD10:G11.4` | Hereditary spastic paraplegia | 60 |
| `ICD10:G60.0` | Hereditary motor and sensory neuropathy | 58 |

[`ICD10:G11.4`](https://icd.who.int/browse10/2019/en#/G11.4) is the worked case: it is
"Hereditary spastic paraplegia", carried by [`DOID:2476`](http://purl.obolibrary.org/obo/DOID_2476)
and by all 60 of its subtypes, so `glom()` fused 61 mutually-exclusive diseases into one clique.

**Kept: 4,841 rows on 4,841 codes cited by exactly one DOID term.** These read as ordinary
equivalences and are the reason the categorical exclusion was replaced:

| target | label | subject |
| --- | --- | --- |
| `ICD10:A01.0` | Typhoid fever | [`DOID:13258`](http://purl.obolibrary.org/obo/DOID_13258) "typhoid fever" |
| `ICD10:G56.3` | Lesion of radial nerve | [`DOID:12170`](http://purl.obolibrary.org/obo/DOID_12170) "radial nerve lesion" |
| `ICD9:363.43` | Angioid streaks of choroid | [`DOID:979`](http://purl.obolibrary.org/obo/DOID_979) "angioid streaks of choroid" |

By prefix, the dropped 298 codes are ICD10 245, ICD0 33, ICD9 20. **58 of them have exactly one
claimant whose label matches the code's label** — a parent term sharing its code with its own
subtypes, e.g. `ICD10:G20` "Parkinson's disease" claimed by
[`DOID:14330`](http://purl.obolibrary.org/obo/DOID_14330) "Parkinson's disease" together with
Parkinson's disease 4, 19A and 23. The filter counts subjects and cannot read labels, so it takes
all four. But 47 of those 58 are also asserted by MONDO under `ICD10CM:`, and this change emits that
prefix (see "Open before release"), so those mappings do reach the compendia — as
`ICD10CM:G20` rather than `ICD10:G20`, which means they will not normalize under the `ICD10:`
spelling until [issue #1033](https://github.com/NCATSTranslator/Babel/issues/1033) unifies it. The
other 11, and the question of whether a label match is evidence of equivalence at all, are
[issue #1038](https://github.com/NCATSTranslator/Babel/issues/1038), with the record in
[`mappings/icd-label-matches.csv`](mappings/icd-label-matches.csv). Samples above are drawn from
opposite ends of the file — the most-cited codes, then 1:1 rows spread across it — rather than its
head, since the point turns on both shapes existing.

**`ICD0` rows survive the filter but never reach a compendium.** `ICD0` is not registered in
`biolink:Disease`'s `id_prefixes`, so `write_compendium()` drops every one of the 462 that the
filter keeps — after they have already done their merging in `glom()`. A build-vs-build count
confirms it: ICD members go from 38 to 6,452 across the two disease compendia — `ICD10` 2,243,
`ICD9` 2,192, `ICD10CM` 2,012 and `icd11` 5 — with `ICD0` contributing nothing on either side.

MONDO's `ICD10CM:` had the same defect and is worked around here, by listing the prefix in
`config.yaml: disease_extra_prefixes_by_biolink_class` so `write_compendium()` keeps it (see "Open
before release" below). `ICD0` is deliberately **not** given the same treatment: an ICD-O code is a
tumour *morphology*, so emitting one asserts a disease equivalence nobody has decided. That question
is [issue #1037](https://github.com/NCATSTranslator/Babel/issues/1037). Neither is a regression this
change introduces.

## Overuse in DOID's other namespaces is still open

DOID is in `OVERUSE_FILTERED_CONCORDS` **scoped to ICD**, so overuse outside ICD is untouched.

- [ ] 537 non-ICD targets (1,258 rows) are still claimed by 2+ DOID terms: MESH 248, SNOMEDCT 130,
  orphanet 59, UMLS 41, NCIT 35, GARD 11, OMIM 11, KEGG.DISEASE 2. Some are wrong and some
  (pancreatitis, papilloma) are right, so this needs per-case review rather than widening the scope
  to `None`, plus the question of whether MONDO/UMLS already supply the correct mappings anyway.
  The record is [`mappings/overused-targets.csv`](mappings/overused-targets.csv), which lists all
  835 overused targets — the 298 ICD ones the filter drops and the 537 it does not — so filter on
  `target_prefix` to see just what still merges.

  Widening is a one-word change (`OVERUSE_FILTERED_CONCORDS["DOID"] = None`), which is the point of
  scoping it this way: the decision is per-namespace and reversible, not baked into how the concord
  is written.

## Open before release

- [x] **Confirmed on a real build with `babel-clique-diff`**, two local `disease` builds of the same
  intermediates — the branch's base commit against its head. Result in
  [`mappings/clique-diff-top-100.csv`](mappings/clique-diff-top-100.csv) — the full diff is 4,302
  rows, so this is the ranked head of it: every dropped member first, then every cross-compendium
  move, then the largest cliques. The per-compendium totals are in
  [`mappings/clique-diff.json`](mappings/clique-diff.json), and "Regenerating" below rebuilds the
  whole CSV.

  | | before | after |
  | --- | --- | --- |
  | identifiers across both compendia | 738,483 | **746,552** |
  | `Disease.txt` cliques | 365,510 | 365,087 |
  | `PhenotypicFeature.txt` cliques | 75,478 | 75,477 |
  | `ICD10` / `ICD9` / `icd11` members | 30 / 8 / 0 | **2,243 / 2,192 / 5** |
  | `ICD10CM` members | 0 | **2,012** |

  4,296 cliques changed, 37 members moved between compendia, and **no member is dropped**.
  Identifiers rise by 8,069 while cliques fall by 424: the renames, the kept ICD rows and the
  newly-emitted `ICD10CM:` CURIEs mostly *add members to existing cliques* rather than merging
  cliques together, which is what mappings that were previously joining nothing should do.

  The `ICD10CM:` members are emitted deliberately ahead of the Biolink Model, which registers no
  such prefix for `biolink:Disease` — see `config.yaml: disease_extra_prefixes_by_biolink_class`.
  They ride along as clique members and can never be the preferred CURIE (`write_compendium()`
  appends `extra_prefixes` after the registered ones), so they will not normalize until
  [issue #1033](https://github.com/NCATSTranslator/Babel/issues/1033) unifies the spelling. Keeping
  MONDO's ~2,030 curated ICD-10 mappings in the output beats discarding them and re-deriving them
  later.

- [x] **No identifier is lost.** An earlier run of this diff lost five — `DOID:0080409` "familial
  adenomatous polyposis 1", `orphanet:733`, `orphanet:321` "Multiple osteochondromas",
  `OMIM:133701` and `OMIM:600209` — because the new mappings followed
  [`DOID:206`](http://purl.obolibrary.org/obo/DOID_206) "hereditary multiple exostoses" and
  [`DOID:0050424`](http://purl.obolibrary.org/obo/DOID_0050424) "familial adenomatous polyposis"
  into `biolink:PhenotypicFeature` cliques, which register none of `DOID`/`OMIM`/`orphanet`, so
  `write_compendium()` dropped them silently. Four bad-xref pairs now cut the disease/phenotype
  boundary — see `input_data/umls_badxrefs.txt` and `input_data/badHPx.txt`. The upstream error is
  that `UMLS:C0015306` "Hereditary Multiple Exostoses" and `NCIT:C3339` "Familial Adenomatous
  Polyposis", both diseases, sit inside HP phenotype cliques; those pairs work around it rather
  than fixing it. This is the check [`docs/sources/CLAUDE.md`](../CLAUDE.md) describes under "Where
  an identifier ends up is a claim about it".

- [ ] **EFO and HP emit ICD xrefs too** (62 and 46 rows), and MONDO emits 2,030 under the other
      spelling, `ICD10CM:`. Tracked in
      [issue #1033](https://github.com/NCATSTranslator/Babel/issues/1033), which also records the 21
      codes that unifying the spellings would newly merge, and why `diseasephenotype.py`'s HP
      `ignore_list=["ICD"]` — a latent no-op — must not be "fixed" before that question is settled.

## Prefixes DOID spells its own way

Beyond ICD, every xref target has to reach the Babel prefix its clique uses, or it is a merge hazard
of exactly the same shape: a CURIE no ids file carries joins nothing, gets dropped by
`write_compendium()` as an unregistered prefix — and still reaches `glom()` first, fusing every DOID
term that cites it. A rename that is missing does not error. It is silent.

The renames each disease source needs now live in one reviewable block,
`config.yaml: disease_xref_prefixes`, applied by `babel_utils.norm()` and validated against
`src/prefixes.py` when loaded. Three were missing for DOID:

- **`SNOMEDCT_US_2025_09_01:` and six other release stamps.** DOID stamps its SNOMED prefix with
  the release it was drawn from, so the map's four pinned dates had gone stale: of the 5,358 SNOMED
  rows in the release measured here, exactly **one** matched a listed date. `norm()` now retries a
  missed prefix with a trailing `_YYYY_MM_DD` stripped, so the map names the stem `SNOMEDCT_US`
  once — the spelling HP's map already used — and a future DOID release needs no edit.
- **`MIM:` — 6,483 rows.** `MIM` is an alternative CURIE prefix for OMIM
  ([issue #321](https://github.com/NCATSTranslator/Babel/issues/321)); both now standardize to
  `OMIM` until the Biolink Model registers `MIM`, at which point that issue flips the direction.
  332 of those rows are phenotypic series (`MIM:PS303350`), which Babel spells
  `OMIM.PS:303350` — the `PS` belongs to the prefix, not to the local id — so this one rename
  depends on the local id and not just the source prefix. `Text.omim_curie()` holds that rule for
  both `norm()` and `Text.opt_to_curie()`.
- **`ORDO:2822` — 2,321 rows.** DOID's spelling of Orphanet, which Babel writes `orphanet:`. MONDO
  and HP already emit `orphanet:`, so until now none of DOID's Orphanet mappings could join theirs.

One is known and deliberately left for its own change: **`ICD10:` vs `ICD10CM:`**, where DOID, EFO
and HP emit one spelling and MONDO the other — two namespaces for one vocabulary that never merge.
This change stops short of renaming them: it emits MONDO's `ICD10CM:` CURIEs rather than discarding
them (see "Open before release"), but unifying the spellings would newly merge 21 codes across
MONDO, EFO and HP, and each of those bridges a disease to a phenotype. That review is
[issue #1033](https://github.com/NCATSTranslator/Babel/issues/1033).

## Regenerating

The audit CSVs come from the built concord, which now keeps its ICD rows — no special pre-filter
build is needed any more. `icd-label-matches.csv` is derived from `icd-targets.csv` by the snippet
in [issue #1038](https://github.com/NCATSTranslator/Babel/issues/1038).

```bash
# every ICD row, kept or dropped: --min-subjects 1 lists them all, and subject_count says which
# side of the filter each falls on
uv run babel-overused-xrefs --concord babel_outputs/intermediate/disease/concords/DOID \
    --min-subjects 1 --target-prefixes ICD10,ICD9,ICD0,ICD11 \
    --out docs/sources/DOID/mappings/icd-targets.csv --mrconso babel_downloads/UMLS/MRCONSO.RRF

# every overused target in the concord, ICD and not
uv run babel-overused-xrefs --concord babel_outputs/intermediate/disease/concords/DOID \
    --out docs/sources/DOID/mappings/overused-targets.csv --mrconso babel_downloads/UMLS/MRCONSO.RRF

# the clique table above, all four scenarios
uv run python docs/sources/DOID/mappings/scripts/measure_icd_xrefs.py
```

The build-vs-build diff needs two `disease` builds of the same intermediates, one at this change's
base commit and one at its head, each keeping
`babel_outputs/compendia/{Disease,PhenotypicFeature}.txt`:

```bash
uv run babel-clique-diff --before <base build> --after <head build> \
    --files Disease.txt PhenotypicFeature.txt \
    --out-csv <full diff>.csv --out-json docs/sources/DOID/mappings/clique-diff.json
```

Delete `intermediate/disease/concords` and the `disease` rule's own outputs between the two passes.
Snakemake's "code has changed" trigger hashes a rule's `run:` block, not the modules it imports, so
a change inside `norm()` or `get_xref_prefix_map()` otherwise leaves the first pass's concords in
place and diffs a build against itself.

The measurement script rebuilds the concord through production
`build_disease_doid_relationships()` and toggles the production constants, and the tool shares
`find_overused_xref_targets()` with it, so neither can drift from what the build does.

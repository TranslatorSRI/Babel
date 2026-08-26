# GARD — NCATS Genetic and Rare Diseases registry

GARD is the NCATS Genetic and Rare Diseases information center's rare-disease registry. It is a
flat list of rare-disease terms -- each a `GARD:` CURIE with a preferred label and pipe-separated
synonyms -- distributed by NCATS as a single CSV. Babel ingests it as a `biolink:Disease` source in
the existing `disease` (`diseasephenotype`) pipeline.

## What GARD contributes

GARD is a **registry, not an ontology**: it carries no cross-references to other disease
vocabularies (no MONDO/DOID/UMLS/Orphanet mappings). It therefore contributes **identifiers and
labels/synonyms only** -- it asserts nothing Babel can build a concord from. Every GARD term is
typed `biolink:Disease`, so each lands in `Disease.txt`.

Babel does build one concord *about* GARD from GARD's own data: `GARD_LABEL` links the ~277 registry
terms that neither MONDO nor DOID maps to an identically labelled identifier already in the
pipeline, so they join the clique they duplicate instead of shipping as single-identifier cliques.
It is the only concord in this pipeline derived from labels rather than an asserted mapping; the
measurement that justifies it, and the three guards that make it safe, are in
[`label-matches.md`](label-matches.md).

Cliques still merge, in the other direction: **MONDO and DOID both cross-reference GARD.** MONDO
maps 15,930 registry terms and DOID a further 2,195, so between them almost the whole registry is
already mapped by an existing Babel source — which is why GARD joins existing cliques rather than
forming its own. MONDO's mappings needed a new concord to reach Babel at all: it asserts them as
`oboInOwl:hasDbXref`, and the `MONDO` concord reads only `skos:exactMatch`. See
[`docs/sources/MONDO/README.md`](../MONDO/README.md) for that exception and its scoping.

Measured on the finished build: the two disease compendia hold 16,508 distinct GARD identifiers
across 16,363 cliques — **16,351 of them shared with another vocabulary, and only 12 GARD-only.**
Ingesting GARD adds 258 net cliques to `Disease.txt` (365,087 → 365,345, +0.07%) before the
`GARD_LABEL` concord, which then merges 265 of them away (365,345 → 365,080).

DOID also asserts 300 GARD ids the current registry no longer publishes (MONDO asserts none). Those
join their DOID clique without a label, exactly like any other xref target Babel does not ingest —
retired ids are worth keeping, since data that still cites them normalizes to the right clique. (294
reach `Disease.txt`; five are cited by two DOID terms each and are dropped by the overuse filter
described below, since nothing says which of the two they meant; `GARD:10191` does not, because its
only subject [`DOID:1824`](http://purl.obolibrary.org/obo/DOID_1824) "neuroretinitis" reaches
neither disease compendium — a pre-existing condition, absent from the `main` build too, unrelated
to GARD, and now tracked as
[#1042](https://github.com/NCATSTranslator/Babel/issues/1042).)

## Local-id form: unpadded

The distribution zero-pads every local id to seven digits (`GARD:0006038` "Chikungunya fever"),
but DOID emits the **unpadded** form for all but 29 of its 2,196 distinct GARD xrefs (`GARD:6038`,
from [`DOID:0050012`](http://purl.obolibrary.org/obo/DOID_0050012) "chikungunya"); 28 carry the
registry's 7-digit padding (`GARD:0018564`, from
[`DOID:0061263`](http://purl.obolibrary.org/obo/DOID_0061263) "lethal congenital contracture
syndrome 7"), so DOID is internally inconsistent too. The 29th is a typo, below.

Babel standardizes on the unpadded form. `normalize_gard_curie()` in `src/datahandlers/gard.py`
strips leading zeros, and is applied in two places:

1. when parsing the registry CSV, so `labels`, `synonyms` and the `ids/GARD` file are all unpadded;
2. to every source's GARD xref targets, through the rename map: `config.yaml:
   disease_xref_prefixes` lists `GARD: GARD` under both `DOID` and `MONDO`, and
   `LOCAL_ID_DEPENDENT_RENAMES` in `src/createcompendia/diseasephenotype.py` resolves that entry to
   `normalize_gard_curie` (the same mechanism as `MIM` → `OMIM`/`OMIM.PS`). A source that starts
   emitting GARD xrefs needs that one config line, and nothing in its handler.

Without this, `GARD:0006038` and `GARD:6038` are two identifiers for one disease: 1,886 rare
diseases would normalize to two conflicting cliques, and none of DOID's GARD xrefs would ever pick
up a registry label.

The registry's own web endpoint trims leading zeros the same way, so
`https://rarediseases.info.nih.gov/?gard_id=6038` and `?gard_id=0006038` both resolve — use that
form when linking a GARD CURIE from documentation (GARD is absent from the Biolink prefix map, so
there is no `get_biolink_prefix_map()` expansion for it).

### The one xref that is not padding

[`DOID:0061030`](http://purl.obolibrary.org/obo/DOID_0061030) "hemophilia" writes its GARD xref as
`GARD:0418` — four digits, so neither the unpadded form nor the registry's 7-digit padding.
Hemophilia is [`GARD:10418`](https://rarediseases.info.nih.gov/?gard_id=10418); `GARD:0418` is a
typo, and it unpads to `GARD:418` "Essential pentosuria", which
[`DOID:0111258`](http://purl.obolibrary.org/obo/DOID_0111258) "pentosuria" already xrefs. The two
cliques do not merge — both hold a MONDO identifier and MONDO is in `DISEASE_UNIQUE_PREFIXES`, so
`glom()` refuses the union — so the contested identifier goes to whichever concord claims it first.

Two things settle it, neither specific to this row. `MONDO_GARD` is glommed before `DOID`
(`disease_concords` order is load-bearing, and a unit test pins it), and MONDO maps
[`MONDO:0009846`](http://purl.obolibrary.org/obo/MONDO_0009846) "pentosuria" to `GARD:418`, so
the id is in the pentosuria clique before DOID's concord is read. And DOID's concord is
overuse-filtered on GARD as well as ICD (`OVERUSE_FILTERED_CONCORDS["DOID"]`): a GARD id claimed
by two DOID terms is dropped, the same family-code logic as ICD, since a registry term names one
rare disease. Twelve of DOID's 2,196 GARD targets are in that position — `GARD:625` would
otherwise fuse [`DOID:0051080`](http://purl.obolibrary.org/obo/DOID_0051080) "Alport syndrome 3B"
with [`DOID:0110033`](http://purl.obolibrary.org/obo/DOID_0110033) "Alport syndrome 2", and
`GARD:7674` would pull [`DOID:0060160`](http://purl.obolibrary.org/obo/DOID_0060160) "childhood
spinal muscular atrophy" out of its MONDO clique — and MONDO's own mapping places every one of the
twelve, so the filter costs nothing.

The typo is reported upstream as
[DiseaseOntology#1620](https://github.com/DiseaseOntology/HumanDiseaseOntology/issues/1620); Babel
reports upstream xrefs, it does not rewrite them, and the correct edge arrives on its own once DOID
fixes it. Restricting `normalize_gard_curie()` to 7-digit local ids would also have unmerged the
two, but by accident — it would leave a dangling `GARD:0418` in the concord and silently swallow the
next mistyped id instead of recording it.

## Biolink registration (the `extra_prefixes` escape hatch)

`GARD` is registered **neither** in the Biolink Model's `disease` `id_prefixes` nor in its prefix
map (both verified against the pinned `biolink_version` in `config.yaml`; the missing prefix-map
entry is why the impact report renders GARD CURIEs without a resolving link).
`write_compendium` keeps only identifiers whose prefix is in the clique type's `id_prefixes` and
silently drops the rest, so without intervention every GARD CURIE would vanish from `Disease.txt`
-- both the ~16k registry terms and the 2,186 that arrive via DOID's concord. The disease
compendium build therefore passes `extra_prefixes=[GARD]` (the
[documented escape hatch](../../AddingNewSources.md)) at the `write_compendium` call site in
`src/createcompendia/diseasephenotype.py`.

Registering GARD with the Biolink team for `biolink:Disease` is the long-term fix; once registered,
GARD can be dropped from `disease_extra_prefixes` in `config.yaml`. This is the same situation GTDB
is in for `biolink:OrganismTaxon` (PR #978 ships GTDB under the same `extra_prefixes` escape hatch).
It is tracked as [#1051](https://github.com/NCATSTranslator/Babel/issues/1051).

Because `extra_prefixes` is a **per-class** allowlist and GARD is on the Disease list only, a GARD
identifier that lands in a phenotype clique is not moved to `PhenotypicFeature.txt` — it is dropped
from the build outright. Five registry terms are in exactly that position, and it is why
`build_gard_label_concord()` refuses to link a GARD term to a clique it would not type
`biolink:Disease`. Registering GARD upstream removes both this section and that guard.

## Download

The GARD term list is the "GARD Rare Disease List \<Mon\>\<Year\>.csv" link on
<https://rarediseases.info.nih.gov/about>, under "Which rare diseases are included in GARD?" — NCATS
publishes no stable or documented data URL. The link is a Salesforce file-distribution URL
(`https://ncats.file.force.com/sfc/dist/version/download/?...&ids=068...`) whose `ids` value is a
ContentVersion id, which names **one uploaded version** of the file: the link pinned in
`config.yaml` as `gard_download_url` is "GARD Rare Disease List Jun2026.csv". When NCATS uploads a
new list the About page gets a new link, and the old one may stop resolving, so **re-check the About
page before each release** and repoint `gard_download_url`. `uv run pytest
tests/datahandlers/test_gard.py::test_gard_download_url_is_current --network` does the check: it
fails if the configured link has gone from the page, and warns if the page carries another
distribution link (most likely a newer upload), naming it. `gard_download_url` therefore sits with
the other per-release versions at the top of `config.yaml`. The URL is passed to the `get_gard`
rule (in `src/snakefiles/datacollect.snakefile`) as a `params` value, so repointing it retriggers
the download. It is a query-string URL with no stable filename on the server, so the rule calls
`src.datahandlers.gard.pull_gard()` directly rather than the shared `pull_via_urllib` helper.

Three guards keep a broken distribution from producing a green build with rare diseases missing
from it: the download rejects an HTML response (an expired ContentVersion link serves an HTML error
page with HTTP 200, which `urllib` does not raise on), and the parser raises if the
`ID`/`DisplayName` headers are missing, if a GARD row has an empty `DisplayName`, or if no term
parses at all.

The CSV is UTF-8 with a BOM and CRLF line endings, with columns `ID,DisplayName,Synonyms,URL`. The
`URL` column (the rarediseases.info.nih.gov page) is not ingested: Babel handlers emit only
labels, synonyms, taxa and descriptions, and there is no per-identifier URL attribute file for it
to go in.

## Wiring

| Concern | Location |
| --- | --- |
| Prefix constant | `src/prefixes.py` (`GARD = "GARD"`) |
| Data handler | `src/datahandlers/gard.py` |
| Local-id normalization | `normalize_gard_curie()` in `src/datahandlers/gard.py`; for xref targets, the `GARD: GARD` entries of `disease_xref_prefixes` in `config.yaml`, resolved by `LOCAL_ID_DEPENDENT_RENAMES` in `src/createcompendia/diseasephenotype.py` |
| Download rule | `get_gard` in `src/snakefiles/datacollect.snakefile` |
| Labels/synonyms rule | `get_gard_labels_and_synonyms` in `src/snakefiles/datacollect.snakefile` |
| ids rule | `disease_gard_ids` in `src/snakefiles/diseasephenotype.snakefile` |
| `extra_prefixes=[GARD]` | `disease_extra_prefixes` in `config.yaml`, read by `build_compendium` in `src/createcompendia/diseasephenotype.py` |
| MONDO's GARD xrefs | `MONDO_GARD` concord, written by `build_disease_obo_relationships()`; see [`docs/sources/MONDO/README.md`](../MONDO/README.md) |
| Doubly-claimed DOID xrefs | `OVERUSE_FILTERED_CONCORDS["DOID"]` (ICD + GARD) and `["MONDO_GARD"]` in `src/createcompendia/diseasephenotype.py` |
| Label-match concord | `build_gard_label_concord()` in `src/createcompendia/diseasephenotype.py`, rule `disease_gard_label_concord`; evidence in [`label-matches.md`](label-matches.md) |
| Label-match pool | `disease_gard_label_match_prefixes` in `config.yaml` (priority order; HP and MP deliberately excluded) |
| Config lists | `disease_ids`, `disease_labelsandsynonyms`, `disease_concords` (`MONDO_GARD`, `GARD_LABEL`), `disease_extra_prefixes`, `disease_gard_label_match_prefixes`, `gard_download_url` in `config.yaml` |

The `disease_gard_ids` rule is a simple `awk` transform of the labels file (every GARD term is a
Disease), mirroring the DOID/Orphanet ids rules.

## Source-impact report

Generated (synthetic mode) and committed at [`impact-report.md`](impact-report.md), with the two
committed reductions in [`impact-report/`](impact-report/). It was run against a complete local
`disease` intermediate set (all 11 `disease_ids` files and all 9 `disease_concords`), from the same
finished build the clique diffs below compare.

Summary:

- **16,214 identifiers** added (all `GARD:`, all `biolink:Disease`).
- **12 new cliques** -- one single-identifier clique per registry term that neither MONDO, DOID nor
  a label match places (a 0.00% increase over the 440,647 pre-existing disease cliques). Before the
  `GARD_LABEL` concord this figure was 277.
- **15,872 existing cliques contain GARD identifiers.** The report excludes `MONDO_GARD` and
  `GARD_LABEL` along with GARD's ids file (`is_excluded()` splits a compound concord name on `_`, so
  both are recognized as GARD data; a "before" state that kept either would already hold GARD
  CURIEs), which is why it sees 14,304 cliques gaining a structurally new GARD identifier and 22
  merges.
- **Section 3 reports 0 cross-reference rows, and that is a reporting artifact, not the truth.**
  `discover_source()` looks for a concord file named exactly `GARD`, so it does not find
  `GARD_LABEL`, and the join-pathway table below it labels `GARD_LABEL`'s 265 rows
  `from_other_source`. The inbound pathways it lists are right -- `MONDO_GARD` (15,936 rows) and
  `DOID` (1,902) -- and the GARD_LABEL rows are the 210 NCIT, 42 MONDO, 7 orphanet and 6 MESH
  entries in the same table. Tracked as
  [#1059](https://github.com/NCATSTranslator/Babel/issues/1059).
- **Section 4 is a worst-case (upper-bound) view:** it is computed before the Biolink per-class
  prefix filter runs, so the sample cliques are flagged "NOT emitted -- prefix not registered in
  Biolink Model for `biolink:Disease`". That flag is *exactly* why the build passes
  `extra_prefixes=[GARD]` (see above); with it, the identifiers are kept at `write_compendium`
  time. Registering GARD upstream removes both the flag and the need for `extra_prefixes`.
- Section 2's "Final compendium-assigned" line confirms all 16,214 GARD identifiers reach
  `Disease.txt` in the finished build, which is the check `extra_prefixes` exists to pass.

Regenerate after a typing or extraction change:

```bash
uv run source-impact-report --source GARD
```

## Build-vs-build clique diff

The impact report only walks after-cliques containing a GARD CURIE, so it cannot show a
before-clique that splits, shrinks, or loses its leader — and it cannot show *which* of two
competing cliques a new GARD identifier joined. [`clique-diff.md`](clique-diff.md) records two
`babel-clique-diff` runs that close both gaps: `main` vs this branch
([`on-addition/`](on-addition/), which confirms the addition is purely additive — 0 regrouped, 0
moved, 0 dropped, 0 leader changes) and a second, uncommitted diff isolating the
hemophilia/pentosuria fix, tabulated in full on that page.

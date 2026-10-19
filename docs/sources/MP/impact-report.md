# Source impact report: MP

- Generated: 2026-07-10 00:19:14 UTC
- Babel commit: f29db754111750b0539397e65f94744087380e58
- Source pipelines: disease
- Source prefixes: MP
- Comparison mode: synthetic

## 1. Identifiers added

Totals: 14,750 identifiers across 1 prefix(es) in 1 pipeline(s).

### By prefix

- MP: 14,750

### By pipeline

- disease: 14,750

## 2. Biolink types

### Overall declared type breakdown

- biolink:PhenotypicFeature: 14,750

### Source-declared (from each ids file)

- disease / MP
  - biolink:PhenotypicFeature: 14,750

### Final compendium-assigned (after glom)

- disease / PhenotypicFeature.txt: 14,750 MP identifiers

## 3. Cross-references added

Totals: 78 cross-reference rows across 1 concord file(s).

### By pipeline

- disease / MP: 78

### Partner prefix breakdown (per pipeline)

- disease
  - MGI: 70
  - MPATH: 4
  - HP: 2
  - UMLS: 2

### Join pathways (every asserted cross-reference, not only this source's own)

`status` is `added` when MP's own concord file asserts the pathway and
`from_other_source` when another source's does — the latter may predate this addition. The
prefix pair is sorted, so `asserted_by` is what tells you which side declared it.

| pipeline | predicate | prefix pair | asserted by | status | xrefs |
|---|---|---|---|---|---|
| disease | `xref` | MGI ↔ MP | `MP` | added | 70 |
| disease | `xref` | HP ↔ MP | `HP` | from_other_source | 9 |
| disease | `xref` | MP ↔ MPATH | `MP` | added | 4 |
| disease | `xref` | HP ↔ MP | `MP` | added | 2 |
| disease | `xref` | MP ↔ UMLS | `MP` | added | 2 |

## 4. Clique impact

**Worst-case view.** This report is computed from the intermediate identifier and concord files and
cannot see downstream filtering that happens later in the build — most notably the Biolink Model's
per-class prefix restrictions, which drop identifiers whose prefix is not permitted for a clique's
biolink type. The counts and detail files below are therefore an *upper bound*: they show every
change the source could introduce before that filtering is applied.

### disease

- 14,750 new cliques composed only of MP identifiers (a 3.46% increase over the 426,264 pre-existing
  cliques)
- 0 existing cliques contain MP identifiers in the after state (0.00% of the 426,264 pre-existing
  cliques). Of these, 0 cliques gain at least one structurally new identifier from MP, and 0 already
  contained the MP CURIE via an xref from another source — MP's ids file now also lists those
  existing CURIEs as first-class typed identifiers.
- 0 existing cliques will be merged because of new MP cross-references
- 0 structurally-new MP identifiers are added to existing cliques (0 via expansion, 0 via merges).
  This is distinct from the 0 existing cliques that change, since one clique can gain several
  identifiers.
- Total cliques in this pipeline go from 426,264 to 441,005
- Sample of new cliques (top 100, unsurvivable and largest first):
  [`impact-report/new-cliques-top-100.csv`](impact-report/new-cliques-top-100.csv)
- Full list of modified cliques (one row per added/preexisting MP identifier):
  `impact-report/modified-cliques.csv` -- not committed (see `.gitignore`); regenerate it with
  `uv run source-impact-report`
- Cross-reference summary (join pathways with counts and example rows):
  [`impact-report/new-xrefs-summary.csv`](impact-report/new-xrefs-summary.csv)

#### Sample pure-new cliques (up to 3)

- [`MP:0003632`](http://purl.obolibrary.org/obo/MP_0003632) "abnormal nervous system morphology"
  **(preferred)**
  - [`MGI:2173613`](http://identifiers.org/mgi/2173613)
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:PhenotypicFeature`)**
  - [`MGI:2173617`](http://identifiers.org/mgi/2173617)
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:PhenotypicFeature`)**
- [`MP:0003633`](http://purl.obolibrary.org/obo/MP_0003633) "abnormal nervous system physiology"
  **(preferred)**
  - [`MGI:2173615`](http://identifiers.org/mgi/2173615)
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:PhenotypicFeature`)**
  - [`MGI:2173618`](http://identifiers.org/mgi/2173618)
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:PhenotypicFeature`)**
- [`MP:0002080`](http://purl.obolibrary.org/obo/MP_0002080) "prenatal lethality" **(preferred)**
  - [`MGI:2173525`](http://identifiers.org/mgi/2173525)
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:PhenotypicFeature`)**

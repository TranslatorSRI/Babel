# Leftover UMLS

The "leftover UMLS" step is the last thing the pipeline does. After every other compendium has been
built, `src/createcompendia/leftover_umls.py` (Snakemake rule `leftover_umls`) goes through
`MRCONSO.RRF` and collects every valid UMLS concept that **no other compendium already claimed**,
writing each one as a single-identifier clique into `babel_outputs/compendia/umls.txt`. The point is
coverage: even when Babel can't merge a UMLS concept into a richer clique, downstream services (Node
Normalization, Name Resolver) can still return its label and a Biolink type. This addresses
[#579], and the typing corrections below address [#569] and the umbrella [#410].

## How a leftover concept gets its Biolink type

Each UMLS concept carries one or more **semantic types**. UMLS identifies a semantic type by a code
such as `T033`; that code is technically the "TUI", but the namespace UMLS and the Biolink Model
Toolkit (bmt) actually use is `STY` (the mapping key is `STY:T033`), so this code and the override
table use **STY** throughout.

`tui_to_biolink_type()` is a thin wrapper over bmt's `STY:<code>` mapping and returns exactly what
Biolink says (or `None` if Biolink has no mapping). For each concept:

1. Every semantic type is resolved to one of three outcomes — a Biolink type, a **rejection**
   (override maps it to `None`), or **unmapped** (Biolink has no mapping and there is no override).
2. If any semantic type is unmapped, the whole concept is skipped (we don't emit a concept we can't
   fully type) and reported as `NO_UMLS_TYPE`.
3. Rejected semantic types simply drop out. If nothing is left, the concept is skipped and reported
   as `REJECTED`.
4. If the surviving Biolink types disagree, generic types in `GENERIC_TYPES` are dropped first (so a
   specific co-type wins), then `TYPE_COMBO_OVERRIDES` is consulted to pick one; if more than one
   type still remains the concept is skipped and reported as `MULTIPLE_UMLS_TYPES`.

## The override tables

The right place to fix a wrong UMLS-to-Biolink mapping is the Biolink Model itself, but it is hard
to anticipate how a Biolink change will land in real Babel data. The two tables at the top of
`leftover_umls.py` let us correct mappings locally and reversibly:

- **`STY_OVERRIDES: dict[str, str | None]`** — override the Biolink type bmt assigns to a single
  semantic type. `None` means "deliberately reject". Current entries:
  - `T033` "Finding" → `biolink:Phenomenon` ([#569]; Biolink has no STY mapping for it, so without
    this the concepts would be dropped as unmapped).
  - `T034` "Laboratory or Test Result" → `biolink:ClinicalFinding` ([#569]; Biolink maps it to the
    broader `biolink:Phenomenon`).
  - `T058` "Health Care Activity" → `biolink:ClinicalIntervention` ([#90]; bmt assigns the generic
    `biolink:Activity`).
  - `T045` "Genetic Function" → `biolink:BiologicalProcess` ([#421]; no STY mapping in Biolink).
  - `T021` "Fully Formed Anatomical Structure" → `biolink:GrossAnatomicalStructure` ([#421]; no STY
    mapping in Biolink).
  - `T120` "Chemical Viewed Functionally" → `biolink:ChemicalEntity` ([#421]; no STY mapping in
    Biolink).
  - `T122` "Biomedical or Dental Material" → `biolink:ChemicalEntity` ([#421]; no STY mapping in
    Biolink).
  - `T168` "Food" → `biolink:Food` ([#421]; no STY mapping in Biolink).
  - `T072` "Physical Object" and `T073` "Manufactured Object" → `biolink:PhysicalEntity` ([#840]).
    bmt already maps these to `biolink:PhysicalEntity`, so the override is an intentional pin (see
    `GENERIC_TYPES` below) rather than a correction.
- **`TYPE_COMBO_OVERRIDES: dict[frozenset[str], str]`** — when a concept resolves to more than one
  Biolink type, pick a single one (e.g. `{Device, Drug} → Drug`).
- **`GENERIC_TYPES: frozenset[str]`** — very high-level types (currently `biolink:PhysicalEntity`)
  that must never shadow a more specific co-type. When a concept resolves to more than one Biolink
  type and one is generic, the generic type is dropped so the specific one wins. A concept typed
  *only* as a generic type still keeps it. This is the successor to rejecting `T072`/`T073` with
  `None`: rejection kept the specific type only by contributing no type at all, which also dropped
  concepts whose *only* type was `T072`/`T073`.

### Prefix-less Biolink types are still writable here

Some of these target types (`biolink:Phenomenon`, `biolink:ClinicalFinding`,
`biolink:PhysicalEntity`) carry no `id_prefixes` in the Biolink Model. They are nonetheless writable
in *this* compendium because every leftover clique is a single `UMLS:` identifier and the rule
passes `extra_prefixes=[UMLS]` to `write_compendium()`. `NodeFactory.create_node()` raises "No
Biolink prefixes for ..." only when a type has no `id_prefixes` **and** no `extra_prefixes` are
supplied. (Previously `create_node()` raised on the bare `get_prefixes()` call before it considered
`extra_prefixes`, which crashed the rule after ~5h on the HPC.)

### Preflight: failing fast

`write_leftover_umls()` runs a preflight before loading any compendia, MRSTY or MRCONSO: it calls
`create_node(input_identifiers=[], node_type=T, extra_prefixes=[UMLS])` for every type in
`writable_output_types()` (all non-`None` `STY_OVERRIDES` values, all `TYPE_COMBO_OVERRIDES` values,
and `biolink:NamedThing`). If any type is unwritable the rule aborts in seconds with a clear message
instead of after the multi-hour MRCONSO scan. The companion test
`test_all_override_target_types_are_writable` guards the same set in CI.

Every `STY_OVERRIDES` entry must cite the GitHub issue that motivates it.

## Keeping overrides honest: the drift test

`tests/createcompendia/test_leftover_umls.py` (marked `network`, because building a bmt toolkit
fetches the Biolink model) records, in `RECORDED_STY_BASELINE`, the Biolink `STY:<code>` mapping
that was current when each override was added, for the `biolink_version` pinned in `config.yaml`. On
each run it compares the live mapping against the recorded baseline:

- **Hard fail** when the live mapping diverges from the recorded baseline — Biolink changed
  underneath us, so the override (and the baseline) must be re-reviewed.
- **Warning only** when the live mapping has come to equal the override — Biolink now agrees, so the
  override is redundant and can be removed.

Because the test is `network`-marked it runs in the nightly/weekly cadences and on demand
(`uv run pytest tests/createcompendia/test_leftover_umls.py --network`), not on every PR. The most
important time to run it is when bumping `biolink_version`.

## Coverage report

The rule writes all UMLS reports to `babel_outputs/reports/umls/`. The human-readable log is
`log.txt`; the machine-readable CSVs are:

- `compendium-coverage.csv` — where UMLS lands inside Babel, broken down by Biolink type and
  semantic type. One row per (compendium, Biolink type, most-specific UMLS semantic-type set):
  `compendium`, `biolink_type`, `tui_set`, `tui_set_labels`, `tree_set`, `curie_count`,
  `single_umls_clique_count` (cliques whose only identifier is a single UMLS CURIE), and sample
  `CURIE=label`s. The `biolink_type` is read directly from each clique's `type` field as the file is
  parsed (falling back to the compendium filename, e.g. `MolecularMixture.txt` →
  `biolink:MolecularMixture`, for any clique lacking one), so when one semantic type is split across
  two Biolink types it gets one row per type. The semantic types are emitted as **codes** in
  `tui_set`/`tree_set` (pipe-joined TUIs and matching `MRSTY` tree numbers) with their names in
  `tui_set_labels`, so the type grouping is scannable at a glance (use `tui-sty.tsv` to decode a
  code). Each concept's TUIs are reduced to the *most specific* ones first (a TUI that is a
  tree-number ancestor of another TUI on the same concept is dropped), via
  `babel_utils.reduce_to_most_specific_tree_codes()`. Summing `curie_count` over a compendium
  reproduces its total unique UMLS count. The file spans every compendium that consumes UMLS
  **plus** the leftover `umls.txt` compendium itself (whose rows tend to be the most type-diverse);
  filter on the `compendium` column to focus on one. An empty semantic-type set (a UMLS CURIE absent
  from `MRSTY`) is written as `(none)`.
- `tui-coverage.csv` — the **same rows** as `compendium-coverage.csv` (same columns, reordered to
  `tui_set`, `tui_set_labels`, `tree_set`, `compendium`, `biolink_type`, `curie_count`,
  `single_umls_clique_count`, `sample_curies`) but sorted **TUI-set first**, so the file answers
  "where does semantic type `T047` go across all of Babel, and which Biolink types does it get?"
  rather than "what's in compendium X". It is the transpose of `compendium-coverage.csv`, not new
  data; keep whichever ordering suits the question at hand.
- `duplicate-curies.csv` — UMLS CURIEs that landed in **more than one compendium clique**: either
  across two different compendium files (**cross-file**) or in two distinct cliques of one file with
  different clique leaders (**within-file**). Both are clique-merge bugs ([#276]). One row per
  duplicated CURIE: `umls_curie`, `umls_label`, `tui_set`, `tui_set_labels`, `num_compendia`,
  `num_distinct_cliques`, `duplicate_scope` (`cross-file`/`within-file`/`both`), and an
  `occurrences` column rendering each landing as `compendium[biolink_type, leader=CURIE, name=...]`.
  The clique leaders are the key to tracing the cause: they show which two cliques the CURIE was
  glommed into, so you can work back to the upstream concords that pulled it both ways. The leftover
  `umls.txt` compendium never appears here because it only claims CURIEs that no other compendium
  took. The mechanism behind most rows, and the rule that removes them, is
  [`CuiFollowsMesh.md`](CuiFollowsMesh.md).
- `unmapped-types.csv` — per semantic type that was unmapped or rejected: status, exact affected
  CUI count, and sample CURIEs.
- `multi-type-curies.csv` — CURIEs that resolved to multiple Biolink types even after
  `TYPE_COMBO_OVERRIDES`: the type combo, exact count, and sample CURIEs.
- `no-mrsty-curies.csv` — CURIEs that had **no entry in `MRSTY.RRF`** and were therefore typed as
  `biolink:NamedThing` as a fallback. Columns: `umls_curie`, `label`, `biolink_type` (always
  `biolink:NamedThing`), `source_count`, and `sources` (pipe-joined MRCONSO SAB abbreviations for
  that CUI). The SAB list shows which vocabularies reference the CURIE even though UMLS carries no
  semantic-type annotation for it — useful for tracing CUIs from older or superseded releases.
- `tui-sty.tsv` — the raw STY-code → semantic-type-name dump from `MRSTY.RRF`.

The old `types-coverage.csv` (per Biolink type of the leftover cliques only) has been removed: it is
derivable from `compendium-coverage.csv` by filtering to `compendium == umls.txt` and summing
`curie_count` per `biolink_type`.

### Counts vs. samples: why the CSVs carry both

The CSVs report an **exact count** per bucket *and* up to `_SAMPLE_LIMIT` (currently 5)
`CURIE=label` examples. These answer different questions and are not redundant: the count is
*quantitative* ("how many CUIs landed as `biolink:Phenomenon`?"), while the samples are
*qualitative* ("what does one of those concepts actually look like?"). The samples let a reviewer
sanity-check an override or a skip reason straight from the CSV without cross-referencing another
file.

The sample cap is **not** an approximation of the counts — the counts are always exact. It only
bounds memory: we keep at most 5 examples per bucket instead of accumulating every CURIE across the
millions of `MRCONSO` lines. The exhaustive per-CURIE record is not lost, because it already lives
elsewhere:

- every mapped (kept) concept is written to `babel_outputs/compendia/umls.txt`; and
- every skipped concept is streamed to `log.txt` as it is encountered, tagged `NO_UMLS_TYPE`,
  `REJECTED`, or `MULTIPLE_UMLS_TYPES`.

So the split is deliberate: **`log.txt` is the complete per-CURIE record, the CSVs are aggregate
counts plus a handful of illustrative examples.** If the samples ever feel like noise, the right
change is to drop the `sample_curies` columns entirely (the log already has every CURIE), not to
expand them into full per-bucket CURIE lists held in memory.

[#90]: https://github.com/NCATSTranslator/Babel/issues/90
[#276]: https://github.com/NCATSTranslator/Babel/issues/276
[#410]: https://github.com/NCATSTranslator/Babel/issues/410
[#421]: https://github.com/NCATSTranslator/Babel/issues/421
[#569]: https://github.com/NCATSTranslator/Babel/issues/569
[#579]: https://github.com/NCATSTranslator/Babel/issues/579
[#840]: https://github.com/NCATSTranslator/Babel/issues/840

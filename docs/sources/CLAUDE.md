# CLAUDE.md — docs/sources/

Cross-cutting conventions for working with source/cross-reference data, for Claude Code. Per-source
narrative docs (what a source is, how it's ingested) live in `docs/sources/<PREFIX>/` — see
[`README.md`](README.md) for the index; add there, not here, when you learn something non-obvious
about one specific source.

## Investigating a source: commit the check, not the conclusion

When you characterize a source's data to justify a parsing decision — how a field is quoted, whether
a delimiter is safe to split on, whether a pattern is an artifact or real — the check itself belongs
in the repo, next to the per-source doc, as a script that regenerates its own output. A conclusion
recorded only in prose (a PR description, a paragraph here) cannot be re-run against next month's
download, and cannot be checked by a reviewer who doubts it.

This is not pedantry. In the #744 NCBIGene work, a count computed in a throwaway shell one-liner had
the wrong denominator, and nobody could see it until the same check was written into a committed
script — where it immediately disagreed with the prose. Two claims, both stated confidently, both
wrong, both caught only by making them reproducible.

So: keep the analysis script and its generated output under `docs/sources/<PREFIX>/`, have the
script import the production predicate it is reasoning about (rather than re-implementing it, which
lets the two drift), and where the finding constrains real output, add a `pipeline`-marked test that
asserts it over the full downloaded file. `docs/sources/NCBIGene/quoting/` is the worked example:
`analyze_quoting.py` and `double_prime_report.py` both import from `src/datahandlers/ncbigene.py`,
and `tests/pipeline/test_ncbigene.py` enforces the conclusion against every row.

### Markdown gets a sample; the CSV gets the rest

The generated Markdown is an *argument*, read top to bottom by a person: **5–10 examples is plenty**
to show a shape. Anything longer is a data dump that buries the finding — and if it is ranked purely
by frequency, its head is usually one shape repeated (NCBIGene's `''` tokens are all protein-subunit
names for the first dozen rows, hiding the chemical locants entirely). Sample deliberately: spread
across the list, or take the most common of each shape, and say in the report which you did.

Emit the exhaustive per-row record as a **CSV** beside the Markdown and commit it — that is what a
reviewer greps and what a future run diffs against (`shredded_pieces.csv`, one row per dropped
synonym). If the full record is big enough to be unwieldy in Git, leave it out and let the script
regenerate it locally; say so in the report, and make sure the script's default output path is the
one the report names.

## An OBO `hasDbXref` is not an equivalence

Concords are fed to `glom()` as equivalence assertions, but many ontologies use
`oboInOwl:hasDbXref` to mean "this term is *about* that thing". MP is the worst offender: it xrefs
the anatomy an abnormality occurs in (`MP:0009873` "abnormal aorta tunica media morphology" →
`MA:0002903`), the process a phenotype perturbs (`MP:0002998` "abnormal bone remodeling" →
`GO:0046849` "bone remodeling"), plus citations and bare Wikipedia URLs. **Always audit the
target-prefix breakdown of a new source's xrefs before trusting them** (section 3 of the
source-impact report, or `cut -f3 <concord> | sed 's/:.*//' | sort | uniq -c`).

Two filters exist on `ubergraph.build_sets()`:

- `ignore_list=[...]` blocks named target prefixes and **fails open** — a namespace the source
  newly starts emitting is silently trusted (`anatomy.build_anatomy_obo_relationships`).
- `allowed_prefixes=[...]` is the complement and **fails closed** — only listed prefixes are
  written, so a new namespace is dropped until someone reviews it. Prefer this for a source whose
  xrefs are mostly junk (`diseasephenotype.MP_XREF_ALLOWED_PREFIXES`).

Both are matched against `Text.get_prefix_or_none()`, which **upper-cases**, so entries must be
upper-case: `"MPATH"`, `"HTTP"` — a lower-case entry silently never matches. Worked example:
`docs/sources/MP/mappings.md`. Note the matching is exact *equality*, not `startswith`, so
`ignore_list=["ICD"]` blocks nothing when the concord holds `ICD10:`/`ICD9:` — a no-op that reads
like a working filter (HP's build has exactly this).

A source not built through `build_sets()` needs the filter threaded into its own handler; follow
`excluded_target_prefixes` on `efo.make_concords()` / `doid.build_xrefs()`. Apply it **after**
`norm()` so it names post-rename prefixes (`ICD10`, not `ICD10CM`).

### A missing prefix rename is the same bug, spelled differently

`norm()` renames a source's spelling of another vocabulary (`MSH`, `UMLS_CUI`, `MIM`,
`SNOMEDCT_US_2025_09_01`) to Babel's. A prefix it does **not** rename produces no error and no log
line: the CURIE joins nothing (no ids file carries that spelling, and `write_compendium()` drops the
prefix as unregistered) yet still reaches `glom()`, where it fuses every subject that cites it into
one clique. That is the overused-xref failure with a different cause, and it is invisible in the
per-compendium metadata YAMLs — a prefix pair that never joins simply has no `prefix_counts` row.

So **tabulate a source's xref target prefixes against its rename map** before trusting a concord,
the same way you tabulate them for junk targets.
`cut -f3 <concord> | sed 's/:.*//' | sort | uniq -c` against
`config.yaml: disease_xref_prefixes[<SOURCE>]` is the whole check; DOID had 5,358 SNOMED and 6,483
OMIM rows falling through it (see [`DOID/mappings.md`](DOID/mappings.md)).

The maps live in `config.yaml`, not beside the code, precisely because the review question is "what
is missing" — which you cannot ask of a dict you have to go find. This is the deliberate exception
to AGENTS.md's "source-content cleaning stays a Python constant": an ignore-list says *this source's
targets are junk*, while a rename says *these two spellings are the same vocabulary*, which is a
fact about the vocabularies and belongs where all of them can be seen at once. A rename whose target
depends on the local id (OMIM's `PS` phenotypic series) cannot be expressed in YAML; those are
`LOCAL_ID_DEPENDENT_RENAMES` in `diseasephenotype.py`, and `norm()` takes the callable.

### Overuse filtering or a prefix exclusion?

`remove_overused_xrefs` drops any target claimed by 2+ subjects. A prefix exclusion drops a
namespace outright. They are not interchangeable, and picking by "which shrinks the cliques more"
gets it wrong:

(It counts **rows**, not distinct subjects. A concord written by walking two roots -- both MONDO
concords -- repeats every pair under both, and an overuse filter would read the repeat as a second
claimant. Deduplicate on write, as `MONDO_GARD` does, before putting such a file in
`OVERUSE_FILTERED_CONCORDS`.)

- Overuse is a **statistical proxy** — right when a namespace is usually fine but occasionally
  promiscuous. Its blast radius shifts with every upstream release, and it discards a correct
  mapping whenever a too-narrow sibling shares the target.
- A prefix rule states what the namespace **means**, so it stays true. Use it when a whole
  vocabulary is the wrong kind of thing: EFO→MP xrefs cross the phenotype/disease line however few
  subjects cite them, so `EFO_EXCLUDED_XREF_PREFIXES` drops them all.

**Most often you want both at once**, which is what `OVERUSE_FILTERED_CONCORDS` encodes: it maps a
concord basename to the target prefixes the filter may act on (`None` = all of them). Scoping says
*which namespace is suspect* while the count says *which rows in it actually misbehave*, so neither
instrument has to answer a question it is bad at. DOID's ICD codes are the worked case — the
namespace is suspect (an ICD code often names a disease family) but 4,841 of its 6,425 rows are 1:1
and many are correct, so a categorical drop destroys them and an unscoped filter instead savages
DOID's MeSH and SNOMED rows. Scoped, it drops the 1,584 rows on codes claimed twice and nothing
else. Reach for a categorical exclusion only when *no* row of the namespace could be an equivalence.

A scoped filter fails **open**: a namespace missing from the list is not policed at all. Pair it
with a `pipeline`-marked check over the real concord asserting every spelling present is in scope —
`tests/pipeline/test_doid.py` is the example.

`uv run babel-overused-xrefs` (docs/tools/OverusedXrefs.md) is how you tell them apart, and
`--min-subjects 1 --target-prefixes …` enumerates what an exclusion would drop. **Generate that
record before the change lands** — afterwards the rows are gone and cannot be listed. Worked
example, including the ~4,800 1:1 rows an overuse filter would have kept:
`docs/sources/DOID/mappings.md`.

## Bad-xref files

`input_data/*_badxrefs.txt` drop individual `subject object` pairs (**space** separated, `#`
comments, blank lines allowed) from a concord before glom — for individually wrong pairs that
survive prefix filtering. `read_badxrefs()` **raises** on a line that is not exactly two
space-separated tokens rather than skipping it: a suppression that silently does nothing lets the
bad xref back into the compendia with nothing anywhere saying why, and a tab instead of a space is
the easy way to write one.
See the code comments at `diseasephenotype.DEFAULT_BAD_XREFS` and the `disease_compendia`
Snakemake rule (`src/snakefiles/diseasephenotype.snakefile`) for the registration gotcha: a key
must be added in **both** places. `compute_cliques_for_impact_report()` now raises if a key names
no concord basename (a typo), but a key added to one dict and not the other — or forgotten
entirely — still fails open, so a unit test asserting a new key exists and its pairs parse remains
the cheap guard for that.

Anatomy uses a simpler shape worth copying for a new pipeline: one file
(`input_data/anatomy_badxrefs.txt`, `anatomy.ANATOMY_BAD_XREFS`) applied to **every** concord, with
pairs matched in either direction. A pair names both of its CURIEs, so there is nothing for a
per-concord key to disambiguate — and with no key there is no two-place registration to get wrong.
It is wired through the `concord_pair_filter` hook that `glom_from_files()` already exposes, so it
needed no new plumbing.

## Where an identifier ends up is a claim about it — check that claim

Every compendium file is an assertion about the *kind* of thing each clique is. So the file an
identifier lands in, or fails to land in, is free evidence about whether the clique that produced it
is sound. Two questions are worth asking after any build, in any pipeline:

**Is anything in a compendium it has no business being in?** A clique whose members disagree about
what kind of thing they are has usually been merged across a category boundary, and the compendium
it was typed into makes that disagreement visible from the outside. Look for members whose prefix or
label belongs to a different kind than the file they are in: a structure among cells, a process
among substances, a gene among proteins, a taxon anywhere. Chase whatever the source of that
clique's type-vote was, and expect the culprit to be a single bad pair rather than a broken rule.

**Did anything in an `ids` file reach no compendium at all?** `write_compendium()` drops identifiers
whose prefix is not registered for the clique's Biolink type, silently. That is invisible to
`babel-clique-diff` — a CURIE that appears on neither side is not a difference — and invisible to
the source-impact report's clique counts, so the only way to see it is to ask directly. This check
is worth running for any new source, whatever the pipeline:

```bash
uv run python -c "
import re, glob
PIPELINE, PREFIX = 'anatomy', 'EMAPA'   # <- the pipeline and source you are checking
ids = {l.split(chr(9))[0] for l in open(f'babel_outputs/intermediate/{PIPELINE}/ids/{PREFIX}')}
seen = set()
for f in glob.glob('babel_outputs/compendia/*.txt'):
    for line in open(f): seen.update(re.findall(rf'\"({PREFIX}:[^\"]+)\"', line))
print(f'{len(ids - seen)} of {len(ids)} not in any compendium:', sorted(ids - seen)[:20])"
```

The two questions are related: an identifier dropped by the prefix check often got there because a
bad merge put it in a clique of the wrong type, so the loss and the mistyped clique are the same
bug seen from two sides. Worked example: `CL:0000166` "chromaffin cell" had been shipping in a
`biolink:Cell` clique with `UBERON:0001236` "adrenal medulla" — the cell type merged with the
structure it sits in — and that clique was also swallowing two EMAPA terms and dropping them. One
bad xref (`input_data/anatomy_badxrefs.txt`) caused both.

## Keeping two prefixes disjoint

`glom()`'s `unique_prefixes` only forbids *duplicate same-prefix* identifiers in a clique; it does
**not** stop two *different* prefixes from co-occurring, and dropping one source's concord is also
insufficient (other sources' concords bridge them directly or transitively). To guarantee two
prefixes never share a clique, run a **post-glom split** as the last step of the shared
clique-builder so the build and the source-impact report agree — see
`diseasephenotype.split_mutually_exclusive_cliques` /
`MUTUALLY_EXCLUSIVE_PREFIX_GROUPS = [[HP, MP]]` (mirrors the type-driven split in `chemicals.py`)
and `docs/sources/MP/disjointness.md`. A split can strand an identifier that is in a concord but no
ids file (an out-of-date mapping); `create_typed_sets` drops such an untypeable clique with a
warning rather than aborting the build.

## Leftover UMLS

`src/createcompendia/leftover_umls.py` (rule `leftover_umls`) runs last and sweeps up every valid
UMLS concept in MRCONSO that no other compendium already claimed, writing each as a
single-identifier clique into `compendia/umls.txt`. The manual `STY_OVERRIDES` /
`TYPE_COMBO_OVERRIDES` tables at the top of that module — including the "runs last, so an override
never fires for a CUI another pipeline already typed" reach caveat — are documented in the module
itself. See [`docs/sources/UMLS/Leftover.md`](UMLS/Leftover.md) for the coverage report and the
drift test that keeps the tables honest.

## Commit a ranked sample, not a huge derived table

A per-row artifact that runs to thousands of near-identical rows should be committed as a **ranked
top-N slice**, not in full and not gitignored away. The full file is megabytes that no SME reads and
that goes stale on the next build; nothing at all loses the record of what the output looked like on
first ingest. Name the file for the cap (`new-cliques-top-100.csv`) so a reader knows it is a sample
without opening it, and say in the prose page how to regenerate the full version.

The cap is only safe if the writer **ranks before truncating**, and the ranking must put the rows
that would change someone's mind first — for the source-impact report that means identifiers the
Biolink prefix filter would drop, then the largest cliques, then CURIE order for a stable diff. A
blind `rows[:N]` over a CURIE-sorted file keeps an arbitrary identifier-range prefix and can hide
exactly the problems the artifact exists to surface. `write_new_cliques_csv` in
`src/reports/source_impact_details.py` is the worked example; see
[`src/tools/source_impact_report/CLAUDE.md`](../../src/tools/source_impact_report/CLAUDE.md).

### Aggregate when rows are not the unit of interest

A slice is only the right reduction when the rows themselves are what a reviewer reads. When the
interesting structure is *what kinds of rows there are*, aggregate instead: group on the columns
that identify a kind, give each group its total, and attach a handful of example rows so the groups
stay judgeable. The source-impact `new-xrefs-summary.csv` is the worked case — EMAPA's 4,336 xrefs
are one join pathway, so listing 100 of them tells a reviewer far less than one row saying "UBERON
asserts 4,336 `xref`s to EMAPA" plus ten examples.

Two rules that generalise from it:

- **Canonicalise the grouping key, but only where the variation is an artifact.** Sorting a prefix
  pair is right, because which side a concord file writes first is arbitrary. Merging the two
  *asserters* of that pair is wrong, because that is the difference between a mapping this change
  introduces and one that already existed.
- **Keep the examples honest.** Spread them across the group rather than taking the first N, or the
  sample silently becomes "the lowest identifiers".

## Storing generation scripts with the artifact

When a non-trivial script produces a **committed** artifact (an audit CSV, a curated mapping, a
report table) under a source's task directory, commit the script alongside it in a `scripts/`
subdirectory — e.g. `docs/sources/<PREFIX>/<task>/scripts/`. That way the artifact can be
regenerated after an upstream refresh, and the next change can reuse or adapt the script instead of
reverse-engineering how the file was built. Prefer scripts that
**import the production classification/parsing code** rather than reimplementing it, so the
committed artifact cannot drift from the pipeline. Worked example:
`docs/sources/DRUGBANK/food-and-extracts/scripts/generate_csvs.py`, which regenerates the two
DrugBank retype CSVs from the same `classify_food_or_extract` the build uses.

### Do not write unit tests for these scripts

They are documentation, not production code. Their job is to show how a committed artifact was
derived and to be *rewritten* — possibly from scratch, possibly quite differently — by whoever next
needs to redo that analysis. Tests pinning their internals only make that rewrite more expensive,
and they are not on any code path a build depends on. This applies to `/wrap` and any other
coverage sweep: a source script with no tests is the intended state, not a gap.

What *is* worth asserting is the finding itself, where it constrains real output — as a
`pipeline`-marked test over the full downloaded file (see "Investigating a source" above), not as a
unit test of the script that discovered it.

### Replaying a pipeline function beats rebuilding to measure a change

The same shape works for *measuring* a change, not just regenerating an artifact. A completed
build's `babel_outputs/intermediate/` holds exactly the inputs its compendium-building functions
consumed, so a change to one of them can be measured in seconds by importing the production
function and re-running it over those files, rather than paying for a multi-hour rebuild.
`create_typed_sets` re-typed `babel-1.18`'s 293 `Food.txt` cliques from `partials/types` plus
`ids/DRUGBANK_food_extracts` in 20 seconds, giving the exact per-clique before/after split. Import
the production function so the measurement cannot drift from the pipeline, sort the output so
re-runs diff cleanly, and commit the script with its output — worked example:
`docs/sources/DRUGBANK/food-and-extracts/scripts/replay_type_vote.py`.

This complements `babel-clique-diff`, it does not replace it. A replay only sees the cliques the
build already produced, so it cannot show cliques that a change *creates, splits, or moves between
compendia*. Use it to iterate cheaply, then confirm with a real build-vs-build diff.

## Writing the PR for a new source

The PR description for a source addition is read by a **subject-matter expert**, not primarily by a
code reviewer. An SME wants to know two things: *what does this do to the cliques that already
exist*, and *how will this source be represented in Babel*. Everything else is supporting evidence
or belongs in the commits. #781 (EMAPA) is the worked example.

Lead with the effect, not the implementation. This order works:

1. **Summary** — what the source is, how many identifiers it contributes, where the generated
   artifacts live. Two or three sentences.
2. **What this does to existing cliques** — the `babel-clique-diff` result, with the per-compendium
   before/after table. "Zero change rows" is the strongest thing a source addition can say; if it
   is not zero, this is the section that has to explain every row.
3. **How the source is represented** — the typing rule, where the source sits in the pipeline's
   type-precedence order, and a *declared vs. final* count table. Matching totals across that table
   are what show a difference is retyping rather than loss.
4. **Anything the addition fixes or exposes** — pre-existing conflations, identifiers that were
   being silently dropped. Say plainly which problems predate the PR and already ship.
5. **SME sign-off** — each judgement call as its own `- [ ]` checkbox, stating the alternative that
   was rejected and why it is not simply better. A question an SME can answer yes/no to beats a
   paragraph inviting them to form an opinion.
6. **Implementation notes** — compressed to a handful of bullets, prefaced with "none of this
   changes the answers above".
7. **Test plan.**

Spell out the biology where a judgement depends on it. "`GO:0042600` is a cellular component" asks
the SME to look it up; "a mammalian extraembryonic membrane merged with the acellular envelope of
an insect egg" lets them rule on it directly. Link every CURIE to its OBO PURL with the preferred
label, as everywhere else in these docs.

### What to leave out

These accrete over a long review and are worth deleting before asking for sign-off:

- **A changelog of the review iterations** ("what changed since the earlier review"). Once a
  concern is addressed, the current description should read as though it were always correct.
- **Repository housekeeping** — files deleted, `.gitignore` rules, formatting.
- **Follow-up issues about other pipelines.** A registry refactor or a duplicated path in
  `diseasephenotype` is not evidence about this source; the issue links back to the PR anyway.
- **Internal mechanics that no longer affect the outcome.** A latent bug the work surfaced deserves
  one bullet, not a section, once the final configuration means it decides nothing here.

Two failure modes to check for after trimming: a **number that drifts between sections** (#781 had
both 8,078 and 8,098 for the same count — the ids file settled it), and a **forward reference to a
section that was cut**.

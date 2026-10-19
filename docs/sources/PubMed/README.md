# PubMed (via pubmed2db)

Babel's Publication compendium — every PMID with its DOI and PMCID, typed `biolink:Publication` —
is built from the NDJSON export of [pubmed2db](https://github.com/TranslatorSRI/pubmed2db) rather
than from the PubMed XML directly. pubmed2db downloads the PubMed baseline and update files, loads
them into DuckDB, and exports one record per PMID. That replaced Babel's own XML parser, which was
the single longest rule in the build (20 hours single-threaded in 2026jul22) and which neither
de-duplicated revised articles nor honoured `DeleteCitation`.

## Pinning an export

**Whenever you touch the PubMed code, and before every release, check
<https://stars.renci.org/var/babel_outputs/pubmed2db/> for a newer export.** The network test
`tests/createcompendia/test_publications_export.py::test_pinned_export_is_the_newest` fails when
there is one.

Three settings at the top of `config.yaml` pin the export a build reads:

- `pubmed2db_base_url` — where pubmed2db publishes its exports.
- `pubmed2db_version` — the export directory under it, e.g. `2026sep26`.
- `pubmed2db_validation_report` — the export's validation report, relative to that directory. It
  can't be derived from the version and has moved before, so look it up each time you move
  `pubmed2db_version`.

`rule download_pubmed2db` downloads the report, then exactly the shards it lists, into
`babel_downloads/PubMed2DB/<version>/`; `generate_pubmed_concords` and `generate_pubmed_compendia`
read the shards from there. Because every path includes the version, moving `pubmed2db_version`
re-runs all three rules, and nothing from an older export can be reused by mistake. The directory
can be carried forward between runs — see "Preloading PubMed downloads" in
[`RunningBabel.md`](../../RunningBabel.md).

### pubmed2db's data model version

pubmed2db [versions its outputs][p2db-versioning] with a semantic "data model" version, separate
from its code version. Data model 1.0.0 is still in development
([pubmed2db#63](https://github.com/TranslatorSRI/pubmed2db/pull/63)), so 2026sep26 and every earlier
export carry no version. For the NDJSON, 1.0 changes no field (see its [changelog][p2db-changelog]),
but two things change when you pin the first export made with it:

- **Exports are named for their corpus cutoff**, the UTC date NCBI posted the last file pubmed2db
  loaded (`metadata.json`'s `corpus.build_name`), rather than the day the export finished. Existing
  directories keep their names. The latest-dated directory, which is what
  `test_pinned_export_is_the_newest` looks for, therefore holds the newest corpus.
- **Each export says which data model it follows**, as `data_model_version` in `metadata.json` and
  in the validation report. Only a major bump can rename, remove or change the meaning of a field,
  and pubmed2db's changelog entry for each one says what an NDJSON consumer has to do, so read it
  before moving `pubmed2db_version` across one. Babel does not check the version yet.

[p2db-versioning]: https://github.com/TranslatorSRI/pubmed2db/blob/68eceab/docs/data-model.md#versioning-and-stability
[p2db-changelog]: https://github.com/TranslatorSRI/pubmed2db/blob/68eceab/docs/data-model.md#changelog

## The export contract Babel relies on

### Layout

As of 2026sep26, an export directory holds:

- `pubmed_metadata_0.ndjson.gz` … `pubmed_metadata_7.ndjson.gz` — 8 shards of ~2.1 GB, 41,153,516
  records in all.
- `manifests/validation_report-20260926.json` — pubmed2db's validation report.
- `manifests/pmids-20260926.txt.gz` and `pubmed.duckdb` (~76 GB) — pubmed2db's own PMID manifest
  and database. Babel ignores both, which is why it downloads from the report's shard list rather
  than crawling the directory.

The layout is not stable between exports. 2026aug5 had 16 shards named `pubmed_metadata_00000` …
`00015` and a gzipped `validation_report.json.gz` at the top level, whose `records_by_shard` keys
lacked the `.gz`. Check a new export's layout against this section before pinning it.

Exports made with data model 1.0 also hold `metadata.json` (the data model version, the record
count, the corpus cutoff, and each file's size and SHA-256) and `SHA256SUMS` (the same hashes, for
`sha256sum -c`) beside the shards. 2026sep26 and earlier have neither. pubmed2db's
[recipe for checking a download][p2db-checking] is the one to follow once Babel reads them.

[p2db-checking]: https://github.com/TranslatorSRI/pubmed2db/blob/68eceab/docs/data-model.md#getting-a-build-and-checking-it

### The validation report

Babel reads three things from the report, and nothing from its human-readable
`checks_run[].observed` strings:

- `status` — an export whose validator reported `fail` is refused outright. `warn` is accepted.
  It is worth reading a new export's warnings anyway: of 2026sep26's, `invalid_months` concerns a
  field Babel doesn't read, but `identifier_mismatches` found one of 240 sampled records (PMID
  6491726) exported without the DOI Entrez gives it, and `field_mismatches` includes titles whose
  punctuation and whitespace differ from Entrez's (PMID 10205128 is exported as
  `"Rationing-talk and action in health care "`, Entrez has `"Rationing-talk and action in health
  care."`).
- `checks.structure.records_by_shard` — `{shard filename: record count}`. This is the list of
  shards to download and parse, and `generate_pubmed_concords` raises if any shard parses to a
  different count, naming the shard. A truncated or corrupted shard doesn't need the count: each
  shard is a single gzip stream, so damage already fails decompression in `_read_shard()`. What the
  count catches is an intact shard from a different export. From data model 1.0, `SHA256SUMS`
  catches that too, even when the substitute has the same count, but Babel doesn't check it yet.
- `checks.structure.records_total` — must equal the sum of `records_by_shard`.

### Records

Each line of a shard is a flat JSON record; Babel reads three of its fields:

```json
{"id": "PMID:33", "identifiers": ["PMID:33", "doi:10.1111/j.1365-2141.1975.tb01817.x"], "article_title": "Effect of human erythrocyte stromata on complement activation.", "...": "..."}
```

- `id` — the PMID CURIE.
- `identifiers` — the PMID first, then its DOI and PubMed Central CURIEs, sorted and de-duplicated.
  DOIs are `doi:`. PubMed Central ids are `PMC:PMC123` in 2026aug5 (2026jun30 has no `identifiers`
  field at all) and `PMCID:PMC123` from 2026aug21 on, which Babel respells `PMC:` (see below). Only
  those two `ArticleId` types are promoted to CURIEs; the rest stay in pubmed2db. A record whose
  list is empty or doesn't start with its own PMID fails the build. PubMed publishes DOIs as
  supplied, so a few are malformed (`doi:10.1126/science. 1134405`, ~140 in shard 0 of 2026sep26);
  `write_compendium()` fails the build on any CURIE with whitespace, `|` or a control character, so
  `_read_shard()` skips those and the concord metadata counts them as
  `skipped_malformed_article_ids`. Repairing them instead is
  [Babel#1112](https://github.com/NCATSTranslator/Babel/issues/1112).
- `article_title` — becomes the PMID's label (see "Titles"). Absent values are `""`, never null.

pubmed2db puts every NDJSON field in its "stable" tier: from data model 1.0, none of these three
changes except with a major version (see "pubmed2db's data model version" above).

What pubmed2db guarantees, and Babel therefore does not re-check record by record:

- **One record per PMID, corpus-wide.** Superseded versions of an article are resolved inside
  pubmed2db (`latest_article`), and the validator's `pmid-unique` check asserts zero duplicates
  across all shards.
- **Deleted PMIDs are absent.** `DeleteCitation` entries are applied, so a retracted-and-removed
  citation never reaches Babel.
- **Record order and shard membership are arbitrary.** 2026sep26 is not sorted by PMID. Babel's
  output follows shard order and record order within a shard, so it is deterministic for a given
  export; nothing depends on the order otherwise.

## How the compendium is built

`src/createcompendia/publications.py` makes two passes over the shards:

1. `generate_pubmed_concords` parses the shards in parallel, one worker per shard. Each worker
   writes its shard's rows to part files and holds one record at a time; the parts are then
   concatenated in shard order into `intermediate/publications/ids/PMID`
   (`PMID:x\tbiolink:JournalArticle`), `intermediate/publications/concords/PMID_DOI`
   (`PMID:x\teq\t<doi: or PMC:>`) and `babel_downloads/PubMed2DB/titles.tsv` (see "Titles"). The
   metadata YAML records the export URL as the source. The parent's memory is dominated by the set
   of every DOI and PMCID, built while looking for shared identifiers (below).
2. `generate_pubmed_compendia` streams the shards straight into `write_compendium()`, one record at
   a time in a single process: a record *is* a clique, so there is no `glom()` and no global labels
   dictionary (the old rule peaked at 126 GiB holding both). `write_compendium()` is the bottleneck
   here, so parsing ahead in worker processes would save little time and cost a parsed shard's worth
   of memory per worker.

Measured locally on shard 0 of 2026sep26 (5,252,405 records, about an eighth of the export), pass
1 took 31 s and peaked at 1.27 GB, nearly all of it the set of 5.7M DOIs and PMCIDs (~210 bytes
each), so ~10 GB for the whole export. Pass 2 took 102 s (~51k cliques/s) at 0.10 GB, which does not
grow with the corpus, plus the ~3 GB every compendium rule spends loading the common UberGraph
files. Pass 2 only stays flat because the synonym, description and property lookups don't store
misses (`tests/node/test_lookups_do_not_grow.py`); before that was fixed it grew by ~2 KB per
clique, which would have been ~80 GB here. The rule sizes in `publications.snakefile` and
`slurm/README.md` come from these numbers; replace them with the first cluster benchmark.

### Identifiers shared by more than one PMID

PubMed publishes DOIs and PMCIDs as supplied, so the same DOI occasionally appears on several
PMIDs. Two cliques must never share an identifier, and the old `glom(unique_prefixes=[PMID])`
handled this by silently *ignoring* a concord that would have joined two PMIDs — the first PMID
seen kept the identifier. The new code makes the same choice order-independent: pass 1 writes
`intermediate/publications/concords/shared_identifiers.tsv` (`identifier\twinner_pmid\tn_records`),
naming the **lowest PMID** that carries each shared identifier, and pass 2 keeps the identifier in
that clique only. The file doubles as a data-quality report of ambiguous identifiers.

## Titles

Each PMID's label in the Publication compendium — and so its `preferred_name` — is its
`article_title` exactly as exported; an empty title gives no label. This is a deliberate change from
the XML era, when labels were read back from `titles.tsv`: a newline used to arrive as the two
characters `\n` and trailing whitespace was stripped by the loader, while now the label has a real
newline and keeps its whitespace. The new label is the exported text rather than an artifact of an
intermediate file. The XML parser also kept only the text before a title's first inline element
(ElementTree's `.text`): the released build that Node Normalization served on 2026-10-19 labels
[`PMID:35249155`](https://pubmed.ncbi.nlm.nih.gov/35249155/) "A comparison of water-gas shift
reaction on ZnO", where pubmed2db exports the whole title. Publication's synonyms file is empty
(names come from per-prefix synonym files, and PMID has none), so titles do not reach Name
Resolution either way.

What titles contain, from [`titles/characterize_titles.py`](titles/characterize_titles.py) over
shard 0 of 2026sep26 (5,252,405 records; examples in [`titles/counts.json`](titles/counts.json)):

| Shape | Titles | Notes |
| --- | ---: | --- |
| empty | 4,169 | No label. |
| trailing whitespace | 728 | Mostly a single space (`PMID:9245294`). |
| leading whitespace | 81 | Includes non-breaking and thin spaces, and whole titles wrapped in indentation. |
| newline | 110 | Line-wrapped titles (`PMID:30803192`) and flattened MathML (`PMID:35249155`). |
| backslash | 37 | Mostly LaTeX math. |
| other control | 18 | C1 characters from mis-decoded cp1252, e.g. `\x93smart\x94`. |
| tab, carriage return, whitespace-only | 0 | |

Some of these are formatting artifacts rather than content — MathML flattened into runs of
newlines and spaces, indentation around a whole title. Babel does not normalise them; collapsing
whitespace in labels would be a separate decision.

`titles.tsv` itself is not read by the build: it exists to diff titles between builds. Titles are
written through `escape_tsv_field()`, which escapes backslash, tab, newline and carriage return, so
every row has exactly two fields and the original can be recovered. The XML parser escaped only
newlines, so the one visible difference from an older `titles.tsv` is that backslashes are now
doubled. `tests/pipeline/test_publications.py` asserts the two-field shape over a whole export.

## `PMC:` versus `PMCID:`

Every Babel release so far has written PubMed Central identifiers as `PMC:PMC1234567`. pubmed2db's
2026aug5 export spells them that way too, but its exports from 2026aug21 on, including the pinned
2026sep26, spell them `PMCID:PMC1234567`. Babel keeps `PMC:`. `_read_shard()` respells the export's
`PMCID:` according to `PUBMED2DB_PREFIX_RENAMES` in `src/createcompendia/publications.py`, so the
concords, `shared_identifiers.tsv` and the compendium all say `PMC:`, whichever spelling the export
used. The reasons:

- **Consistency while the question is open.** Which prefix Translator should use is
  [Babel#1044](https://github.com/NCATSTranslator/Babel/issues/1044), and
  [pubmed2db#33](https://github.com/TranslatorSRI/pubmed2db/issues/33) for the export. Consumers of
  Babel's output already hold `PMC:` CURIEs, and Node Normalization resolves `PMC:PMC1234567` but
  returned `null` for `PMCID:PMC1234567` on 2026-10-19. Following the export would rename ~12M
  CURIEs before that is decided, and rename them back if the answer is `PMC:`.
- **Biolink registers `PMC` on the type Babel uses.** Biolink 4.4.3 and 4.4.5 register `PMC` on
  `biolink:Publication`, the type of every clique here, and `PMCID` only on
  `biolink:JournalArticle`. `PMC:` therefore needs no `extra_prefixes`, which a `PMCID:` would need
  to avoid being silently dropped by `create_node()`.

Neither spelling expands to a working URL with Biolink's prefix map (#1044 shows both), so that
doesn't decide it either way. Switching to `PMCID:` later is the change described in the
`PUBMED2DB_PREFIX_RENAMES` comment: delete the mapping and pass `extra_prefixes=["PMCID"]`.

pubmed2db's data model 1.0 ([pubmed2db#63](https://github.com/TranslatorSRI/pubmed2db/pull/63)) goes
back to `PMC:` to match Babel, as an interim choice tracked in pubmed2db#33. Once `pubmed2db_version`
names a 1.0 build, the respelling renames nothing; check pubmed2db#33 then, and either delete the
mapping or make the `PMCID:` change above.

## What was dropped

The XML parser also wrote `babel_downloads/PubMed/statuses.jsonl.gz`, every `PubStatus` a PMID had
ever carried, as a placeholder for one day exposing whether a publication had been retracted
(#155). Nothing ever read it and pubmed2db does not export statuses, so it is gone. The idea — a
per-version status such as "Retracted" or "Updated" — is tracked as
[Babel#1049](https://github.com/NCATSTranslator/Babel/issues/1049) and
[pubmed2db#45](https://github.com/TranslatorSRI/pubmed2db/issues/45), which links the old code.

# PubMed (via pubmed2db)

Babel's Publication compendium — every PMID with its DOI and PMCID, typed `biolink:Publication` —
is built from the NDJSON export of [pubmed2db](https://github.com/TranslatorSRI/pubmed2db) rather
than from the PubMed XML directly. pubmed2db downloads the PubMed baseline and update files, loads
them into DuckDB, and exports one record per PMID. That replaced Babel's own XML parser, which was
the single longest rule in the build (20 hours single-threaded in 2026jul22) and which neither
de-duplicated revised articles nor honoured `DeleteCitation`.

## Pinning an export

`config.yaml: pubmed2db_url` names the export directory a build reads, e.g.
`https://stars.renci.org/var/babel_outputs/pubmed2db/2026aug5/`. It sits with the other
per-release versions at the top of `config.yaml` and should be moved to the newest export before
every build. `rule download_pubmed2db` fetches everything under that URL into
`babel_downloads/PubMed2DB/`; `generate_pubmed_concords` and `generate_pubmed_compendia` then read
the shards from there. The directory can be carried forward between runs — see "Preloading PubMed
downloads" in [`RunningBabel.md`](../../RunningBabel.md).

## The export contract Babel relies on

An export directory contains `pubmed_metadata_*.ndjson.gz` shards (16 × ~1.1 GB, ~41M records in
2026aug5) and a `validation_report.json.gz`. Each line is a flat JSON record; Babel reads three of
its fields:

```json
{"id": "PMID:33", "identifiers": ["PMID:33", "doi:10.1111/j.1365-2141.1975.tb01817.x"], "article_title": "Effect of human erythrocyte stromata on complement activation.", "...": "..."}
```

- `id` — the PMID CURIE.
- `identifiers` — the PMID first, then its DOI and PubMed Central CURIEs, sorted and de-duplicated.
  DOIs are `doi:`; PubMed Central ids are `PMC:PMC123` in exports up to 2026aug5 and `PMCID:PMC123`
  in later ones (see below). Only those two `ArticleId` types are promoted to CURIEs; the rest stay
  in pubmed2db.
- `article_title` — becomes the PMID's label. Absent values are `""`, never null.

What pubmed2db guarantees, and Babel therefore does not re-check record by record:

- **One record per PMID, corpus-wide.** Superseded versions of an article are resolved inside
  pubmed2db (`latest_article`), and the validator's `pmid-unique` check asserts zero duplicates
  across all shards.
- **Deleted PMIDs are absent.** `DeleteCitation` entries are applied, so a retracted-and-removed
  citation never reaches Babel.
- **Record order and shard membership are arbitrary.** Nothing in Babel depends on either.

Babel does check the export as a whole: `parse_pubmed2db_into_tsvs()` refuses an export whose
validation report has `status: fail`, and raises if the number of records it parsed differs from the
report's `records-present` count — the only way to notice a truncated shard, since no per-shard
checksum is published.

## How the compendium is built

`src/createcompendia/publications.py` makes two passes over the shards, each parsed in parallel
(`threads` shards at a time):

1. `generate_pubmed_concords` writes the same three TSVs the XML parser did, so build-vs-build
   diffs still line up: `intermediate/publications/ids/PMID` (`PMID:x\tbiolink:JournalArticle`),
   `babel_downloads/PubMed2DB/titles.tsv`, and `intermediate/publications/concords/PMID_DOI`
   (`PMID:x\teq\t<doi: or PMCID:>`). The metadata YAML records `pubmed2db_url` as the source.
2. `generate_pubmed_compendia` streams the shards straight into `write_compendium()`: a record
   *is* a clique, so there is no `glom()` and no global labels dictionary (the old rule peaked at
   126 GiB holding both).

### Identifiers shared by more than one PMID

PubMed publishes DOIs and PMCIDs as supplied, so the same DOI occasionally appears on several
PMIDs. Two cliques must never share an identifier, and the old `glom(unique_prefixes=[PMID])`
handled this by silently *ignoring* a concord that would have joined two PMIDs — the first PMID
seen kept the identifier. The new code makes the same choice order-independent: pass 1 writes
`intermediate/publications/concords/shared_identifiers.tsv` (`identifier\twinner_pmid\tn_records`),
naming the **lowest PMID** that carries each shared identifier, and pass 2 keeps the identifier in
that clique only. The file doubles as a data-quality report of ambiguous identifiers.

### `PMCID:` versus `PMC:`

Babel historically wrote PubMed Central identifiers as `PMC:PMC1234567`, as does the 2026aug5
export; pubmed2db exports made after it spell them `PMCID:PMC1234567`. Which one Translator should
use is [Babel#1044](https://github.com/NCATSTranslator/Babel/issues/1044), and until it is settled
Babel passes the export's spelling through unchanged, so the first build on a newer export will
rename every PMC CURIE. Two things to know:

- Biolink 4.4.3 registers `PMC` on `biolink:Publication` but `PMCID` only on
  `biolink:JournalArticle`, so `generate_compendium()` passes `extra_prefixes=["PMCID"]` — without
  it `create_node()` would silently drop every PMCID. That is the only place Babel spells the
  prefix out; settling on `PMC` would be a re-export from pubmed2db and nothing in Babel.
- `extra_prefixes` identifiers can never become a clique's preferred CURIE. PMID always wins for
  publications anyway, so this has no visible effect.

## What was dropped

The XML parser also wrote `babel_downloads/PubMed/statuses.jsonl.gz`, every `PubStatus` a PMID had
ever carried, as a placeholder for one day exposing whether a publication had been retracted
(#155). Nothing ever read it and pubmed2db does not export statuses, so it is gone. The idea — a
per-version status such as "Retracted" or "Updated" — is tracked as
[Babel#1049](https://github.com/NCATSTranslator/Babel/issues/1049) and
[pubmed2db#45](https://github.com/TranslatorSRI/pubmed2db/issues/45), which links the old code.

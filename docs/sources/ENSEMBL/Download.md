# Ensembl download

How Babel obtains Ensembl gene and protein identifiers and their cross-references. There are
two backends, selected by `ensembl_source` in `config.yaml`:

| Backend | Source | Coverage | Cost | When to use |
|---------|--------|----------|------|-------------|
| `biomart` (default) | Ensembl BioMart API via `apybiomart` | Every Ensembl species (200+) | Slow, flaky, long network harvest | Full production builds |
| `mygene` | MyGene.info BioThings API | Major model organisms only | Lightweight, resumable, offline-testable | Development on a laptop; the well-covered species |

The download handler for BioMart is `src/datahandlers/ensembl.py`; for MyGene it is
`src/datahandlers/ensembl_mygene.py`. Both are driven by the `get_ensembl` rule in
`src/snakefiles/datacollect.snakefile` and both write the **same** on-disk artifact —
`babel_downloads/ENSEMBL/<dataset>/BioMart.tsv` with the BioMart display-header columns — so
the downstream consumers are agnostic to the backend.

## Downstream consumers

| Rule | Function | Output | Consumed by the gene/protein build? |
|------|----------|--------|--------------------------------------|
| `gene_ensembl_ids` | `gene.write_ensembl_gene_ids()` | `intermediate/gene/ids/ENSEMBL` | **Yes** (`ENSEMBL` is in `gene_ids`) |
| `protein_ensembl_ids` | `protein.write_ensembl_protein_ids()` | `intermediate/protein/ids/ENSEMBL` | **Yes** (`ENSEMBL` is in `protein_ids`) |
| `get_gene_ensembl_relationships` | `gene.build_gene_ensembl_relationships()` | `intermediate/gene/concords/ENSEMBL` | **No** — see [Known issues](#known-issues) |

Both ids functions walk `babel_downloads/ENSEMBL/*/BioMart.tsv` and read two header columns by
exact string: `Gene stable ID` and `Protein stable ID`. **These header strings are a hard
contract** — any new backend must emit them verbatim. `build_gene_ensembl_relationships`
additionally keys its xref extraction on the headers `NCBI gene (formerly Entrezgene) ID`,
`ZFIN ID`, `SGD gene name ID`, `WormBase Gene ID`, `FlyBase ID`, `MGI ID`, and `RGD ID`.

## The BioMart backend (`ensembl_source: biomart`)

### Why BioMart, and why it is fragile

Ensembl does not publish a simple bulk identifier list that can be fetched without downloading
hundreds of gigabytes of surrounding data, so `pull_ensembl()` queries the BioMart API for each
species dataset instead. BioMart is unreliable and the download logic is written defensively
against two known failure modes:

1. **HTML error pages instead of TSV data.** BioMart occasionally returns an HTML error page
   where the TSV payload is expected, causing a pandas parse error deep in
   `TextReader._convert_column_data`. It is transient — the same dataset usually succeeds on a
   later attempt.
2. **SSL certificate failures.** `apybiomart` runs a redundant HTTPS pre-flight connectivity
   check that fails in some HPC environments; it is bypassed in `ensembl.py`
   ([apybiomart#131](https://github.com/robertopreste/apybiomart/issues/131),
   [Babel#588](https://github.com/NCATSTranslator/Babel/pull/588)).

Because of these, **each dataset is retried up to `BIOMART_MAX_RETRIES` (5) times** with a
`BIOMART_RETRY_DELAY_SECS` (30 s) pause. A dataset that exhausts its retries is recorded and the
job continues; the run fails only at the very end. **Do not remove or weaken this
retry/continue-on-failure logic** — aborting a multi-hour, 200-species harvest on the first
transient error wastes already-completed work.

### Attribute batching and the real BioMart limit

`pull_ensembl()` discovers which desired attributes a species offers (`apybiomart.find_attributes`)
and downloads them. BioMart rejects a query that selects **too many external-reference attributes**
with `Query ERROR: ... Too many attributes selected for External References`. Note this limit is on
*external-reference* attributes specifically, not on total attributes; a live test against release
116 (2026-07) failed at **4** external-reference attributes, i.e. a practical limit of about **3**.

The code batches at `BIOMART_MAX_ATTRIBUTE_COUNT = 6` *total* attributes per query (each batch also
carries `ensembl_gene_id` so batches can be rejoined on `Gene stable ID`). Because the limit is on
external-reference attributes rather than total attributes, a batch of 6 that contains 4+ xref
attributes can still trip the error — worth keeping in mind if the desired-attribute set changes.
The desired attributes are:

| Attribute | Use |
|-----------|-----|
| `ensembl_gene_id` | primary ENSEMBL gene CURIE (`Gene stable ID` header) |
| `ensembl_peptide_id` | ENSEMBL protein CURIE (`Protein stable ID` header) |
| `description` | human-readable description |
| `external_gene_name` / `external_gene_source` / `external_synonym` | symbol authority + synonyms |
| `chromosome_name` / `source` / `gene_biotype` | location + annotation |
| `entrezgene_id`, `zfin_id_id`, `mgi_id`, `rgd_id`, `flybase_gene_id`, `sgd_gene`, `wormbase_gene` | cross-references |

Not every attribute exists for every species; the handler takes the intersection of desired and
available attributes.

### Permanently broken datasets — the skip list

Some datasets fail on every attempt (species retired from Ensembl, no gene identifiers, a persistent
schema mismatch). When a dataset fails repeatedly across runs, **add its BioMart dataset ID to
`ensembl_datasets_to_skip` in `config.yaml`** rather than letting it block every future run, and
leave a comment saying why so a future maintainer can retry it in a later Ensembl release.

### Resumability

The `get_ensembl` rule declares only the sentinel `BioMartDownloadComplete` as its output — **not**
the `ENSEMBL/` directory. On failure Snakemake deletes only the sentinel; already-written
per-dataset `BioMart.tsv` files survive and are skipped on the next run via the
`if os.path.exists(outfile)` guard. A run that fails after 180 of 200 datasets restarts and
downloads only the remaining 20. **Do not declare `directory(ENSEMBL)` as an output** — that would
make Snakemake wipe the whole directory on failure.

## The MyGene.info backend (`ensembl_source: mygene`)

[MyGene.info](https://mygene.info) is a BioThings API the Translator already trusts as a registered
Knowledge Provider. It aggregates Ensembl gene/protein stable IDs with the cross-references Babel
needs, and a full species can be retrieved with a scrolling query (`fetch_all`) — no bulk download,
no 200-species harvest. This makes it practical to develop and test the Ensembl logic on a laptop.

### Coverage (verified against the live API, 2026-07)

`ensembl_mygene_taxa` in `config.yaml` lists the species MyGene covers well enough to substitute for
BioMart (NCBI taxid → BioMart-style dataset directory name):

| Taxid | Species | Dataset dir | Gene objects with `ensembl.gene` |
|-------|---------|-------------|----------------------------------|
| 9606 | human | `hsapiens_gene_ensembl` | ~83k |
| 10090 | mouse | `mmusculus_gene_ensembl` | ~78k |
| 10116 | rat | `rnorvegicus_gene_ensembl` | ~43k |
| 7955 | zebrafish | `drerio_gene_ensembl` | ~38k |
| 6239 | worm | `celegans_gene_ensembl` | ~47k |
| 7227 | fly | `dmelanogaster_gene_ensembl` | ~24k |
| 8364 | xenopus | `xtropicalis_gene_ensembl` | ~25k |

**Yeast (4932) and dicty (44689) are deliberately excluded**: MyGene has ~2 yeast genes and 0 dicty
genes carrying an `ensembl.gene`, and it has no SGD field. A full build that needs those species
must use `biomart` (or run `mygene` for the species above and `biomart` for the rest). MyGene also
excludes Ensembl "Pre!" genes; for fly and worm the `ensembl.gene` values are Ensembl-Genomes
identifiers (`FBgn…` / `WBGene…`), which is what Babel expects under the `ENSEMBL:` prefix.

### Field mapping and the MGI prefix trap

`ensembl_mygene.py` maps MyGene fields onto the BioMart display-header columns and normalizes the
str-or-list shapes MyGene returns (`ensembl.protein` is a scalar for a single-protein gene but a
list for a multi-isoform gene; `ensembl` itself can be a list of dicts when one MyGene gene
aggregates several Ensembl gene entries, common in zebrafish — proteins are kept scoped to their own
entry).

One mapping needs special care: **MyGene returns MGI already prefixed** (`"MGI:3704398"`), but the
BioMart `MGI ID` column is bare and the downstream consumer prepends `MGI:` itself. The handler
therefore strips a leading `MGI:` before writing the cell; otherwise the concord would contain
`MGI:MGI:3704398`. The other MOD identifiers (ZFIN `ZDB-GENE-…`, RGD, FlyBase `FBgn…`, WormBase
`WBGene…`) arrive bare and pass through unchanged.

### Enabling, resumability, and testing

Set `ensembl_source: mygene` in `config.yaml` (and adjust `ensembl_mygene_taxa` if needed). The pull
writes the same `<dataset>/BioMart.tsv` files and a `BioMartDownloadComplete` sentinel recording
per-dataset status and `num_genes`/`num_rows` counts. Each dataset is written **atomically** — to
`BioMart.tsv.part`, then `os.replace`d onto `BioMart.tsv` only after the whole taxon is pulled — so
a mid-pull failure never leaves a partial file that a later run would mistake for complete; a
complete `BioMart.tsv` is skipped on re-run for resumability (as with the BioMart backend).

Every network touch point is behind an injectable `http_get(url, params) -> dict`, so the parsing
and paging logic is unit-tested **offline** against real MyGene responses captured verbatim in
`tests/data/ensembl_mygene/` (each fixture names the gene id it was captured from). Only
`_default_http_get` speaks HTTP. Run the offline tests with
`uv run pytest -m unit tests/datahandlers/test_ensembl_mygene.py`; a live smoke test runs with
`--network`.

## Strategic caveat — Ensembl platform retirement

Ensembl release **116 (June 2026) is the final release on the legacy `www.ensembl.org` platform**,
which Ensembl states will be **retired in July 2026** in favour of the new platform at
`beta.ensembl.org` (legacy archive at `jun2026.archive.ensembl.org`). This clouds the longevity of
the BioMart endpoint, the Ensembl FTP dumps, and the public MySQL server that the `biomart` backend
relies on. `rest.ensembl.org` still serves release 116 today. Any long-lived change here should be
re-checked against the new platform before it is relied upon, and the Ensembl release should be
pinned consistently across sources to avoid clique drift.

## Known issues

- **`build_gene_ensembl_relationships` renders prefixes as Python sets.** Its `column_to_prefix`
  maps each header to a *set* (`{NCBIGENE}`) and writes `f"{pref}:{value}"`, so an edge renders as
  `{'NCBIGene'}:1017` instead of `NCBIGene:1017`. This is a latent bug but it is **dormant**: the
  `get_gene_ensembl_relationships` rule writes `gene/concords/ENSEMBL`, and `ENSEMBL` is **not** in
  `config.yaml gene_concords`, so `gene_compendia` never consumes that file. The live
  Ensembl↔NCBIGene linkage comes from the `NCBIGeneENSEMBL` concord (`gene2ensembl.gz`). This is
  pinned by `test_round_trip_build_gene_ensembl_relationships_pins_current_set_prefix_behavior` in
  `tests/datahandlers/test_ensembl_mygene.py`; invert that test if the rendering is fixed.
- **MyGene has no SGD field**, so the `mygene` backend never populates the `SGD gene name ID`
  column; yeast cross-references require the `biomart` backend.

## Validating a backend switch before production

Changing `ensembl_source` changes gene/protein clique membership. Before flipping the default (or
relying on `mygene` for a release), run on the cluster:

```bash
uv run source-impact-report --source ENSEMBL   # see src/tools/source_impact_report/CLAUDE.md
uv run babel-clique-diff ...                    # see src/tools/clique_diff/CLAUDE.md (restructures cliques)
```

and compare MyGene-vs-BioMart coverage at the **same Ensembl release**. These need intermediate
build artifacts and cannot run on a laptop.

## Related code and issues

- `src/datahandlers/ensembl.py` — `pull_ensembl()` (BioMart backend).
- `src/datahandlers/ensembl_mygene.py` — `pull_ensembl_via_mygene()` (MyGene backend).
- `src/createcompendia/gene.py` — `write_ensembl_gene_ids()`, `build_gene_ensembl_relationships()`.
- `src/createcompendia/protein.py` — `write_ensembl_protein_ids()`.
- `src/snakefiles/datacollect.snakefile` — `get_ensembl` rule (backend dispatch).
- `config.yaml` — `ensembl_source`, `ensembl_mygene_taxa`, `ensembl_datasets_to_skip`.
- `tests/datahandlers/test_ensembl_mygene.py` — offline unit tests + live smoke test.
- [Babel#588](https://github.com/NCATSTranslator/Babel/pull/588) — SSL bypass workaround.

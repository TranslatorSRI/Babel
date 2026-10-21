# Ensembl download via BioMart

This document describes how Babel downloads Ensembl gene and protein identifiers, the
quirks of the BioMart API, and how to handle datasets that fail or should be permanently
skipped.

The download handler is `src/datahandlers/ensembl.py`; the Snakemake rule is `get_ensembl`
in `src/snakefiles/datacollect.snakefile`.

## Why BioMart and why it is fragile

Ensembl does not publish a simple bulk identifier dump. The only practical way to retrieve
the full set of gene identifiers across all species is the BioMart API, accessed via the
`apybiomart` Python library.

**BioMart is unreliable and the download logic must be written defensively.** Two known
failure modes:

1. **HTML error pages instead of TSV data.** BioMart occasionally returns an HTML error
   page (e.g. a 500 or "service unavailable") where the TSV payload is expected. This
   causes a pandas parse error deep in `TextReader._convert_column_data`. It is transient
   — the same dataset usually succeeds on a second or third attempt.

2. **SSL certificate failures.** `apybiomart` includes a pre-flight connectivity check
   over HTTPS that fails in some HPC environments. This check is redundant (a real
   connectivity failure surfaces during the actual query anyway) and is disabled in
   `ensembl.py` until the library is fixed
   ([apybiomart#131](https://github.com/robertopreste/apybiomart/issues/131),
   [Babel#588](https://github.com/NCATSTranslator/Babel/pull/588)).

Because of these failure modes, **the download code retries each dataset up to
`BIOMART_MAX_RETRIES` times** (currently 5) with a `BIOMART_RETRY_DELAY_SECS`-second
pause between attempts (currently 30 s). If a dataset exhausts all retries, the error is
recorded and the download continues with the remaining datasets — the job only fails at the
very end. On the next Snakemake retry, only the datasets that actually failed are
re-attempted (see "Resumability" below).

**Do not remove or weaken this retry / continue-on-failure logic.** Hundreds of species
datasets are downloaded in a single job run; aborting the whole run on the first transient
error wastes hours of already-completed work.

## Permanently broken datasets — adding to the skip list

Some datasets fail on every attempt, not just transiently. Common causes: the species has
been retired from Ensembl, the dataset has no gene identifiers, or a schema mismatch causes
a persistent parse error.

When a dataset fails repeatedly across multiple pipeline runs, **add it to
`ensembl_datasets_to_skip` in `config.yaml`** rather than letting it block every future
run:

```yaml
ensembl_datasets_to_skip: [elucius_gene_ensembl, hgfemale_gene_ensembl, ...]
```

The currently skipped datasets are listed there. Each entry is a BioMart dataset ID (the
value in `apybiomart.find_datasets()["Dataset_ID"]`). Add a comment in the config or a note
in this file if you know *why* a dataset is broken, so future maintainers can decide whether
to retry it in a later Ensembl release.

## Download structure

`pull_ensembl()` enumerates all BioMart datasets via `apybiomart.find_datasets()`, skips any
in `ensembl_datasets_to_skip`, then for each remaining dataset:

1. Discovers which of the desired attributes are available for that species via
   `apybiomart.find_attributes()`.
2. Downloads those attributes. If more than `BIOMART_MAX_ATTRIBUTE_COUNT` (6) attributes
   are available, they are fetched in batches of 6 (BioMart rejects larger requests;
   see [bioconductor post](https://support.bioconductor.org/p/39744/#39751)). Each batch
   always includes `ensembl_gene_id` so batches can be joined on `Gene stable ID`.
3. Writes the merged DataFrame as a tab-separated file at
   `babel_downloads/ENSEMBL/<dataset_id>/BioMart.tsv`.

The desired attributes (columns) are:

| Attribute | Use |
|-----------|-----|
| `ensembl_gene_id` | primary ENSEMBL gene CURIE |
| `ensembl_peptide_id` | ENSEMBL protein CURIE |
| `description` | human-readable description |
| `external_gene_name` | gene symbol from the source authority |
| `external_gene_source` | which authority provided the symbol |
| `external_synonym` | alternative gene names |
| `chromosome_name` | chromosome (used to filter alt/patch scaffolds) |
| `source` | data source annotation |
| `gene_biotype` | gene type (protein_coding, lncRNA, …) |
| `entrezgene_id` | NCBI Gene cross-reference |
| `zfin_id_id` | ZFIN cross-reference |
| `mgi_id` | MGI cross-reference |
| `rgd_id` | RGD cross-reference |
| `flybase_gene_id` | FlyBase cross-reference |
| `sgd_gene` | SGD cross-reference |
| `wormbase_gene` | WormBase cross-reference |

Not every attribute is available for every species dataset; the handler takes the
intersection of desired and available attributes.

When all datasets have been processed, `pull_ensembl()` writes a JSON summary to
`babel_downloads/ENSEMBL/BioMartDownloadComplete`. This sentinel file is the only declared
Snakemake output; downstream rules depend on it rather than the directory.

## Resumability

The Snakemake rule declares only `BioMartDownloadComplete` as its output — **not** the
`ENSEMBL/` directory. When the rule fails, Snakemake deletes only the sentinel file.
Already-downloaded per-dataset `BioMart.tsv` files survive and are skipped on the next run
via the `if os.path.exists(outfile): continue` guard in `pull_ensembl()`.

This means a run that fails after downloading 180 of 200 datasets will restart and only
download the remaining 20. Do not change the rule to declare `directory(ENSEMBL)` as an
output — that would cause Snakemake to wipe the whole directory on failure.

## Downstream consumers

### Yeast protein IDs are gene IDs

Ensembl's *Saccharomyces cerevisiae* annotation is imported from SGD and reuses the systematic name
as the gene, transcript and translation ID: gene `YOR125C`, transcript `YOR125C_mRNA`, translation
`YOR125C`. Every protein-coding row of `scerevisiae_gene_ensembl/BioMart.tsv` therefore has "Protein
stable ID" equal to "Gene stable ID" (11,117 rows in release 115). The gene pipeline claims the ID
(`gene.write_ensembl_gene_ids()`), so `protein.write_ensembl_protein_ids()` skips any protein stable
ID identical to its gene stable ID and logs the per-dataset count. Before that, ~6,600 `ENSEMBL:Y…`
CURIEs led a singleton clique in both `Gene.txt` and `Protein.txt`
([#276](https://github.com/NCATSTranslator/Babel/issues/276)). UniProt has no `Ensembl`
cross-reference for yeast entries (they are under EnsemblFungi), so the UniProtKB→Ensembl concord
never re-introduces them. A second species with the same convention would show up in that log line.

| Rule | Function | Output |
|------|----------|--------|
| `get_ensembl_gene_ids` (gene.snakefile) | `gene.write_ensembl_gene_ids()` | `ids/gene/ENSEMBL` |
| `get_ensembl_protein_ids` (protein.snakefile) | `protein.write_ensembl_protein_ids()` | `ids/protein/ENSEMBL` |

Both functions walk `babel_downloads/ENSEMBL/*/BioMart.tsv`, extract the relevant
identifier column, and write a standard Babel IDs file.

## Related code and issues

- `src/datahandlers/ensembl.py` — `pull_ensembl()`, retry/skip logic, BioMart attribute
  batching.
- `src/createcompendia/gene.py` — `write_ensembl_gene_ids()`.
- `src/createcompendia/protein.py` — `write_ensembl_protein_ids()`.
- `src/snakefiles/datacollect.snakefile` — `get_ensembl` rule.
- `config.yaml` → `ensembl_datasets_to_skip` — list of permanently broken datasets.
- [Babel#588](https://github.com/NCATSTranslator/Babel/pull/588) — SSL bypass workaround.

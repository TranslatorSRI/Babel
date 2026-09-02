# PubMed

PubMed is ingested by `src/createcompendia/publications.py`, which downloads the ~1,500 gzipped XML
files of the baseline and updatefiles corpora (~50 GB) and parses them into PMID identifiers,
titles, publication statuses, and `PMID`↔`DOI`/`PMC` concords.

## `generate_pubmed_concords` is the most expensive rule in Babel

From the `babel-1.18` Snakemake `benchmark:` TSVs:

| Rule | Wall | CPU | `mean_load` | `max_rss` |
|------|------|-----|-------------|-----------|
| `generate_pubmed_concords` | 71,947 s (20 h) | 71,753 s | 99.7% | 31 GB |

It is 3.6× the next most expensive rule and about 24% of the 83.7 h of summed rule wall time in the
whole pipeline, and it is almost perfectly CPU-bound on a single core — `cpu_time` is 99.7% of wall
time, so essentially none of that 20 hours is waiting on I/O.

## Parsing is what costs, and it is worth measuring

Two defects in `parse_pubmed_into_tsvs()` accounted for most of it:

- The parse loop drove an `ET.XMLPullParser` with `for line in pubmedf: parser.feed(line)`, i.e.
  one parser call per line of a ~30 MB decompressed file.
- Nothing released a parsed article. Every `PubmedArticle` stayed reachable from the tree root for
  the whole file, so resident memory grew with the file rather than staying flat.

Replacing both with `ET.iterparse` — which pulls the stream in blocks itself — plus a `root.clear()`
once each article has been written gives, over two real baseline files (`pubmed26n0001.xml.gz` and
`pubmed26n0002.xml.gz`, 60,000 articles):

```text
             origin/main:   16.2 s    1.50 GB peak RSS
            working tree:    8.4 s    0.09 GB peak RSS
                 speedup:   1.94x   16.14x less memory
                  output: identical in titles.tsv, PMID, PMID_DOI, statuses.jsonl.gz
```

Reproduce with [`bench_parse.py`](./bench_parse.py), which runs the working tree and a named git
revision in separate processes (so each peak RSS is its own) and fails if any output file differs:

```bash
mkdir -p data/scratch/pubmed && cd data/scratch/pubmed
curl -O https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed26n0001.xml.gz
curl -O https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed26n0002.xml.gz
cd -
PYTHONPATH=. uv run python docs/sources/PubMed/bench_parse.py data/scratch/pubmed origin/main
```

Two caveats on extrapolating the speedup to the recorded 71,947 s. The memory figure will not
scale the same way — peak RSS on the real rule is bounded by `pmid_status`, the accumulating
`defaultdict(set)` of every PMID's statuses, which this change does not touch; what it removes is
the per-file tree growth on top of that. And the time figure is measured on two files on a laptop,
not on 1,500 files on Hatteras, where the parallel filesystem contributes.

`root.clear()` alone is enough. An additional `elem.clear()` before it measured identically (0.09 GB
either way), because after the root drops its children the only remaining reference to the article
is the loop variable, which is rebound on the next iteration.

## Regression coverage

`tests/createcompendia/test_publications.py` parses
[`tests/data/pubmed_three_articles.xml.gz`](../../../tests/data/pubmed_three_articles.xml.gz) —
three articles copied verbatim from `pubmed26n0001.xml.gz`, chosen so one carries a DOI
(`PMID:1`), one a PMCID (`PMID:114`), and one neither (`PMID:10`). The tests assert every article
reaches every output, which is what catches clearing an article before it has been read; moving
`elem.clear()` above the write block fails both of them.

The end-to-end version, over freshly downloaded files, is
`tests/pipeline/test_publications.py` (`--pipeline --network`).

# Running Babel

## Configuration

The [`../kubernetes`](../kubernetes) directory contains Kubernetes manifest files
that can be used to set up a Pod to run Babel in. They'll give you an idea of the disk
space and memory requirements needed to run this pipeline.

Before running, read through `config.yaml` and make sure that the settings look correct.
You will need to update the version numbers of some databases that need to be downloaded,
or change the download and output directories. One download has no version number to bump but a
link that goes stale instead: `gard_download_url` pins one uploaded version of the GARD list, so
run `uv run pytest tests/datahandlers/test_gard.py::test_gard_download_url_is_current --network`,
which fails if the link has gone from <https://rarediseases.info.nih.gov/about> and warns if a newer
one is there (see [`sources/GARD/README.md`](sources/GARD/README.md), "Download").

A UMLS API key is required in order to download UMLS and RxNorm databases. You will need
to set the `UMLS_API_KEY` environmental variable to a UMLS API key, which you can obtain
by creating a profile on the [UMLS Terminology Services website](https://uts.nlm.nih.gov/uts).

## Building Compendia

To run Babel, you will need to
[install `uv`](https://docs.astral.sh/uv/getting-started/installation/). `uv` manages the Python
environment and installs dependencies for you.

Compendia building is managed by snakemake. To build, for example, the anatomy related compendia,
run

```uv run snakemake --cores 1 anatomy```

Currently, the following targets build compendia and synonym files:

* anatomy
* cell_line
* chemical
* disease
* gene
* genefamily
* leftover_umls
* protein
* macromolecular_complex
* taxon
* process
* publications

And these two build conflations:

* geneprotein
* drugchemical

Each target builds one or more compendia corresponding to a biolink model category. For instance,
the anatomy target builds compendia for `biolink:AnatomicalEntity`, `biolink:Cell`,
`biolink:CellularComponent`, and `biolink:GrossAnatomicalStructure`.

You can also just run:

```uv run snakemake --cores 1```

without a target to create all the files that are produced as part of Babel, including all reports
and alternate exports.

If you have multiple CPUs available, you can increase the number of `--cores` to run multiple steps
in parallel.

### Per-target sizing

Memory and time requirements vary widely by target. The README's 500 GB figure refers to the
largest builds (protein, drugchemical-conflated, and the full pipeline together), not to every
target.

**Only `gene`, `protein` and `chemical` need a workstation or HPC node**, along with the two
conflations and the full no-target pipeline. The conflations are expensive because of what they
depend on, not because conflation itself is costly: `geneprotein` takes `Gene.txt` and
`Protein.txt` as inputs, and `drugchemical` takes `Drug.txt` and the chemical outputs, so asking
for either transitively builds the heavy targets underneath it.

Every other target is small enough to build locally, and building one locally is the default way
to work on it: `anatomy`, `disease`, `process`, `taxon`, `genefamily`, `publications`,
`cell_line` and `macromolecular_complex`. (Note the Snakemake target is `disease`, while the
intermediate directory and Python module are `diseasephenotype`.)

Concretely, `anatomy` builds end-to-end on a Mac in roughly 25 minutes wall time, with the UMLS
download dominating the runtime; peak memory is in the low GBs. The other local targets are in
the same range. A first build is usually dominated by downloads rather than computation, so a
warm `babel_downloads/` makes a rebuild dramatically faster.

If you only need the intermediates for a single semantic type (for example, to generate a
source-impact report — see [AddingNewSources.md](./AddingNewSources.md)), building just that
target is much cheaper than `snakemake --cores N` with no target.

### Preloading PubMed downloads

PubMed is the largest download in the pipeline (~1,500 gzipped XML files across `baseline/` and
`updatefiles/`), and almost all of it is unchanged between runs. You can carry it forward into a
new run's download directory:

```shell
mkdir -p <new-run>/babel_downloads/PubMed
mv <old-run>/babel_downloads/PubMed/baseline    <new-run>/babel_downloads/PubMed/
mv <old-run>/babel_downloads/PubMed/updatefiles <new-run>/babel_downloads/PubMed/
rm -f <new-run>/babel_downloads/PubMed/baseline/*.md5 \
      <new-run>/babel_downloads/PubMed/updatefiles/*.md5
```

Two things make this work, and each is easy to break:

* **Do not copy the `downloaded` or `verified` marker files.** Those are what Snakemake tracks; if
  they are present it skips the download and verification rules entirely, and you never pick up the
  new updatefiles.
* **Preserve modification times.** `mv` (or `cp -p` / `rsync -a`) preserves them; a plain `cp` does
  not. `download_pubmed` runs `wget --timestamping`, which re-downloads a file only when the
  server's copy is newer than the local one or the sizes differ. With mtimes intact, a file PubMed
  has since revised is correctly re-fetched.

Deleting the `.md5` files, as above, isn't strictly necessary: PubMed republishes a file's `.md5`
whenever it revises the `.gz`, so with mtimes intact `--timestamping` re-fetches the checksum along
with the file, and a stale-but-self-consistent `.gz`/`.md5` pair can't survive the download. It is
cheap insurance against a preload that lost its mtimes — the `.md5` files are a few kilobytes each —
so drop them and let `verify_pubmed` check every carried-over `.gz` against a checksum fetched in
*this* run.

`verify_pubmed` then MD5s every `.gz` in both directories — the carried-over files included — and
re-downloads any that fail, so a corrupt or truncated file from the previous run heals itself.
Checksumming the full corpus takes a few minutes; it happens on every run regardless.

A recursive download is always timestamped and never resumed: `pull_via_wget()` raises if a caller
asks for `--continue` alongside recursion, or for recursion without `--timestamping`. wget resumes
by *appending* the bytes it thinks are missing to whatever local file it finds, which silently
corrupts a file whose content changed upstream rather than merely being truncated. Any file that
disagrees with the server on size or mtime is therefore re-fetched in full, at the cost of
restarting — rather than resuming — a large file whose download was interrupted.

The `baseline/` and `updatefiles/` directories are deliberately *not* declared as `directory()`
outputs of `download_pubmed`. Snakemake recursively deletes existing `directory()` outputs before
running a job, which would delete anything preloaded into them.

### Common build issues

* **Stale Snakemake lock.** If a previous run was killed (Ctrl-C, OOM, power loss) Snakemake
  may refuse to start with `LockException: Directory cannot be locked`. Clear it with
  `uv run snakemake --unlock` and retry. **Check first that no Snakemake process is actually
  still running** (`pgrep -fl snakemake`) — see the next bullet for why.
* **Never run two Snakemake invocations against the same working directory.** The directory
  lock is the only thing preventing this, so `--unlock` while a build is still alive lets a
  second run start on top of the first. When a rule fails, Snakemake keeps executing unrelated
  jobs rather than exiting immediately, so a build that printed `Error in rule ...` may still
  be running for many minutes afterwards. Two runs then execute the same rule concurrently and
  interleave their writes. The symptom is a compendium that fails JSON parsing partway through,
  with one record spliced into the middle of another:

  ```text
  {"type": "biolink:Ana{"type": "biolink:AnatomicalEntity", "ic": 77.6, ...
  ```

  and `check_*` rules failing with `line contains invalid json`. Only a small fraction of lines
  is usually affected (157 of 147,523 in one observed case), so eyeballing the head of the file
  will not catch it. The intermediate `ids/` and `concords/` files usually survive (each is
  written by a single rule); it is the compendia that get corrupted. Recovery: kill every
  Snakemake process, delete the affected compendia, synonyms, reports and metadata for that
  pipeline, then rerun the target once. Validate before trusting a rebuild — every line of a
  compendium must be parseable JSON. `write_compendium()` does not write atomically, which is
  what makes this possible; tracked in
  [#910](https://github.com/NCATSTranslator/Babel/issues/910).
* **Put the target *before* `--forcerun`.** `--forcerun` takes a list of rule names, so a target
  written after it is swallowed as another rule name and Snakemake falls back to building the
  **default target** — the entire pipeline. The tell is a job list far larger than expected, then a
  failure in a rule you were not building (an unrelated download, typically). `--rerun-incomplete`
  makes it worse: it pulls every incomplete file from *any* previous run into that DAG.
  `snakemake -c 4 babel_outputs/compendia/Disease.txt --forcerun get_disease_doid_relationships`
  is right; the same words with the path last is not.
* **UberGraph transient failures.** Rules that fetch from UberGraph (anatomy's UBERON, GO, CL,
  EMAPA rules; similar elsewhere) sometimes time out, 5xx, or return truncated JSON. They carry
  `retries: 3` and the underlying `TripleStore` adds bounded retry/backoff, so most transient
  blips heal themselves. A full UberGraph outage will still propagate as a job failure — wait
  and rerun. A `JSONDecodeError` reporting a character offset in the hundreds of millions is a
  truncated response to an oversized query, not malformed data; see "Oversized UberGraph
  queries" below. Running two heavy UberGraph rules in parallel (`-c all` will happily schedule
  `get_icrdf` alongside `get_anatomy_obo_relationships`) makes it markedly more likely.
* **UMLS_API_KEY not set.** The UMLS download rule fails fast with a clear error if this is
  missing. Set it in your shell before invoking Snakemake, not just inline (`UMLS_API_KEY=… uv
  run snakemake …` works only for the parent process and may not propagate into all subjobs
  depending on scheduler).
* **Partial/incomplete state from a prior aborted run.** Add `--rerun-incomplete` to force
  Snakemake to regenerate any outputs it considers possibly-stale, which is the safest
  default after a kill.
* **A stale target sentinel silently skips a rebuild.** Deleting intermediates to force a
  rebuild is *not* enough. Each pipeline target's own output is a sentinel file —
  `babel_outputs/reports/<pipeline>_done` — and Snakemake only asks whether the requested
  target is up to date. With the sentinel present it prints

  ```text
  Nothing to be done (all requested files are present and up to date).
  ```

  and exits 0, having rebuilt nothing, even with `--rerun-incomplete` and even though the
  concords and compendia you deleted are missing. The exit code and the log both look like
  success, so a code fix can appear to have "no effect" when it was never actually run. Delete
  the sentinel along with the files you want regenerated, then confirm with `-n` that the rules
  you expect are actually scheduled before starting the real run.

### Oversized UberGraph queries

`UberGraph.get_subclasses_and_xrefs()` (and its `_exacts`/`_close` siblings) fetch every
descendant of a root **across all ontologies loaded into UberGraph**, because the redundant
graph's `rdfs:subClassOf` closure crosses ontology boundaries. `build_sets()` then throws away
every row whose subject prefix differs from the root's, client-side.

For `UBERON:0001062` "anatomical entity" that means downloading **2,885,566 rows (~396 MB of
SPARQL JSON) and keeping 48,592** — under 2%. The full response regularly gets truncated
mid-stream, surfacing as:

```text
JSONDecodeError: Expecting property name enclosed in double quotes: line 14182994 column 7 (char 396247040)
```

`retries: 3` plus `TripleStore`'s own backoff usually gets it through eventually, so this reads
as a flaky endpoint rather than the design problem it is. Tracked in
[#909](https://github.com/NCATSTranslator/Babel/issues/909).

The fix is to push the prefix filter into the SPARQL query (a `FILTER(STRSTARTS(STR(?descendent),
"<root prefix IRI>"))` when `hop_ontologies` is false) rather than to batch the download.
Batching would page 396 MB in chunks; filtering avoids transferring 98% of it in the first
place. This belongs in `UberGraph`, not in a single caller — `build_sets()` applies the same
client-side prefix filter for every source, so every caller of the `get_subclasses_*` family
pays this cost today. Note that `UberGraph` already has a batching idiom (`QUERY_BATCH_SIZE`
with a `COUNT` then `OFFSET`/`LIMIT`, used by `get_all_labels`, `get_all_descriptions`,
`get_all_synonyms` and `write_normalized_information_content`); reach for it only if a query is
still too large *after* filtering.

## Analyzing and tuning a SLURM run

When running on the RENCI Hatteras cluster via SLURM, the `src/tools/slurm` package analyzes a
(possibly partial) run: `uv run babel-slurm-errors <version>` aggregates failing-rule logs when a
run stalls so you can see what to re-run, and `uv run babel-slurm-resources <run-dir>` recommends
right-sized per-rule `mem`/`cpus` from the run's benchmark data. See
[tools/README.md](tools/README.md) for the full set of developer tools and
[slurm/README.md](../slurm/README.md) for the SLURM profile itself.

## Build Process

The information contained here is not required to create the compendia, but may be useful to
understand. The build process is divided into two parts:

1. Pulling data from external sources and parsing it independent of use.
2. Extracting and combining entities for specific types from these downloaded data sets.

This distinction is made because a single data set, such as MeSH or UMLS may contain entities of
many different types and may be used by many downstream targets.

### Pulling Data

The datacollection snakemake file coordinates pulling data from external sources into a local
filesystem. Each data source has a module in `src/datahandlers`. Data goes into the
`babel_downloads` directory, in subdirectories named by the curie prefix for that data set. If the
directory is misnamed and does not match the prefix, then labels will not be added to the
identifiers in the final compendium.

Once data is assembled, we attempt to create two extra files for each data source: `labels` and
`synonyms`. `labels` is a two-column tab-delimited file. The first column is a CURIE identifier from
the data source, and the second column is the label from that data set. Each entity should only
appear once in the `labels` file. The `labels` file for a data set does not subset the data for a
specific purpose, but contains all labels for any entity in that data set.

`synonyms` contains other lexical names for the entity and is a 3-column tab-delimited file, with
the second column indicating the type of synonym (exact, related, xref, etc.)

### Creating compendia

The individual details of creating a compendium vary, but all follow the same essential pattern.

First, we extract the identifiers that will be used in the compendia from each data source that will
contribute, and place them into a directory. For instance, in the build of the chemical compendium,
these ids are placed into `babel_outputs/intermediate/chemicals/ids`. Each file is a two-column file
containing curie identifiers in column 1, and the Biolink type for that entity in column 2.

Second, we create pairwise concords across vocabularies. These are placed in e.g.
`babel_outputs/intermediate/chemicals/concords`. Each concord is a three-column file of the format:

`<curie1> <relation> <curie2>`

While the relation is currently unused, future versions of Babel may use the relation in building
cliques.

Third, the compendia is built by bringing together the ids and concords, pulling in the categories
from the id files, and the labels from the label files.

Fourth, the compendia is assessed to make sure that all the ids in the id files made into one of the
possibly multiple compendia. The compendia are further assessed to locate large cliques and display
the level of vocabulary merging.

## Running on an HPC Cluster (SLURM)

The production Babel runs are executed on Hatteras, an HPC cluster managed by RENCI, using a SLURM
profile in [`slurm/`](../slurm/). See [`slurm/README.md`](../slurm/README.md) for:

* How to submit the pipeline with `sbatch` and the SLURM profile.
* Per-rule memory and runtime allocations, including which rules need a largemem node.
* DuckDB memory tuning: `memory_limit` caps, single-threaded query settings, per-job spill
  subdirectories, and `write_buffer_row_group_count`.
* Known issues and their mitigations — notably the `vm.max_map_count` mmap-count limit that causes
  `bad allocation` failures with plenty of free RAM, what was investigated (`MALLOC_ARENA_MAX=2`,
  disabling the external file cache) and ruled out, and the `memory_limit` cap that keeps the rules
  running until the cluster raises the kernel limit.

## Building with Docker

You can build this repository by running the following Docker command:

```text
$ docker build .
```

It is also set up with a GitHub Action that will automatically generate and publish
Docker images to <https://github.com/NCATSTranslator/Babel/pkgs/container/babel>.

## Running with Docker

You can also run Babel with [Docker](https://www.docker.com/). There are
two directories you need to bind or mount from outside the container:

```text
$ docker run -it --rm --mount type=bind,source=...,target=/home/runner/babel/babel_downloads --entrypoint /bin/bash ggvaidya/babel
```

The download directory (`babel/babel_downloads`) is used to store data files downloaded during Babel
assembly.

The script `scripts/babel-build.sh` can be used to run `snakemake` with a few useful settings
(although just running `uv run snakemake --cores 5` should work just fine.)

## Running with Kubernetes

The `kubernetes/` directory has example Kubernetes scripts for deploying Babel to a Kubernetes
cluster. You need to create three resources:

* `kubernetes/babel-downloads.k8s.yaml` creates a Persistent Volume Claim (PVC) for downloading
  input resources from the internet.
* `kubernetes/babel-outputs.k8s.yaml` creates a PVC for storing the output files generated by Babel.
  This includes compendia, synonym files, reports and intermediate files.
* `kubernetes/babel.k8s.yaml` creates a pod running the latest Docker image from ggvaidya/babel.
  Rather than running the data generation automatically, you are expected to SSH into this pod and
  start the build process by:
    1. Edit the script `scripts/babel-build.sh` to clear the `DRY_RUN` property so that it doesn't,
       i.e.:

       ```shell
       export DRY_RUN=
       ```

    2. Creating a [screen](https://www.gnu.org/software/screen/) to run the program in. You can
       start a Screen by running:

       ```shell
       $ screen
       ```

    3. Starting the Babel build process by running:

       ```shell
       $ bash scripts/babel-build.sh
       ```

       Ideally, this should produce the entire Babel output in a single run. You can also add
       `--rerun-incomplete` if you need to restart a partially completed job.

       To help with debugging, the Babel image includes .git information. You can switch branches,
       or fetch new branches from GitHub by running `git fetch origin-https`.

    4. Press `Ctrl+A D` to "detach" the screen. You can reconnect to a detached screen by running
       `screen -r`. You can also see a list of all running screens by running `screen -l`.

    5. Once the generation completes, all output files should be in the `babel_outputs` directory.

## Releasing a new Babel version

A full production run happens on an HPC system over many hours, and it almost always
surfaces problems that aren't visible from a local dry run: wrong memory settings,
download endpoints that have moved or require an API key, format changes upstream, and
latent bugs that only fire at full scale. The practical way to keep a run moving is to
fix these directly on the release branch (for example `babel-1.17`) rather than stopping
to open a separate PR for each one. By the time the run is healthy, the release branch
holds a long, date-interleaved mix of trivial tweaks and substantial changes.

Before that branch is merged, it is worth separating the two kinds of change.
The scripts in [`../scripts/commit-split`](../scripts/commit-split) help verify the
split is complete and lossless; see that directory's `README.md`.

### Which commits stay on the release branch

A commit can stay on the release branch if its entire effect fits in a single
release-note line, for example "updated the ENSEMBL dataset skip list", "bumped the
Biolink Model version", or "raised a rule's memory limit". These are the expected
running-a-build adjustments and reviewing them inline with the release is fine.

Everything else should move to its own branch off `main` and be reviewed as a normal
PR, so the change is documented, gets a real review, and earns its own release-note
entry. Related commits move together as one PR even if they were made days apart: all
the download-robustness work is one PR, all the DuckDB memory tuning is another, and so
on. Documentation and formatting commits travel with the code they describe rather than
staying behind on the release branch.

### Splitting the branch

1. **Classify by theme, not by date.** The commits interleave chronologically but group
   cleanly by the files they touch. List `git rev-list --no-merges main..<release-branch>`
   with each commit's changed files and bucket them (download robustness, a specific
   source's ingest, export/reporting, tooling, and so on).
2. **Make a backup branch** (`git branch backups/<release-branch> <release-branch>`) before
   anything that will later rewrite the release branch.
3. **Build one branch per theme off `main`** with `git cherry-pick`, replaying each
   bucket's commits in chronological order. Enable `git rerere` so a conflict you resolve
   once (typically in shared files like `config.yaml`, `datacollect.snakefile`, or
   `AGENTS.md`) is replayed automatically if you have to rebuild the branch.
4. **Watch for entangled and coupled commits.** Two themes that edit the same file in
   alternating commits may need one branch *stacked on* the other (cherry-pick the second
   theme on top of the first) rather than both off `main` — that reconstructs the original
   context and avoids fighting conflicts. Also watch for a "workaround then fix" pair
   split across buckets: for example a commit that raises a memory limit as a stopgap and
   a later commit that removes the stopgap after fixing the root cause must live in the
   same PR, or the net effect on the release branch changes.
5. **Verify each branch independently** with the full CI gate — `uv run ruff check`,
   `uv run ruff format --check`, `uv run snakefmt --check --compact-diff .`,
   `uv run rumdl check .`, and `uv run pytest -m unit` — plus the cluster's own tests. A
   branch that carries a behavior change but no test gets a small regression test added.
6. **Prove nothing was lost.** Compare `git patch-id --stable` for every commit in
   `main..<release-branch>` against the patch-ids present across all theme branches: every
   moved commit should appear in exactly one branch, and every stay-behind commit should
   appear in none. Commits you deliberately adapted while resolving a conflict will differ;
   confirm those by diffing the applied change against the original so only context, not
   added or removed lines, has changed.
7. **Reintegrate.** Once the theme PRs are merged into `main`, merge or rebase `main` into
   the release branch. The commits that were split out arrive via `main` and drop out of
   the release branch's own diff, leaving only the stay-behind commits there.

### Comparing a build against the previous release

Every full run produces a combined prefix report at
`babel_outputs/reports/duckdb/prefix_report.json` (via the `generate_prefix_report` rule) that
counts the CURIEs and cliques per prefix, per clique-leader prefix, and per compendium file. At the
end of the reports phase, the `generate_prefix_comparison` rule diffs this build against the
previous release and writes, into `babel_outputs/reports/tables/`:

* `prefix_comparison_overall.csv` — All CURIEs, All cliques, and per-filename CURIE totals, each
  with the absolute and percentage change.
* `prefix_comparison_by_clique_prefix.csv` — one row per (filename, clique-leader prefix, CURIE
  prefix), sorted by absolute change (largest first, ignoring sign), so the biggest swings surface
  at the top. For example, a row might read "8,443,204 fewer INCHIKEY identifiers in SmallMolecule
  cliques led by PUBCHEM.COMPOUND".
* `prefix_comparison.md` — a human summary that names exactly which baseline was compared against
  and lists the notable changes (anything removed, or beyond the `prefix_comparison_warn_abs` /
  `prefix_comparison_warn_pct` thresholds in `config.yaml`).

CURIE occurrence counts are exact; clique counts are approximate (HyperLogLog, ~2% error). This
report supersedes the manual comparison previously done in babel-validation's
`PrefixComparator.vue`.

Which release it compares against is pinned explicitly by `previous_release` in `config.yaml` —
there is no date guessing. A unit test (`tests/reports/test_prefix_comparison.py`) fails if a newer
baseline was committed under `releases/` without bumping the pin, so a stale pin is caught by the
weekly unit run before the next build starts.

### Archiving a build's reports

After a healthy release run, archive the build's summary reports into the repository. This is both
how the tables stay readable after the build directory is gone and how the *next* release gets a
baseline to compare against:

1. Run the archiver. It copies ~420 KB — the summary tables, the per-compendium content reports, the
   provenance metadata, and the prefix report — into `releases/<build>/`, mirroring the build
   directory's own paths. [`releases/ARTIFACTS.md`](../releases/ARTIFACTS.md) describes each file
   and what is deliberately left out.

   ```bash
   uv run python releases/scripts/archive_build.py <build> --build-dir <build directory> --dry-run
   uv run python releases/scripts/archive_build.py <build> --build-dir <build directory>
   ```

   It fails rather than archive a prefix report whose `name` field is not `<build>`. That field is
   written from `release_name` at build time, so a run that started before the pin was updated
   stamps the *previous* release's name into it — and it is what labels the baseline in the next
   release's comparison, so a wrong value propagates forward. The 2026jul22 build shipped with
   `"name": "2026jul15"` for exactly this reason, which is why the check is no longer left to a
   human. If it fires, work out which release the value names before correcting it.
2. Link the archive from the release's `releases/<build>/README.md` note — `draft_release_notes.py`
   emits those three links, now relative, since the note sits beside the archive.
3. Commit. This copy becomes the reviewed baseline for the next release. Leave `config.yaml` alone
   here: `release_name` still names the build you just archived, and `previous_release` still names
   the baseline it was compared against.
4. When the *next* build is planned, move both pins in one commit: `previous_release` to the release
   just archived, `release_name` to the new build. They always move together, and they must never be
   equal — a run whose `release_name` matches its `previous_release` diffs its own baseline and
   reports that nothing changed. `generate_prefix_comparison()` raises rather than write that
   report, and a unit test catches the drift before a build ever starts.

Steps 1–3 are part of the wider release-note process in
[`releases/README.md`](../releases/README.md), which also drafts the note itself.

import os

import src.createcompendia.publications as publications
import src.assess_compendia as assessments
from src.snakefiles import util


# Trivial done-marker rule runs locally so it doesn't consume a SLURM slot.
localrules:
    publications,


### PubMed, via the pubmed2db NDJSON export pinned in config.yaml (see docs/sources/PubMed/README.md).

# Each export version downloads into its own directory, so moving config.yaml: pubmed2db_version to a newer
# export changes every path below and Snakemake re-runs all three rules; nothing from the old export can be
# reused by mistake. The URL is also a param, so editing pubmed2db_base_url re-runs them too.
PUBMED2DB_URL = config["pubmed2db_base_url"] + config["pubmed2db_version"] + "/"
PUBMED2DB_DIR = config["download_directory"] + "/PubMed2DB/" + config["pubmed2db_version"]


# PubMed2DB/<version>/ is deliberately NOT declared as a directory() output: Snakemake recursively deletes
# existing directory() outputs before running a job, which would wipe shards preloaded from a previous run (see
# docs/RunningBabel.md, "Preloading PubMed downloads"). The done marker is what the downstream rules depend on.
rule download_pubmed2db:
    output:
        done_file=PUBMED2DB_DIR + "/downloaded",
    benchmark:
        config["output_directory"] + "/benchmarks/download_pubmed2db.tsv"
    retries: 3
    resources:
        mem="2G",
        cpus_per_task=1,
        # ~17 GB over 8 files from stars.renci.org; 2h is generous for an in-datacenter copy.
        runtime="2h",
    params:
        url=PUBMED2DB_URL,
        report_path=config["pubmed2db_validation_report"],
        download_dir=PUBMED2DB_DIR,
    run:
        publications.download_pubmed2db(params.url, params.report_path, params.download_dir, output.done_file)


rule generate_pubmed_concords:
    input:
        PUBMED2DB_DIR + "/downloaded",
    output:
        titles_file=config["download_directory"] + "/PubMed2DB/titles.tsv",
        pmid_id_file=config["intermediate_directory"] + "/publications/ids/PMID",
        pmid_doi_concord_file=config["intermediate_directory"] + "/publications/concords/PMID_DOI",
        shared_ids_file=config["intermediate_directory"] + "/publications/concords/shared_identifiers.tsv",
        metadata_yaml=config["intermediate_directory"] + "/publications/concords/metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_pubmed_concords.tsv"
    # One worker per shard; 2026sep26 has 8. More threads than shards buy nothing.
    threads: 8
    resources:
        # The SLURM executor allocates cpus_per_task, not threads, and slurm/config.yaml defaults it to 1:
        # without this, all 8 workers would share one core. Keep it equal to threads.
        cpus_per_task=8,
        # Workers write their shard's rows straight to part files, so each holds one record at a time;
        # the parent's peak is the set of every DOI/PMCID built while scanning the concords for shared
        # identifiers. Measured locally on shard 0 of 2026sep26 (5.25M records, 5.7M identifiers): 31s and
        # 1.27 GB, ~210 bytes per identifier, so ~10 GB and a few minutes for all 8 shards. Replaced the
        # single-threaded XML parse (128G/24h requested; 32 GB peak and 20h on 2026jul22). Confirm against
        # the first cluster benchmark.
        mem="16G",
        runtime="1h",
    params:
        url=PUBMED2DB_URL,
        # Not an input: see the comment on download_pubmed2db for why this directory is untracked.
        download_dir=PUBMED2DB_DIR,
    run:
        publications.parse_pubmed2db_into_tsvs(
            params.download_dir,
            output.titles_file,
            output.pmid_id_file,
            output.pmid_doi_concord_file,
            output.shared_ids_file,
            output.metadata_yaml,
            params.url,
            threads,
        )


rule generate_pubmed_compendia:
    input:
        PUBMED2DB_DIR + "/downloaded",
        shared_ids_file=config["intermediate_directory"] + "/publications/concords/shared_identifiers.tsv",
        metadata_yaml=config["intermediate_directory"] + "/publications/concords/metadata.yaml",
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    output:
        publication_compendium=config["output_directory"] + "/compendia/Publication.txt",
        # We generate an empty Publication Synonyms files, but we still need to generate one.
        publication_synonyms_gz=config["output_directory"] + "/synonyms/Publication.txt.gz",
        publication_metadata_yaml=config["output_directory"] + "/metadata/Publication.txt.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_pubmed_compendia.tsv"
    resources:
        # Single-threaded: cliques stream from the shards one record at a time (no glom, no global labels
        # dict), so memory doesn't grow with the corpus. Measured locally on shard 0 of 2026sep26: 102s at
        # ~51k cliques/s and 0.10 GB without the common UberGraph files, which other small compendium rules
        # show add ~3 GB (genefamily_compendia: 2.9 GB on 2026jul22). So ~3 GB and ~15 min for the full
        # export. The glom version peaked at 126 GiB and 1.8h. Confirm against the first cluster benchmark.
        mem="8G",
        runtime="1h",
    params:
        download_dir=PUBMED2DB_DIR,
    run:
        publications.generate_compendium(
            params.download_dir,
            input.shared_ids_file,
            [input.metadata_yaml],
            output.publication_compendium,
            input.icrdf_filename,
        )
        # generate_compendium() will generate an (empty) Publication.txt file, but we need
        # to compress it.
        publication_synonyms = os.path.splitext(output.publication_synonyms_gz)[0]
        util.gzip_files([publication_synonyms])
        os.remove(publication_synonyms)


rule check_publications_completeness:
    input:
        input_compendia=expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["publication_outputs"]),
    output:
        report_file=config["output_directory"] + "/reports/publication_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_publications_completeness.tsv"
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/publications/ids", input.input_compendia, output.report_file
        )


rule check_publications:
    input:
        infile=config["output_directory"] + "/compendia/Publication.txt",
    output:
        outfile=config["output_directory"] + "/reports/Publication.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_publications.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule publications:
    input:
        config["output_directory"] + "/reports/publication_completeness.txt",
        synonyms=expand("{od}/synonyms/{ap}.gz", od=config["output_directory"], ap=config["publication_outputs"]),
        reports=expand("{od}/reports/{ap}", od=config["output_directory"], ap=config["publication_outputs"]),
    output:
        x=config["output_directory"] + "/reports/publications_done",
    shell:
        "echo 'done' >> {output.x}"

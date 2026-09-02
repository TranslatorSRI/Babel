import os

import src.createcompendia.publications as publications
import src.assess_compendia as assessments
from src.snakefiles import util


# Trivial done-marker rule runs locally so it doesn't consume a SLURM slot.
localrules:
    publications,


### PubMed, via the pubmed2db NDJSON export pinned in config.yaml (see docs/sources/PubMed/README.md).


# PubMed2DB/ is deliberately NOT declared as a directory() output: Snakemake recursively deletes existing
# directory() outputs before running a job, which would wipe shards preloaded from a previous run. Keeping it
# undeclared lets wget --timestamping skip files we already have (see docs/RunningBabel.md, "Preloading PubMed
# downloads"). The done marker is what the downstream rules depend on.
rule download_pubmed2db:
    output:
        done_file=config["download_directory"] + "/PubMed2DB/downloaded",
    benchmark:
        config["output_directory"] + "/benchmarks/download_pubmed2db.tsv"
    retries: 3
    resources:
        mem="2G",
        cpus_per_task=1,
        # ~17 GB over 16 files from stars.renci.org; 2h is generous for an in-datacenter copy.
        runtime="2h",
    params:
        download_dir=config["download_directory"] + "/PubMed2DB",
    run:
        publications.download_pubmed2db(config["pubmed2db_url"], params.download_dir, output.done_file)


rule generate_pubmed_concords:
    input:
        config["download_directory"] + "/PubMed2DB/downloaded",
    output:
        titles_file=config["download_directory"] + "/PubMed2DB/titles.tsv",
        pmid_id_file=config["intermediate_directory"] + "/publications/ids/PMID",
        pmid_doi_concord_file=config["intermediate_directory"] + "/publications/concords/PMID_DOI",
        shared_ids_file=config["intermediate_directory"] + "/publications/concords/shared_identifiers.tsv",
        metadata_yaml=config["intermediate_directory"] + "/publications/concords/metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_pubmed_concords.tsv"
    threads: 16
    resources:
        # Measured on one of 16 shards (2.56M records): 29s and 2.6 GB with 2 workers. The parent holds
        # a set of every DOI/PMCID (~46M strings, ~6 GB) plus up to `threads` parsed shards in flight
        # (~1 GB each), so 32G has headroom. Replaced the 20h/32G single-threaded XML parse.
        mem="32G",
        runtime="1h",
    params:
        # Not an input: see the comment on download_pubmed2db for why this directory is untracked.
        download_dir=config["download_directory"] + "/PubMed2DB",
    run:
        publications.parse_pubmed2db_into_tsvs(
            params.download_dir,
            output.titles_file,
            output.pmid_id_file,
            output.pmid_doi_concord_file,
            output.shared_ids_file,
            output.metadata_yaml,
            config["pubmed2db_url"],
            threads,
        )


rule generate_pubmed_compendia:
    input:
        config["download_directory"] + "/PubMed2DB/downloaded",
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
    threads: 8
    resources:
        # Measured on one of 16 shards: 108s at ~22k cliques/s and 5 GB peak with 2 workers, so ~30 min
        # and well under 32G for the full corpus. Cliques stream straight from the shards (no glom, no
        # global labels dict); the glom version peaked at 126 GiB and 1.8h.
        mem="32G",
        runtime="2h",
    params:
        download_dir=config["download_directory"] + "/PubMed2DB",
    run:
        publications.generate_compendium(
            params.download_dir,
            input.shared_ids_file,
            [input.metadata_yaml],
            output.publication_compendium,
            input.icrdf_filename,
            threads,
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

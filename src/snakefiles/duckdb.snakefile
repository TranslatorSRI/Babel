import src.reports.duckdb_reports
from src.snakefiles.util import get_all_compendia, get_all_synonyms_with_drugchemicalconflated, get_all_conflations
import src.exporters.duckdb_exporters as duckdb_exporters
import os

### Write all compendia, synonym and conflation files into DuckDB databases.

# Write all compendia files to DuckDB and Parquet, then create `babel_outputs/duckdb/compendia_done` to signal that we're done.
rule export_all_compendia_to_duckdb:
    input:
        compendium_duckdb_files=expand("{od}/duckdb/duckdbs/filename={fn}/compendium.duckdb",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0], get_all_compendia(config))
        )
    output:
        x = config['output_directory'] + '/duckdb/compendia_done',
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the Parquet files for a particular compendia file.
rule export_compendia_to_duckdb:
    input:
        compendium_file=config['output_directory'] + "/compendia/{filename}.txt",
    output:
        duckdb_filename=config['output_directory'] + "/duckdb/duckdbs/filename={filename}/compendium.duckdb",
        clique_parquet_file=config['output_directory'] + "/duckdb/parquet/filename={filename}/Clique.parquet",
    run:
        duckdb_exporters.export_compendia_to_parquet(input.compendium_file, output.clique_parquet_file, output.duckdb_filename)


# Write all synonyms files to Parquet via DuckDB, then create `babel_outputs/duckdb/synonyms_done` to signal that we're done.
rule export_all_synonyms_to_duckdb:
    input:
        synonyms_duckdb_files=expand("{od}/duckdb/duckdbs/filename={fn}/synonyms.duckdb",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0], get_all_synonyms_with_drugchemicalconflated(config))
        )
    output:
        x = config['output_directory'] + '/duckdb/synonyms_done',
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the Parquet files for a particular compendia file.
rule export_synonyms_to_duckdb:
    input:
        synonyms_file=config['output_directory'] + "/synonyms/{filename}.txt.gz",
    output:
        duckdb_filename=config['output_directory'] + "/duckdb/duckdbs/filename={filename}/synonyms.duckdb",
        synonyms_parquet_filename=config['output_directory'] + "/duckdb/parquet/filename={filename}/Synonyms.parquet",
    run:
        duckdb_exporters.export_synonyms_to_parquet(input.synonyms_file, output.duckdb_filename, output.synonyms_parquet_filename)


# Write all conflation files to DuckDB and Parquet, then create `babel_outputs/duckdb/conflation_done` to signal that we're done.
rule export_all_conflation_to_duckdb:
    input:
        conflation_duckdb_files=expand("{od}/duckdb/duckdbs/filename={fn}/conflations.duckdb",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0], get_all_conflations(config))
        )
    output:
        x = config['output_directory'] + '/duckdb/conflations_done',
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the Parquet files for a particular compendia file.
rule export_conflation_to_duckdb:
    input:
        conflation_file=config['output_directory'] + "/conflation/{filename}.txt",
    output:
        duckdb_filename=config['output_directory'] + "/duckdb/duckdbs/filename={filename}/conflations.duckdb",
        conflation_parquet_file=config['output_directory'] + "/duckdb/parquet/filename={filename}/Conflation.parquet",
    run:
        duckdb_exporters.export_conflation_to_parquet(input.conflation_file, output.duckdb_filename, output.conflation_parquet_file)


# Create `babel_outputs/duckdb/done` once all the files have been converted.
rule export_all_to_duckdb:
    input:
        compendia_done=config['output_directory'] + '/duckdb/compendia_done',
        synonyms_done=config['output_directory'] + '/duckdb/synonyms_done',
        conflations_done=config['output_directory'] + '/duckdb/conflations_done',
    output:
        x = config['output_directory'] + '/duckdb/done',
    shell:
        "echo 'done' >> {output.x}"


# There are some reports we want to run on the Parquet files that have been generated.
rule check_for_identically_labeled_cliques:
    input:
        config['output_directory'] + '/duckdb/done',
    params:
        parquet_dir = config['output_directory'] + '/duckdb/parquet/',
    output:
        duckdb_filename = temp(config['output_directory'] + '/duckdb/duckdbs/identically_labeled_clique.duckdb'),
        identically_labeled_cliques_tsv = config['output_directory'] + '/reports/duckdb/identically_labeled_cliques.tsv',
    run:
        src.reports.duckdb_reports.check_for_identically_labeled_cliques(params.parquet_dir, output.duckdb_filename, output.identically_labeled_cliques_tsv)


rule check_for_duplicate_curies:
    input:
        config['output_directory'] + '/duckdb/done',
        config['output_directory'] + '/duckdb/compendia_done',
    params:
        parquet_dir = config['output_directory'] + '/duckdb/parquet/',
    output:
        duckdb_filename = temp(config['output_directory'] + '/duckdb/duckdbs/duplicate_curies.duckdb'),
        duplicate_curies = config['output_directory'] + '/reports/duckdb/duplicate_curies.tsv',
    run:
        src.reports.duckdb_reports.check_for_duplicate_curies(params.parquet_dir, output.duckdb_filename, output.duplicate_curies)

rule check_for_duplicate_clique_leaders:
    input:
        config['output_directory'] + '/duckdb/done',
        config['output_directory'] + '/duckdb/compendia_done',
    params:
        parquet_dir = config['output_directory'] + '/duckdb/parquet/',
    output:
        duckdb_filename = temp(config['output_directory'] + '/duckdb/duckdbs/duplicate_clique_leaders.duckdb'),
        duplicate_clique_leaders_tsv = config['output_directory'] + '/reports/duckdb/duplicate_clique_leaders.tsv',
    run:
        src.reports.duckdb_reports.check_for_duplicate_clique_leaders(params.parquet_dir, output.duckdb_filename, output.duplicate_clique_leaders_tsv)

rule generate_prefix_report:
    input:
        config['output_directory'] + '/duckdb/done',
        config['output_directory'] + '/duckdb/compendia_done',
    params:
        parquet_dir = config['output_directory'] + '/duckdb/parquet/',
    output:
        duckdb_filename = temp(config['output_directory'] + '/duckdb/duckdbs/prefix_report.duckdb'),
        prefix_report_json = config['output_directory'] + '/reports/duckdb/prefix_report.json',
        prefix_report_tsv = config['output_directory'] + '/reports/duckdb/prefix_report.tsv',
    run:
        src.reports.duckdb_reports.generate_prefix_report(params.parquet_dir, output.duckdb_filename,
            output.prefix_report_json,
            output.prefix_report_tsv)

rule all_duckdb_reports:
    input:
        config['output_directory'] + '/duckdb/done',
        identically_labeled_cliques_tsv = config['output_directory'] + '/reports/duckdb/identically_labeled_cliques.tsv',
        duplicate_curies = config['output_directory'] + '/reports/duckdb/duplicate_curies.tsv',
        duplicate_clique_leaders_tsv = config['output_directory'] + '/reports/duckdb/duplicate_clique_leaders.tsv',
        prefix_report = config['output_directory'] + '/reports/duckdb/prefix_report.json',
    output:
        x = config['output_directory'] + '/reports/duckdb/done',
    shell:
        "echo 'done' >> {output.x}"

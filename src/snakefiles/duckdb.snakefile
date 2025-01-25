from src.snakefiles.util import get_all_compendia, get_all_synonyms_with_drugchemicalconflated
import src.exporters.duckdb_exporters as duckdb_exporters
import os

### Write all compendia, synonym and conflation files into DuckDB databases.

# Write all compendia files to DuckDB and Parquet, then create `babel_outputs/duckdb/compendia_done` to signal that we're done.
rule export_all_compendia_to_duckdb:
    input:
        nodes_files=expand("{od}/duckdb/duckdbs/filename={fn}/compendium.duckdb",
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
        sapbert_training_file=expand("{od}/duckdb/duckdbs/filename={fn}/synonyms.duckdb",
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
        synonyms_file=config['output_directory'] + "/synonyms/{filename}.txt",
    output:
        duckdb_filename=config['output_directory'] + "/duckdb/duckdbs/filename={filename}/synonyms.duckdb",
        synonyms_parquet_filename=config['output_directory'] + "/duckdb/parquet/filename={filename}/Synonyms.parquet",
    run:
        duckdb_exporters.export_synonyms_to_parquet(input.synonyms_file, output.duckdb_filename, output.synonyms_parquet_filename)


# TODO: convert all conflations to Parquet via DuckDB.

# Create `babel_outputs/duckdb/done` once all the files have been converted.
rule export_all_to_duckdb:
    input:
        compendia_done=config['output_directory'] + '/duckdb/compendia_done',
        synonyms_done=config['output_directory'] + '/duckdb/synonyms_done'
    output:
        x = config['output_directory'] + '/duckdb/done',
    shell:
        "echo 'done' >> {output.x}"


# There are some reports we want to run on the Parquet files that have been generated.
rule check_for_identically_labeled_cliques:
    input:
        config['output_directory'] + '/duckdb/done',
        parquet_dir = config['output_directory'] + '/duckdb/parquet/',
    output:
        duckdb_filename = config['output_directory'] + '/duckdb/identically_labeled_clique.duckdb',
        identically_labeled_cliques_tsv = config['output_directory'] + '/reports/duckdb/identically_labeled_cliques.tsv',
    run:
        duckdb_exporters.check_for_identically_labeled_cliques(input.parquet_dir, output.duckdb_filename, output.identically_labeled_cliques_tsv)


rule check_for_duplicate_curies:
    input:
        config['output_directory'] + '/duckdb/done',
        parquet_dir = config['output_directory'] + '/duckdb/parquet/',
    output:
        duckdb_filename = config['output_directory'] + '/duckdb/duplicate_curies.duckdb',
        duplicate_curies = config['output_directory'] + '/reports/duckdb/duplicate_curies.tsv',
    run:
        duckdb_exporters.check_for_duplicate_curies(input.parquet_dir, output.duckdb_filename, output.duplicate_curies)

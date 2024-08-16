from src.snakefiles.util import get_all_compendia, get_all_synonyms_with_drugchemicalconflated
import src.exporters.duckdb_exporters as duckdb_exporters
import os

### Write all compendia, synonym and conflation files into DuckDB databases.

# Write all compendia files to DuckDB and Parquet, then create `babel_outputs/duckdb/compendia_done` to signal that we're done.
rule export_all_compendia_to_duckdb:
    input:
        nodes_files=expand("{od}/duckdb/parquet/filename={fn}/{fn}.duckdb",
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
        duckdb_filename=config['output_directory'] + "/duckdb/parquet/filename={filename}/{filename}.duckdb"
    run:
        duckdb_exporters.export_compendia_to_parquet(input.compendium_file)


# Write all synonyms files to Parquet via DuckDB, then create `babel_outputs/duckdb/synonyms_done` to signal that we're done.
rule export_synonyms_to_duckdb:
    input:
        sapbert_training_file=expand("{od}/sapbert-training-data/{fn}.gz",
            od=config['output_directory'],
            fn=get_all_synonyms_with_drugchemicalconflated(config)
        )
    output:
        x = config['output_directory'] + '/sapbert-training-data/done',
    shell:
        "echo 'done' >> {output.x}"


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
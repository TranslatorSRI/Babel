# The DuckDB exporter can be used to export particular intermediate files into the
# in-process database engine DuckDB (https://duckdb.org) for future querying.
import os.path
import pathlib

import duckdb

from src.node import get_config


def setup_duckdb(duckdb_filename):
    """
    Set up a DuckDB instance using the settings in the config.

    :return: The DuckDB instance to be used.
    """
    db = duckdb.connect(duckdb_filename, config=get_config().get('duckdb_config', {}))

    # Turn on a progress bar.
    db.sql("PRAGMA enable_progress_bar=true")

    return db


def export_synonyms_to_parquet(synonyms_filename, duckdb_filename, cliques_parquet_filename, synonyms_parquet_filename):
    """
    Export a synonyms file to a DuckDB directory.

    :param synonyms_filename: The synonym file (in JSONL) to export to Parquet.
    :param duckdb_filename: A DuckDB file to temporarily store data in.
    :param cliques_parquet_filename: The Cliques.parquet file to create.
    :param synonyms_parquet_filename: The Synonyms.parquet file to create.
    """

    # Make sure that duckdb_filename doesn't exist.
    if os.path.exists(duckdb_filename):
        raise RuntimeError(f"Will not overwrite existing file {duckdb_filename}")

    os.makedirs(os.path.dirname(duckdb_filename), exist_ok=True)
    with setup_duckdb(duckdb_filename) as db:
        # Step 1. Load the entire synonyms file.
        synonyms_jsonld = db.read_json(synonyms_filename, format='newline_delimited')

        # Step 2. Create a Cliques table with all the cliques from this file.
        db.sql("CREATE TABLE Cliques (curie TEXT PRIMARY KEY, label TEXT, clique_identifier_count INT, biolink_type TEXT)")
        db.sql("INSERT INTO Cliques (curie, label, clique_identifier_count, biolink_type) " +
               "SELECT curie, replace(preferred_name, '\"\"\"', '\"') AS label, clique_identifier_count, " +
               "CONCAT('biolink:', json_extract_string(types, '$[0]')) AS biolink_type FROM synonyms_jsonld")

        # Step 3. Create a Synonyms table with all the cliques from this file.
        db.sql("CREATE TABLE Synonyms AS SELECT curie, unnest(names) AS label FROM synonyms_jsonld")

        # Step 3. Export as Parquet files.
        db.sql("SELECT curie, label, clique_identifier_count, biolink_type FROM Cliques").write_parquet(
            cliques_parquet_filename
        )
        db.sql("SELECT curie, label FROM Synonyms").write_parquet(
            synonyms_parquet_filename
        )


def identify_identically_labeled_cliques(duckdb_filename, output_filename):
    """
    Identify identically labeled cliques in the specified DuckDB database.

    :param duckdb_filename: The DuckDB database containing entries.
    """
    db = setup_duckdb(duckdb_filename)

    # Thanks, ChatGPT.
    db.sql("""
        WITH curie_counts AS (SELECT label, COUNT(curie) AS curie_count FROM Cliques
            GROUP BY label
            HAVING COUNT(curie) > 1
            ORDER BY curie_count DESC)
        SELECT 
            curie_counts.label,
            curie_counts.curie_count,
            STRING_AGG(Cliques.curie, '|') AS curies
        FROM 
            curie_counts
        JOIN 
            Cliques ON curie_counts.label = Cliques.label
        GROUP BY 
            curie_counts.label, 
            curie_counts.curie_count
        ORDER BY 
            curie_counts.curie_count DESC;
    """).write_csv(output_filename)


def get_label_distribution(duckdb_filename, output_filename):
    db = setup_duckdb(duckdb_filename)

    # Thanks, ChatGPT.
    db.sql("""
       WITH Lengths AS (
            SELECT 
                curie,
                label, 
                LENGTH(label) AS label_length
            FROM 
                Cliques
        ), Examples AS (
            SELECT 
                curie,
                label, 
                label_length,
                ROW_NUMBER() OVER (PARTITION BY label_length ORDER BY label) AS rn
            FROM 
                Lengths
        )
        SELECT 
            label_length, 
            COUNT(*) AS frequency,
            MAX(CASE WHEN rn = 1 THEN curie ELSE NULL END) AS example_curie,
            MAX(CASE WHEN rn = 1 THEN label ELSE NULL END) AS example_label
        FROM 
            Examples
        GROUP BY 
            label_length
        ORDER BY 
            label_length ASC; 
    """).write_csv(output_filename)


# During development, it'll be easier if we can call this directly.
if __name__ == "__main__":
    os.remove("babel_outputs/intermediate/duckdb/DrugChemicalConflated.db")
    export_synonyms_to_parquet(
        "babel_outputs/synonyms/DrugChemicalConflated.txt.gz",
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated.db",
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated_Cliques.parquet",
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated_Synonyms.parquet"
    )
    identify_identically_labeled_cliques(
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated.db",
        "babel_outputs/reports/DrugChemicalConflated_with_identical_labels.csv")
    get_label_distribution(
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated.db",
        "babel_outputs/reports/DrugChemicalConflated_label_distribution.csv")

    # export_synonyms_to_parquet(
    #     "babel_outputs/synonyms/AnatomicalEntity.txt",
    #     "babel_outputs/intermediate/duckdb/AnatomicalEntity.db",
    #     "babel_outputs/intermediate/duckdb/AnatomicalEntity_Cliques.parquet",
    #     "babel_outputs/intermediate/duckdb/AnatomicalEntity_Synonyms.parquet"
    # )

# The DuckDB exporter can be used to export particular intermediate files into the
# in-process database engine DuckDB (https://duckdb.org) for future querying.
import os.path
import pathlib
import time

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


def export_compendia_to_parquet(compendium_filename, duckdb_filename):
    """
    Export a compendium to a Parquet file via a DuckDB.

    :param compendium_filename: The compendium filename to read.
    :param duckdb_filename: The DuckDB filename to write. We will write the Parquet files into the directory that
        this file is located in.
    """

    # Make sure that duckdb_filename doesn't exist.
    if os.path.exists(duckdb_filename):
        raise RuntimeError(f"Will not overwrite existing file {duckdb_filename}")

    duckdb_dir = os.path.dirname(duckdb_filename)
    os.makedirs(duckdb_dir, exist_ok=True)
    clique_parquet_filename = os.path.join(duckdb_dir, 'Clique.parquet')
    edge_parquet_filename = os.path.join(duckdb_dir, 'Edge.parquet')
    node_parquet_filename = os.path.join(duckdb_dir, 'Node.parquet')

    with setup_duckdb(duckdb_filename) as db:
        # Step 1. Load the entire synonyms file.
        compendium_jsonl = db.read_json(compendium_filename, format='newline_delimited')

        # TODO: add props

        # Step 2. Create a Cliques table with all the cliques from this file.
        db.sql("""CREATE TABLE Clique
                (clique_leader STRING, preferred_name STRING, clique_identifier_count INT, biolink_type STRING,
                information_content FLOAT)""")
        db.sql("""INSERT INTO Clique SELECT
                        json_extract_string(identifiers, '$[0].i') AS clique_leader,
                        preferred_name,
                        len(identifiers) AS clique_identifier_count,
                        type AS biolink_type,
                        ic AS information_content
                    FROM compendium_jsonl""")

        # Step 3. Create an Edge table with all the clique/CURIE relationships from this file.
        db.sql("CREATE TABLE Edge (clique_leader STRING, curie STRING, conflation STRING)")
        db.sql("""INSERT INTO Edge SELECT
                json_extract_string(identifiers, '$[0].i') AS clique_leader,
                UNNEST(json_extract_string(identifiers, '$[*].i')) AS curie,
                'None' AS conflation
            FROM compendium_jsonl""")

        # Step 4. Create a Nodes table with all the nodes from this file.
        db.sql("""CREATE TABLE Node (curie STRING, label STRING, label_lc STRING, description STRING[])""")
        db.sql("""INSERT INTO Node
            SELECT
                json_extract_string(identifier, '$.identifiers.i') AS curie,
                json_extract_string(identifier, '$.identifiers.l') AS label,
                LOWER(label) AS label_lc,
                json_extract_string(identifier, '$.identifiers.d') AS description
            FROM compendium_jsonl, UNNEST(identifiers) AS identifier""")

        # Step 5. Export as Parquet files.
        db.sql("SELECT * FROM Clique").write_parquet(
            clique_parquet_filename
        )
        db.sql("SELECT * FROM Edge").write_parquet(
            edge_parquet_filename
        )
        db.sql("SELECT * FROM Node").write_parquet(
            node_parquet_filename
        )


def export_synonyms_to_parquet(synonyms_filename, duckdb_filename):
    """
    Export a synonyms file to a DuckDB directory.

    :param synonyms_filename: The synonym file (in JSONL) to export to Parquet.
    :param duckdb_filename: A DuckDB file to temporarily store data in.
    """

    # Make sure that duckdb_filename doesn't exist.
    if os.path.exists(duckdb_filename):
        raise RuntimeError(f"Will not overwrite existing file {duckdb_filename}")

    duckdb_dir = os.path.dirname(duckdb_filename)
    os.makedirs(duckdb_dir, exist_ok=True)
    synonyms_parquet_filename = os.path.join(duckdb_dir, f'Synonym.parquet')

    with setup_duckdb(duckdb_filename) as db:
        # Step 1. Load the entire synonyms file.
        synonyms_jsonl = db.read_json(synonyms_filename, format='newline_delimited')

        # Step 2. Create a Cliques table with all the cliques from this file.
        #db.sql("CREATE TABLE Cliques (curie TEXT PRIMARY KEY, label TEXT, clique_identifier_count INT, biolink_type TEXT)")
        #db.sql("INSERT INTO Cliques (curie, label, clique_identifier_count, biolink_type) " +
        #       "SELECT curie, replace(preferred_name, '\"\"\"', '\"') AS label, clique_identifier_count, " +
        #       "CONCAT('biolink:', json_extract_string(types, '$[0]')) AS biolink_type FROM synonyms_jsonl")

        # Step 3. Create a Synonyms table with all the cliques from this file.
        db.sql("""CREATE TABLE Synonyms (clique_leader STRING, preferred_name STRING, preferred_name_lc STRING,
            biolink_type STRING, label STRING, label_lc STRING)""")

        # We can't execute the following INSERT statement unless we have at least one row in the input data.
        # So let's test that now.
        result = db.execute("SELECT COUNT(*) AS row_count FROM synonyms_jsonl").fetchone()
        row_count = result[0]

        # Assuming we have data in synonyms_jsonl, write it out now.
        if row_count > 0:
            db.sql("""INSERT INTO Synonyms
                SELECT curie AS clique_leader, preferred_name,
                    LOWER(preferred_name) AS preferred_name_lc,
                    CONCAT('biolink:', json_extract_string(types, '$[0]')) AS biolink_type,
                    unnest(names) AS label, LOWER(label) AS label_lc
                FROM synonyms_jsonl""")

        # Step 3. Export as Parquet files.
        db.sql("SELECT clique_leader, preferred_name, preferred_name_lc, biolink_type, label, label_lc FROM Synonyms").write_parquet(
            synonyms_parquet_filename
        )


def check_for_identically_labeled_cliques(parquet_root, duckdb_filename, identically_labeled_cliques_csv):
    """
    Generate a list of identically labeled cliques.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param identically_labeled_cliques_csv: The output file listing identically labeled cliques.
    """

    db = setup_duckdb(duckdb_filename)
    cliques = db.read_parquet(
        os.path.join(parquet_root, "**/Cliques.parquet"),
        hive_partitioning=True
    )

    db.sql("""
        WITH curie_counts AS (SELECT LOWER(preferred_name) AS preferred_name_lc, COUNT(clique_leader) AS curie_count FROM cliques
            WHERE filename NOT IN ('DrugChemicalConflated')
            GROUP BY preferred_name_lc HAVING COUNT(clique_leader) > 1
            ORDER BY curie_count DESC)
        SELECT 
            preferred_name_lc,
            curie_count,
            STRING_AGG(DISINCT cliques.filename, '||', ORDER BY cliques.filename ASC) AS filenames,
            STRING_AGG(DISINCT cliques.biolink_type, '||', ORDER BY cliques.biolink_type ASC) AS biolink_types,
            STRING_AGG(cliques.clique_leader, '||', ORDER BY cliques.clique_leader ASC) AS curies
        FROM 
            curie_counts
        JOIN 
            cliques ON curie_counts.preferred_name_lc = LOWER(cliques.preferred_name)
        GROUP BY 
            curie_counts.preferred_name, 
            curie_counts.curie_count
        ORDER BY 
            curie_counts.curie_count DESC;
    """).write_csv(identically_labeled_cliques_csv)


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
    start_time = time.time()
    os.remove("babel_outputs/intermediate/duckdb/DrugChemicalConflated.db")
    export_synonyms_to_parquet(
        "babel_outputs/synonyms/DrugChemicalConflated.txt.gz",
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated.db",
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated_Cliques.parquet",
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated_Synonyms.parquet"
    )
    print(f"Exported DrugChemicalConflated.db and Parquet files in ${time.time() - start_time}")
    start_time = time.time()
    identify_identically_labeled_cliques(
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated.db",
        "babel_outputs/reports/DrugChemicalConflated_with_identical_labels.csv")
    get_label_distribution(
        "babel_outputs/intermediate/duckdb/DrugChemicalConflated.db",
        "babel_outputs/reports/DrugChemicalConflated_label_distribution.csv")
    print(f"Generated reports in ${time.time() - start_time}")

    # export_synonyms_to_parquet(
    #     "babel_outputs/synonyms/AnatomicalEntity.txt",
    #     "babel_outputs/intermediate/duckdb/AnatomicalEntity.db",
    #     "babel_outputs/intermediate/duckdb/AnatomicalEntity_Cliques.parquet",
    #     "babel_outputs/intermediate/duckdb/AnatomicalEntity_Synonyms.parquet"
    # )

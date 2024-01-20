"""
There are some tests we would like to do that apply to the entire Babel synonyms.

To do this, our current strategy is to go through the entire Babel synonyms and
add the relevant information into a SQLite database. We can then check with this
database to look for relevant duplication.
"""
import json
import logging
import sqlite3
from pathlib import Path


def report_on_index_wide_synonym_tests(synonym_files, sqlite_file, report_file):
    # Start writing to the report file so Snakemake knows we're working.
    Path(report_file).touch()
    Path(sqlite_file).touch()

    # Open the SQLite file that we will use to keep track of duplicates.
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file + '.db')
    c = conn.cursor()

    # Create a compendia table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS synonyms (
                        curie TEXT NOT NULL PRIMARY KEY UNIQUE,
                        biolink_type TEXT,
                        preferred_name TEXT,
                        preferred_name_lc TEXT
                    )''')

    # Go through all the compendia files in the order provided.
    for synonyms_file_index, synonyms_file in enumerate(synonym_files):
        # Go through every entry in each synonyms_file
        logging.info(f"Reading synonyms file {synonyms_file} ({synonyms_file_index + 1}/{len(synonym_files)})")

        count_entries = 0
        with open(synonyms_file, 'r') as synonymsfile:
            for line in synonymsfile:
                entry = json.loads(line)
                count_entries += 1

                curie = entry['curie']
                if len(entry['type']) > 0:
                    biolink_type = 'biolink:' + entry['type'][0]
                preferred_name = entry['preferred_name']
                preferred_name_lc = preferred_name.lower()

                # This should give us an error if we see the same CURIE in multiple files.
                c.execute("INSERT INTO synonyms (curie, biolink_type, preferred_name, preferred_name_lc) VALUES (?, ?, ?, ?)",
                (curie, biolink_type, preferred_name, preferred_name_lc))

        logging.info(f"Read {count_entries} entries from {synonyms_file}.")
        conn.commit()

        # Count the number of curie values in the synonyms table in SQLite.
        c.execute("SELECT COUNT(curie) FROM synonyms")
        curie_count = c.fetchone()

        logging.info(f"{curie_count} CURIEs loaded into {sqlite_file}")

    with open(report_file, 'w') as reportfile:
        # TODO: actually check for duplicate labels here.
        c.execute("SELECT COUNT(curie) FROM synonyms")
        curie_count = c.fetchone()

        json.dump({
            'curie_count': curie_count,
        }, reportfile)

    # Close the database connection
    conn.close()

"""
There are some tests we would like to do that apply to the entire Babel compendia.

To do this, our current strategy is to go through the entire Babel compendia and
add the relevant information into a SQLite database. We can then check with this
database to look for relevant duplication.
"""
import json
import logging
import sqlite3
from pathlib import Path


def report_on_index_wide_compendia_tests(compendia_files, sqlite_file, report_file):
    Path(sqlite_file).touch()
    Path(report_file).touch()

    # Open the SQLite file that we will use to keep track of duplicates.
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file + '.db')
    c = conn.cursor()

    # Create a compendia table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS compendia (
                        preferred_curie TEXT NOT NULL PRIMARY KEY,
                        curie TEXT NOT NULL
                    )''')

    # Go through all the compendia files in the order provided.
    for compendia_file_index, compendia_file in enumerate(compendia_files):
        # Go through every entry in each compendia_file
        logging.info(f"Reading {compendia_file} ({compendia_file_index + 1}/{len(compendia_files)})")

        count_curies = 0
        with open(compendia_file, 'r') as compendiafile:
            for line in compendiafile:
                entry = json.loads(line)
                identifiers = entry['identifiers']

                if len(identifiers) > 0:
                    preferred_curie = identifiers[0]['i']
                    for identifier in identifiers:
                        curie = identifier['i']
                        count_curies += 1
                        c.execute("INSERT INTO compendia (preferred_curie, curie) VALUES (?, ?)", (preferred_curie, curie))

        logging.info(f"Read {count_curies} into SQLite database {sqlite_file}.")

        # Query the table to check if the data was inserted correctly
        conn.commit()
        c.execute("SELECT COUNT(*) FROM compendia")
        record_count = c.fetchone()

        logging.info(f"SQLite database contains {record_count} records.")

    # Start writing the report file.
    with open(report_file, 'w') as reportfile:
        c.execute("SELECT COUNT(curie) FROM compendia")
        curie_count = c.fetchone()

        # Look for curies mapped to multiple preferred_curies.
        c.execute("SELECT curie, COUNT(DISTINCT preferred_curie), GROUP_CONCAT(DISTINCT preferred_curie) FROM compendia GROUP BY curie HAVING COUNT(DISTINCT preferred_curie) > 1 ORDER BY COUNT(DISTINCT preferred_curie) DESC;")
        results = c.fetchall()
        duplicates = [{'curie': duplicate[0], 'count': duplicate[1], 'preferred_curies': duplicate[2].split(',')} for duplicate in results]

        json.dump({
            'curie_count': curie_count,
            'duplicates': duplicates
        }, reportfile)

    # Close the database connection
    conn.close()

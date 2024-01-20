"""
There are some tests we would like to do that apply to the entire Babel synonyms.

To do this, our current strategy is to go through the entire Babel synonyms and
add the relevant information into a SQLite database. We can then check with this
database to look for relevant duplication.
"""
import json
import logging
import sqlite3


def report_on_index_wide_compendia_tests(synonym_files, report_file):
    
    # Open the SQLite file that we will use to keep track of duplicates.
    # Connect to the SQLite database
    conn = sqlite3.connect('synonyms.sqlite3')
    c = conn.cursor()

    # Create a compendia table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS synonyms (
                        curie TEXT NOT NULL PRIMARY KEY UNIQUE,
                        preferred_name TEXT,
                        preferred_name_lc TEXT
                    ) STRICT''')

    # Go through all the compendia files in the order provided.
    for synonyms_file_index, synonyms_file in enumerate(synonym_files):
        # Go through every entry in each synonyms_file
        logging.info(f"Reading synonyms file {synonyms_file} ({synonyms_file_index + 1}/{len(synonym_files)})")
        with open(synonyms_file, 'r') as compendiafile:
            for line in compendiafile:
                entry = json.loads(line)

                curie = entry['curie']
                preferred_name = entry['preferred_name']
                preferred_name_lc = preferred_name.lower()

                # This should give us an error if we see the same CURIE in multiple files.
                c.execute("INSERT INTO synonyms (curie, preferred_name, preferred_name_lc) VALUES (?, ?, ?)",
                (curie, preferred_name, preferred_name_lc))




        # Insert test data into the table
        c.execute("INSERT INTO compendia (name, description, data) VALUES (?, ?, ?)",
                  ('Test Compendium', 'This is a test compendium', 'Some test data'))

        # Query the table to check if the data was inserted correctly
        c.execute("SELECT * FROM compendia")
        result = c.fetchone()

        # Close the database connection
        conn.close()

        # Assert that the data was inserted correctly
        assert result == (1, 'Test Compendium', 'This is a test compendium', 'Some test data')

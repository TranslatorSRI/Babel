"""
There are some tests we would like to do that apply to the entire Babel compendia.

To do this, our current strategy is to go through the entire Babel compendia and
add the relevant information into a SQLite database. We can then check with this
database to look for relevant duplication.
"""
import json
import logging
import sqlite3


def report_on_index_wide_compendia_tests(compendia_files, report_file):
    # Open the SQLite file that we will use to keep track of duplicates.
    # Connect to the SQLite database
    conn = sqlite3.connect('compendia.sqlite3')
    c = conn.cursor()

    # Create a compendia table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS compendia (
                        curie TEXT NOT NULL,
                        label TEXT,
                        preferred_curie TEXT NOT NULL,
                    ) STRICT''')

    c.execute('''CREATE INDEX index_preferred_curie ON ''')

    # Start writing the report file.
    with open(report_file, 'w') as reportfile:
        # Go through all the compendia files in the order provided.
        for compendia_file_index, compendia_file in enumerate(compendia_files):
            # Go through every entry in each compendia_file
            logging.info(f"Reading {compendia_file} ({compendia_file_index + 1}/{len(compendia_files)})")
            with open(compendia_file, 'r') as compendiafile:
                for line in compendiafile:
                    entry = json.loads(line)

                    # For each entry, we insert


            # Write the content into the report file
            reportfile.write(content)



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

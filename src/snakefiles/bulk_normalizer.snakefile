#
# bulk_normalizer.snakefile - Rules for bulk normalizing various files
#
# These are generally intended for bulk normalizing files that we hope to eventually incorporate into Babel,
# but I won't judge you if you use it for one-off bulk normalization.
#
import csv
import gzip
import json
from collections import defaultdict

from src.babel_utils import pull_via_wget, WgetRecursionOptions
from src.exporters.duckdb_exporters import setup_duckdb
from src.util import get_logger

logger = get_logger(__name__)

# Step 1. Download bulk identifiers to normalize.
rule download_bulk_normalizer_files:
    output:
        bulk_normalizer_dir = directory(config['download_directory'] + '/bulk-normalizer/bulk_normalizer'),
        file_list = config['download_directory'] + '/bulk-normalizer/file-list.txt',
    run:
        pull_via_wget(
            url_prefix='https://stars.renci.org/var/babel_outputs/',
            in_file_name='bulk_normalizer/',
            subpath='bulk-normalizer',
            decompress=False,
            recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        )
        with open(output.file_list, 'w') as f:
            # Get the list of files in bulk_normalizer_dir
            all_files = os.listdir(output.bulk_normalizer_dir)
            filtered_files = [fname for fname in all_files if '.tsv' in fname or '.txt' in fname]
            f.write("\n".join(filtered_files))

# Step 2. Normalize the input files.
rule bulk_normalize_files:
    # TODO: If I can break this up into two rules, then I'll be able to normalize all these files in parallel.
    input:
        duckdb_done = config['output_directory'] + '/reports/duckdb/done',
        file_list = config['download_directory'] + '/bulk-normalizer/file-list.txt',
        bulk_normalizer_input_dir = directory(config['download_directory'] + '/bulk-normalizer/bulk_normalizer'),
    output:
        bulk_normalizer_output_dir = directory(config['output_directory'] + '/bulk-normalizer'),
        normalizer_done = config['output_directory'] + '/bulk-normalizer/done',
    run:
        os.makedirs(output.bulk_normalizer_output_dir + '/duckdbs', exist_ok=True)
        os.makedirs(output.bulk_normalizer_output_dir + '/bulk_normalizer', exist_ok=True)
        for filename in os.listdir(input.bulk_normalizer_input_dir):
            if '.txt' in filename or '.tsv' in filename:
                # Step 0. Set up DuckDB and filenames.
                duckdb_filename = output.bulk_normalizer_output_dir + '/duckdbs/' + filename + '.duckdb'
                db = setup_duckdb(duckdb_filename)

                input_filename = os.path.join(input.bulk_normalizer_input_dir, filename)
                output_filename = output.bulk_normalizer_output_dir + '/bulk_normalizer/' + filename

                # Step 1. Load the file into DuckDB.
                logger.info(f"Loading {input_filename} ...")
                input_csv = db.read_csv(input_filename)

                # Step 2. Normalize the identifiers.
                cliques = db.from_parquet(config['output_directory'] + '/duckdb/parquet/filename=*/Clique.parquet')
                edges = db.from_parquet(config['output_directory'] + '/duckdb/parquet/filename=*/Edge.parquet')
                conflations = db.from_parquet(config['output_directory'] + '/duckdb/parquet/filename=*/Conflation.parquet')

                result = db.sql("""SELECT
                                       input_csv.*,
                                       edges.clique_leader AS normalized_curie,
                                       COALESCE(conflations.preferred_curie, edges.clique_leader) AS normalized_conflated_curie,
                                       conflations.conflation AS conflation_type,
                                       cliques.preferred_name AS preferred_name,
                                       cliques.biolink_type AS biolink_type
                                   FROM input_csv
                                        LEFT JOIN edges ON UPPER(input_csv.id) = UPPER(edges.curie)
                                        LEFT JOIN cliques ON UPPER(edges.clique_leader) = UPPER(cliques.clique_leader)
                                        LEFT JOIN conflations ON UPPER(input_csv.id) = UPPER(conflations.curie) AND
                                            (conflations.conflation = 'DrugChemical' OR conflations.conflation = 'GeneProtein')""")

                # Step 3. Write out the output file.
                logger.info(f"Writing {output_filename} ...")
                result.to_csv(output_filename, sep='\t')

        with open(output.normalizer_done, 'w') as f:
            f.write("done")

# Generate a report for each normalized file.
rule bulk_normalize_reports:
    input:
        bulk_normalizer_output_dir = config['output_directory'] + '/bulk-normalizer',
        normalizer_done = config['output_directory'] + '/bulk-normalizer/done',
    output:
        bulk_normalizer_report = config['output_directory'] + '/bulk-normalizer/report.tsv',
    run:
        # Prepare to write the output file.
        with open(output.bulk_normalizer_report, 'w') as outf:
            writer = csv.DictWriter(outf, delimiter='\t', fieldnames=[
                'filename',
                'rows',
                'unique_id_count',
                'unique_normalized_curie_count',
                'unique_normalized_curie_percent',
                'unique_normalized_curie_absent',
                'unique_normalized_curie_absent_percent',
                'biolink_types',
                # The following columns probably aren't very useful.
                'rows_with_normalized_curie',
                'rows_with_normalized_curie_percent',
                'rows_without_normalized_curie',
                'rows_without_normalized_curie_percent'
            ])
            writer.writeheader()

            # Iterate over all the files in the bulk-normalizer output directory.
            for filename in os.listdir(input.bulk_normalizer_output_dir + '/bulk_normalizer'):
                filename_lc = filename.lower()
                if '.txt' in filename_lc or '.tsv' in filename_lc:
                    logger.info(f"Generating report for bulk normalized file {filename} ...")
                    if '.gz' in filename_lc:
                        file = gzip.open(input.bulk_normalizer_output_dir + '/bulk_normalizer/' + filename, 'rt')
                    else:
                        file = open(input.bulk_normalizer_output_dir + '/bulk_normalizer/' + filename, 'r')

                    # Get the number of input records and the number of normalized records.
                    row_count = 0
                    rows_with_normalized_curie_count = 0
                    rows_without_normalized_curie_count = 0
                    unique_id = set()
                    unique_normalized_curie = set()
                    biolink_types = defaultdict(int)

                    reader = csv.DictReader(file, delimiter='\t')
                    for row in reader:
                        row_count += 1
                        unique_id.add(row['id'])
                        if not row['normalized_curie']:
                            rows_without_normalized_curie_count += 1
                        else:
                            unique_normalized_curie.add(row['normalized_curie'])
                            rows_with_normalized_curie_count += 1
                        biolink_types[row['biolink_type']] += 1

                    file.close()

                    # Write out the report line.
                    writer.writerow({
                        'filename': filename,
                        'rows': row_count,
                        'unique_id_count': len(unique_id),
                        'unique_normalized_curie_count': len(unique_normalized_curie),
                        'unique_normalized_curie_percent': round(len(unique_normalized_curie) / len(unique_id) * 100, 2),
                        'unique_normalized_curie_absent': len(unique_id) - len(unique_normalized_curie),
                        'unique_normalized_curie_absent_percent': round((len(unique_id) - len(unique_normalized_curie)) / len(unique_id) * 100, 2),
                        'biolink_types': json.dumps(biolink_types),

                        # The following columns probably aren't very useful.
                        'rows_with_normalized_curie': rows_with_normalized_curie_count,
                        'rows_with_normalized_curie_percent': round(rows_with_normalized_curie_count / row_count * 100, 2),
                        'rows_without_normalized_curie': rows_without_normalized_curie_count,
                        'rows_without_normalized_curie_percent': round(rows_without_normalized_curie_count / row_count * 100, 2),
                    })

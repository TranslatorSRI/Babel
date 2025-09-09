#
# bulk_normalizer.snakefile - Rules for bulk normalizing various files
#
# These are generally intended for bulk normalizing files that we hope to eventually incorporate into Babel,
# but I won't judge you if you use it for one-off bulk normalization.
#
from src.babel_utils import pull_via_wget, WgetRecursionOptions

# Step 1. Download bulk identifiers to normalize.
rule download_bulk_normalizer_files:
    output:
        download_done = config['download_directory'] + '/bulk-normalizer/done'
    run:
        pull_via_wget(
            url_prefix='https://stars.renci.org/var/babel_outputs/',
            in_file_name='bulk_normalizer/',
            subpath='bulk-normalizer',
            decompress=False,
            recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        )
        with open(output.download_done, 'w') as f:
            f.write('done')

rule bulk_normalize_files:
    input:
        duckdb_done = config['output_directory'] + '/reports/duckdb/done',
        download_done = config['download_directory'] + '/bulk-normalizer/done',
        input_files = config['download_directory'] + '/bulk-normalizer/bulk_normalizer/{basename}',
    output:
        output_files = config['output_directory'] + '/bulk-normalizer/{basename}',
    wildcard_constraints:
        basename=".*(?:tsv|txt).*"
    run:
        print(f"Gonna process: {input.input_files}")

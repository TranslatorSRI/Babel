#
# bulk_normalizer.snakefile - Rules for bulk normalizing various files
#
# These are generally intended for bulk normalizing files that we hope to eventually incorporate into Babel,
# but I won't judge you if you use it for one-off bulk normalization.
#
from src.babel_utils import pull_via_wget, WgetRecursionOptions
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

def get_bulk_normalizer_output_file_list():
    path = config['output_directory'] + '/bulk-normalizer/'
    if not os.path.exists(path):
        os.makedirs(path)
    with open(config['download_directory'] + '/bulk-normalizer/file-list.txt', 'r') as inf:
        for line in inf:
            yield os.path.join(path, line.strip())

rule bulk_normalize_all_files:
    input:
        file_list = config['download_directory'] + '/bulk-normalizer/file-list.txt',
        completed_files=get_bulk_normalizer_output_file_list,
    output:
        normalizer_done = config['output_directory'] + '/bulk-normalizer/done',
    run:
        with open(output.normalizer_done, 'w') as f:
            f.write('done')

rule bulk_normalize_file:
    input:
        duckdb_done = config['output_directory'] + '/reports/duckdb/done',
        input_file = config['download_directory'] + '/bulk-normalizer/bulk_normalizer/{basename}',
    output:
        output_file = config['output_directory'] + '/bulk-normalizer/{basename}',
    run:
        logger.info(f"Gonna transform {input.input_file} into {output.output_file}")

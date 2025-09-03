# translator_hierarchy.snakefile - Download and normalize the Ubergraph hierarchy for Translator.
from src.babel_utils import pull_via_wget

# Step 1. Download the Ubergraph hierarchy.
rule download_ubergraph_hierarchy:
    output:
        build_metadata_nt = config['output_directory'] + '/UbergraphHierarchy/nonredundant-graph-table/build-metadata.nt',
        node_labels_tsv = config['download_directory'] + '/UbergraphHierarchy/nonredundant-graph-table/node-labels.tsv',
        edge_labels_tsv = config['download_directory'] + '/UbergraphHierarchy/nonredundant-graph-table/edge-labels.tsv',
    run:
        pull_via_wget(
            'https://ubergraph.apps.renci.org/downloads/current/',
            'nonredundant-graph-table.tgz',
            subpath='UbergraphHierarchy',
            uncompressed_filenames=[
                'nonredundant-graph-table/build-metadata.nt',
                'nonredundant-graph-table/node-labels.tsv',
                'nonredundant-graph-table/edge-labels.tsv'
            ],
            decompress=True,
        )

# Step 2. Normalize the Ubergraph hierarchy.
rule normalize_ubergraph_hierarchy:
    input:
        duckdb_done = config['output_directory'] + '/reports/duckdb/done',

# Step 3. Confirm that all the Ubergraph Hierarchy work is done.
rule ubergraph_hierarchy_done:
    output:
        done_file = config['output_directory'] + '/reports/ubergraph_hierarchy_done'
    run:
        with open(output.done_file, 'w') as f:
            f.write('done')

# translator_hierarchy.snakefile - Download and normalize the Ubergraph hierarchy for Translator.
import curies
import duckdb
import pandas as pd

from src.babel_utils import pull_via_wget
from src.exporters.duckdb_exporters import setup_duckdb
from src.util import get_biolink_prefix_map, get_logger

# Step 1. Download the Ubergraph hierarchy.
rule download_ubergraph_hierarchy:
    output:
        build_metadata_nt = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/build-metadata.nt',
        node_labels_tsv = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/node-labels.tsv',
        edge_labels_tsv = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/edge-labels.tsv',
        edge_tsv = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/edges.tsv',
    run:
        pull_via_wget(
            'https://ubergraph.apps.renci.org/downloads/current/',
            'redundant-graph-table.tgz',
            subpath='UbergraphHierarchy',
            uncompressed_filenames=[
                'redundant-graph-table/build-metadata.nt',
                'redundant-graph-table/node-labels.tsv',
                'redundant-graph-table/edge-labels.tsv',
                'redundant-graph-table/edges.tsv'
            ],
            decompress=True,
        )

# Step 2. Normalize the Ubergraph hierarchy.
rule normalize_ubergraph_hierarchy:
    input:
        # duckdb_done = config['output_directory'] + '/reports/duckdb/done',
        node_labels_tsv = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/node-labels.tsv',
        edge_labels_tsv = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/edge-labels.tsv',
        edge_tsv = config['download_directory'] + '/UbergraphHierarchy/redundant-graph-table/edges.tsv',
    output:
        node_labels_harmonized = config['output_directory'] + '/UbergraphHierarchy/node-labels-harmonized.tsv',
        iris_not_transformed = config['output_directory'] + '/UbergraphHierarchy/iris-not-transformed-into-curies.txt',
        ubergraph_redundant_triples_tsv = config['output_directory'] + '/UbergraphHierarchy/ubergraph-redundant-triples-harmonized.tsv',
    run:
        logger = get_logger("snakemake:normalize_ubergraph_hierarchy")
        os.makedirs(config['output_directory'] + '/UbergraphHierarchy', exist_ok=True)

        # Step 1. Prepare for Ubergraph URL-to-CURIE mapping.
        biolink_prefix_map = get_biolink_prefix_map()
        ubergraph_iri_stem_to_prefix_map = curies.Converter.from_reverse_prefix_map(config['ubergraph_iri_stem_to_prefix_map'])

        # Step 2. Load all the entries into memory.
        node_labels_df = pd.read_csv(input.node_labels_tsv, sep='\t', header=None, names=['node_id', 'iri'])
        iris = node_labels_df['iri'].tolist()
        iris_not_transformed = set()

        curies_column = []
        for iri in iris:
            # Transform the IRI into a CURIE.
            curie = biolink_prefix_map.compress(iri)
            if curie is None:
                curie = ubergraph_iri_stem_to_prefix_map.compress(iri)

            if curie is None:
                if iri not in iris_not_transformed:
                    logger.warning(f'Could not transform {iri} into a CURIE.')
                curies_column.append(iri)
                iris_not_transformed.add(iri)
                continue

            curies_column.append(curie)

        # Write out all the IRIs that couldn't be transformed.
        with open(output.iris_not_transformed, 'w') as fout:
            for iri in sorted(iris_not_transformed):
                fout.write(iri + '\n')

        # Add CURIEs back to the dataframe.
        assert len(curies_column) == len(node_labels_df)
        node_labels_df['curie'] = pd.Series(curies_column, index=node_labels_df.index)

        # Load the dataframe into a DuckDB table.
        db = setup_duckdb(config['output_directory'] + '/UbergraphHierarchy/duckdb.db')
        db.register('node_labels', node_labels_df)

        # Load the DuckDB files.
        # For now, we'll load the Parquet files instead, because I have some.
        cliques = db.from_parquet(config['output_directory'] + '/duckdb/parquet/filename=*/Clique.parquet')
        edges = db.from_parquet(config['output_directory'] + '/duckdb/parquet/filename=*/Edge.parquet')

        # Harmonize!
        result = db.sql("""SELECT node_id, iri, node_labels.curie, edges.clique_leader AS normalized_curie, cliques.preferred_name AS preferred_name
                  FROM node_labels
                  LEFT JOIN edges ON node_labels.curie = edges.curie
                  JOIN cliques ON edges.clique_leader = cliques.clique_leader
                  ORDER BY node_id ASC""")

        # Write out the harmonization results.
        result.to_csv(output.node_labels_harmonized, sep='\t', header=True)

        # We can do one other cool thing while we've got all this memory: reduce the edges file based on
        # which IRIs we were able to harmonize.
        predicate_labels = db.read_csv(input.edge_labels_tsv, sep='\t', header=None, names=['predicate_id', 'predicate_iri'])
        ubergraph_edges = db.read_csv(input.edge_tsv, sep='\t', header=None, names=['subject_id', 'predicate_id', 'object_id'])
        harmonized_edges = db.sql("""
            SELECT
                result_subj.node_id AS subject_id,
                result_subj.iri AS subject_iri,
                result_subj.curie AS subject_curie,
                result_subj.normalized_curie AS subject_curie_normalized,
                result_subj.preferred_name AS subject_preferred_name,
                predicate_iri AS predicate,
                result_obj.node_id AS object_id,
                result_obj.iri AS object_iri,
                result_obj.curie AS object_curie,
                result_obj.normalized_curie AS object_curie_normalized,
                result_obj.preferred_name AS object_preferred_name
            FROM ubergraph_edges
            JOIN predicate_labels ON predicate_labels.predicate_id = ubergraph_edges.predicate_id
            JOIN result result_subj ON ubergraph_edges.subject_id = result_subj.node_id AND result_subj.normalized_curie IS NOT NULL
            JOIN result result_obj ON ubergraph_edges.object_id = result_obj.node_id AND result_obj.normalized_curie IS NOT NULL""")
        harmonized_edges.to_csv(output.ubergraph_redundant_triples_tsv, sep='\t', header=True)

        # TODO: make mappings non-redundant.


# Step 3. Confirm that all the Ubergraph Hierarchy work is done.
rule ubergraph_hierarchy_done:
    input:
        node_labels_harmonized = config['output_directory'] + '/UbergraphHierarchy/node-labels-harmonized.tsv',
    output:
        done_file = config['output_directory'] + '/reports/ubergraph_hierarchy_done'
    run:
        with open(output.done_file, 'w') as f:
            f.write('done')

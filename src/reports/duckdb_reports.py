import csv
import json
from collections import Counter, defaultdict

from src.exporters.duckdb_exporters import setup_duckdb


def check_for_identically_labeled_cliques(parquet_root, duckdb_filename, identically_labeled_cliques_tsv):
    """
    Generate a list of identically labeled cliques.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param identically_labeled_cliques_csv: The output file listing identically labeled cliques.
    """

    db = setup_duckdb(duckdb_filename)
    cliques = db.read_parquet(
        os.path.join(parquet_root, "**/Clique.parquet"),
        hive_partitioning=True
    )

    db.sql("""
        WITH curie_counts AS (SELECT LOWER(preferred_name) AS preferred_name_lc, COUNT(clique_leader) AS curie_count FROM cliques
            WHERE filename NOT IN ('DrugChemicalConflated', 'Publication')
            GROUP BY preferred_name_lc HAVING COUNT(clique_leader) > 1
            ORDER BY curie_count DESC)
        SELECT 
            preferred_name_lc,
            curie_count,
            STRING_AGG(DISTINCT cliques.filename, '||' ORDER BY cliques.filename ASC) AS filenames,
            STRING_AGG(DISTINCT cliques.biolink_type, '||' ORDER BY cliques.biolink_type ASC) AS biolink_types,
            STRING_AGG(cliques.clique_leader, '||' ORDER BY cliques.clique_leader ASC) AS curies
        FROM 
            curie_counts
        JOIN 
            cliques ON curie_counts.preferred_name_lc = LOWER(cliques.preferred_name)
        GROUP BY 
            curie_counts.preferred_name_lc, 
            curie_counts.curie_count
        ORDER BY 
            curie_counts.curie_count DESC;
    """).write_csv(identically_labeled_cliques_tsv, sep="\t")


def check_for_duplicate_curies(parquet_root, duckdb_filename, duplicate_curies_tsv):
    """
    Generate a list of duplicate CURIEs.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param duplicate_curies_tsv: The output file listing duplicate CURIEs.
    """

    db = setup_duckdb(duckdb_filename)
    edges = db.read_parquet(
        os.path.join(parquet_root, "**/Edge.parquet"),
        hive_partitioning=True
    )
    cliques = db.read_parquet(
        os.path.join(parquet_root, "**/Clique.parquet"),
        hive_partitioning=True
    )

    db.sql("""
        WITH curie_counts AS (SELECT DISTINCT curie, COUNT(clique_leader) AS clique_leader_count FROM edges
            WHERE conflation = 'None'
            GROUP BY curie HAVING COUNT(DISTINCT clique_leader) > 1
            ORDER BY clique_leader_count DESC
        )
        SELECT 
            curie_counts.curie,
            clique_leader_count,
            STRING_AGG(DISTINCT cliques.filename, '||' ORDER BY cliques.filename ASC) AS filenames,
            STRING_AGG(DISTINCT cliques.biolink_type, '||' ORDER BY cliques.biolink_type ASC) AS biolink_types,
            STRING_AGG(cliques.clique_leader, '||' ORDER BY cliques.clique_leader ASC) AS clique_leaders,
            STRING_AGG(cliques.preferred_name, '||' ORDER BY cliques.preferred_name ASC) AS clique_preferred_names,
        FROM
            curie_counts
        JOIN edges ON edges.curie = curie_counts.curie
        JOIN cliques ON cliques.clique_leader = edges.clique_leader
        GROUP BY 
            curie_counts.curie, 
            curie_counts.clique_leader_count
        ORDER BY 
            curie_counts.clique_leader_count DESC;
    
    """).write_csv(duplicate_curies_tsv, sep="\t")


def generate_prefix_report(parquet_root, duckdb_filename, prefix_report_json, prefix_report_tsv):
    """
    Generate a report about all the prefixes within this system.

    See thoughts at https://github.com/TranslatorSRI/Babel/issues/359

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param prefix_report_json: The prefix report as JSON.
    :param prefix_report_tsv: The prefix report as TSV.
    """

    db = setup_duckdb(duckdb_filename)
    edges = db.read_parquet(
        os.path.join(parquet_root, "**/Edge.parquet"),
        hive_partitioning=True
    )
    cliques = db.read_parquet(
        os.path.join(parquet_root, "**/Clique.parquet"),
        hive_partitioning=True
    )

    # Step 1. Generate a by-prefix summary.
    curie_prefix_summary = db.sql("""
        SELECT
            split_part(curie, ':', 1) AS curie_prefix,
            COUNT(curie) AS curie_count,
            COUNT(DISTINCT curie) AS curie_distinct_count,
            COUNT(DISTINCT clique_leader) AS clique_distinct_count,
            STRING_AGG(edges.filename, '||' ORDER BY edges.filename ASC) AS filenames
        FROM
            edges
        GROUP BY
            curie_prefix
        ORDER BY
            curie_prefix ASC
    """)
    rows = curie_prefix_summary.fetchall()

    by_curie_prefix_results = {}
    for row in rows:
        curie_prefix = row[0]

        filename_counts = Counter(row[4].split('||'))

        by_curie_prefix_results[curie_prefix] = {
            'curie_count': row[1],
            'curie_distinct_count': row[2],
            'clique_distinct_count': row[3],
            'filenames': filename_counts,
        }

    # Step 2. Generate a by-clique summary.
    clique_summary = db.sql("""
        SELECT
            filename,
            split_part(clique_leader, ':', 1) AS clique_leader_prefix,
            COUNT(DISTINCT clique_leader) AS clique_count,
            STRING_AGG(split_part(curie, ':', 1), '||' ORDER BY curie ASC) AS curie_prefixes
        FROM
            edges
        GROUP BY
            filename, clique_leader_prefix
        ORDER BY
            filename ASC, clique_leader_prefix ASC
    """)
    rows = clique_summary.fetchall()

    by_clique_results = {}
    for row in rows:
        filename = row[0]
        clique_leader_prefix = row[1]
        clique_count = row[2]
        curie_prefixes = row[3].split('||')
        curie_prefix_counts = Counter(curie_prefixes)

        if clique_leader_prefix not in by_clique_results:
            by_clique_results[clique_leader_prefix] = {
                'count_cliques': 0,
                'by_file': {}
            }

        by_clique_results[clique_leader_prefix]['count_cliques'] += clique_count

        if filename not in by_clique_results[clique_leader_prefix]['by_file']:
            by_clique_results[clique_leader_prefix]['by_file'][filename] = defaultdict(int)

        for curie_prefix in curie_prefix_counts.keys():
            by_clique_results[clique_leader_prefix]['by_file'][filename][curie_prefix] += curie_prefix_counts[curie_prefix]

    # Generate totals.
    total_cliques = 0
    total_curies = 0
    for curie_leader_prefix in by_clique_results.keys():
        count_curies = 0
        total_cliques += by_clique_results[curie_leader_prefix]['count_cliques']
        for filename in by_clique_results[curie_leader_prefix]['by_file'].keys():
            count_curies += sum(by_clique_results[curie_leader_prefix]['by_file'][filename].values())
        by_clique_results[curie_leader_prefix]['count_curies'] = count_curies
        total_curies += count_curies

    # Step 3. Write out prefix report in JSON.
    with open(prefix_report_json, 'w') as fout:
        json.dump({
            'count_cliques': total_cliques,
            'count_curies': total_curies,
            'by_clique': by_clique_results,
            'by_curie_prefix': by_curie_prefix_results
        }, fout, indent=2, sort_keys=True)

    # Step 4. Write out prefix report in TSV. This is primarily based on the by-clique information, but also
    # includes totals.
    with open(prefix_report_tsv, 'w') as fout:
        csv_writer = csv.DictWriter(fout, dialect='excel-tab', fieldnames=[
            'Clique prefix', 'Filename', 'Clique count', 'CURIEs'
        ])
        csv_writer.writeheader()

        curie_totals = defaultdict(int)

        for prefix in sorted(by_clique_results.keys()):
            by_clique_result = by_clique_results[prefix]
            by_file = by_clique_result['by_file']

            count_cliques = by_clique_result['count_cliques']
            filename_curie_counts = defaultdict(int)

            for filename in by_file.keys():
                curie_prefixes_sorted = map(lambda x: f"{x[0]}: {x[1]}", sorted(by_file[filename].items(), key=lambda x: x[1], reverse=True))

                filename_count_curies = 0
                for curie_prefix in by_file[filename]:
                    curie_totals[curie_prefix] += by_file[filename][curie_prefix]
                    filename_curie_counts[curie_prefix] += by_file[filename][curie_prefix]
                    filename_count_curies += by_file[filename][curie_prefix]

                csv_writer.writerow({
                    'Clique prefix': prefix,
                    'Filename': filename,
                    'Clique count': count_cliques,
                    'CURIEs': f"{filename_count_curies}: " + ', '.join(curie_prefixes_sorted)
                })

            filename_curie_sorted = map(lambda x: f"{x[0]}: {x[1]}", sorted(filename_curie_counts.items(), key=lambda x: x[1], reverse=True))
            count_curies = sum(filename_curie_counts.values())

            # Don't bother with a total for the prefix unless there are at least two files.
            if len(by_file) > 1:
                csv_writer.writerow({
                    'Clique prefix': prefix,
                    'Filename': f"Total for prefix {prefix}",
                    'Clique count': count_cliques,
                    'CURIEs': f"{count_curies}: " + ', '.join(filename_curie_sorted)
                })

        curie_totals_sorted = map(lambda x: f"{x[0]}: {x[1]}", sorted(curie_totals.items(), key=lambda x: x[1], reverse=True))
        total_curies = sum(curie_totals.values())
        csv_writer.writerow({
            'Clique prefix': 'Total cliques',
            'Filename': '',
            'Clique count': total_cliques,
            'CURIEs': f"{total_curies}: " + ', '.join(curie_totals_sorted)
        })


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

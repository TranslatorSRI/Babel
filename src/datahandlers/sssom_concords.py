#
# This file contains functions for helping to convert SSSOM files into concord files.
# This will be helpful in incorporating SSSOM mappings from the Mapping Commons and
# other SSSOM-based sources into Babel.
#
import logging

from sssom import parsers

def convert_sssom_to_concord(sssom_filename, concord_filename, sssom_format='tsv', threshold=0, filter_predicates=None):
    """

    :param sssom_filename:
    :param concord_filename:
    :return:
    """

    if not filter_predicates:
        filter_predicates = set()

    # Step 1. Load the SSSOM file (or URL).
    logging.info(f"Loading SSSOM file {sssom_filename} with format {sssom_format}.")
    if sssom_format == 'tsv':
        mappings = parsers.parse_sssom_table(sssom_filename)
    elif sssom_format == 'rdf':
        mappings = parsers.parse_sssom_rdf(sssom_filename)
    else:
        raise ValueError(f"Unknown SSSOM format (only 'tsv' and 'rdf' are supported): {sssom_format}.")

    df = mappings.df

    # Step 2. Filter mappings using the threshold.
    if 'confidence' in df.columns:
        df_filtered = df[(df['confidence'] > threshold)]
        logging.info(f"Filtered {df.size} to {df_filtered.size} by filtering by confidence > {threshold}")
    else:
        df_filtered = df

    # Step 3. Filter mappings using particular predicates.
    if filter_predicates:
        df_filtered = df_filtered[(df_filtered['predicate_id'].isin(filter_predicates))]
        logging.info(f"Filtered {len(df_filtered)} from an original set of {len(df)} mappings using predicates: {filter_predicates}")

    # Step 4. Write the filtered mappings to a file.
    count_mappings = 0
    with open(concord_filename, 'w') as outf:
        for index in df_filtered.index:
            subject_id = df_filtered['subject_id'][index]
            object_id = df_filtered['object_id'][index]
            predicate_id = df_filtered['predicate_id'][index]

            if subject_id == 'sssom:NoTermFound' or object_id == 'sssom:NoTermFound':
                continue

            outf.print(f"{subject_id}\t{predicate_id}\t{object_id}\n")
            count_mappings += 1

    logging.info(f"Extracted {count_mappings} mappings to {concord_filename}")

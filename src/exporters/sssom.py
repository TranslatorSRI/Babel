# Once we generate the compendium files, we need to convert them into the
# SSSOM format (https://mapping-commons.github.io/sssom/).
# This file provides code for doing that, based on the code from
# https://github.com/TranslatorSRI/babel-validation/blob/84809c647a1e62f215606b6fe2c8b90c71886937/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L209-L282
import csv
import gzip
import hashlib
import json
import os
from itertools import combinations

import logging
from src.util import LoggingUtil

# Default logger for this file.
logger = LoggingUtil.init_logging(__name__, level=logging.INFO)


def convert_compendium_to_sssom(compendium_filename, sssom_filename):
    """
    Convert a compendium file to SSSOM format (https://mapping-commons.github.io/sssom/).

    Based on the code in https://github.com/TranslatorSRI/babel-validation/blob/84809c647a1e62f215606b6fe2c8b90c71886937/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L209-L282

    :param compendium_filename: The compendium file to convert.
    :param sssom_filename: The SSSOM gzipped file to write out.
    """

    logger.info(f"convert_compendium_to_sssom({compendium_filename}, {sssom_filename})")
    count_mappings = 0

    # We should be able to stream the output directly from the compendium JSON file, except for the header,
    # which includes a CURIE map. But we won't know what CURIE map to emit until after we've read all the inputs,
    # so we will write out the TSV file first, then reopen it to add the metadata.
    prefixes_to_url = {
        'skos': 'http://www.w3.org/2004/02/skos/core#',
        'biolink': 'https://w3id.org/biolink/vocab/',
        'semapv': 'https://w3id.org/semapv/vocab/',
    }
    header = [
        'subject_id',
        'subject_label',
        'predicate_id',
        'object_id',
        'object_label',
        'mapping_justification'
    ]
    with open(compendium_filename, "r") as fin, gzip.open(sssom_filename, "wt") as fout:
        csvfile = csv.DictWriter(fout, header, dialect=csv.excel_tab)
        csvfile.writeheader()
        for line in fin:
            clique = json.loads(line.strip())

            clique_type = clique["type"]
            identifiers = clique["identifiers"]
            if len(identifiers) == 0:
                logger.error(f"No identifiers found in clique {clique}, skipping.")
                continue

            preferred_identifier = identifiers[0]
            secondary_identifiers = identifiers[1:]

            # If we don't have any secondary identifiers, we add the clique by mapping the preferred identifier to
            # itself.
            if len(secondary_identifiers) == 0:
                secondary_identifier = [preferred_identifier]

            # Write out identifier mappings.
            for secondary_identifier in secondary_identifiers:
                csvfile.writerow({
                    'subject_id': preferred_identifier['i'],
                    'subject_label': preferred_identifier.get('l', ''),
                    'subject_category': clique_type,
                    'predicate_id': 'skos:exactMatch',
                    'object_id': secondary_identifier['i'],
                    'object_label': secondary_identifier.get('l', ''),
                    'object_category': clique_type,
                    'mapping_justification': 'semapv:MappingChaining',
                })
                count_mappings += 1

    # TODO: add header
    logger.info(f"Converted {compendium_filename} to SSSOM: " +
                f"wrote {count_mappings} mappings to {sssom_filename}")
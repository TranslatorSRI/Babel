# Sapbert (https://github.com/RENCI-NER/sapbert) requires input files
# in a particular pipe-delimited format:
#   biolink:Gene||NCBIGene:10554||AGPAT1||1-acylglycerol-3-phosphate o-acyltransferase 1||lysophosphatidic acid acyltransferase, alpha
# i.e. the format we need is:
#   biolink-type||preferred ID||preferred label||synonym 1||synonym 2
# Also, we can't do more than fifty synonym pairs for each preferred ID.
#
# This file provides code for doing that, based on the code from
# https://github.com/TranslatorSRI/babel-validation/blob/f21b1b308e54ec0af616f2c24f7e2738ac4c261c/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L107-L207
import gzip
import hashlib
import itertools
import json
import os
import random
import re
from itertools import combinations

import logging

from src.util import LoggingUtil, get_config

# Default logger for this file.
logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

# Configuration options
# Should we generate a DrugChemicalSmaller.txt.gz file at all?
GENERATE_DRUG_CHEMICAL_SMALLER_FILE = False
# Limit DrugChemicalSmaller.txt.gz to terms that have a preferred name of 50 characters or more.
DRUG_CHEMICAL_SMALLER_MAX_LABEL_LENGTH = 40
# Include up to 50 synonym pairs for each synonym.
MAX_SYNONYM_PAIRS = 50
# Should we lowercase all the names?
LOWERCASE_ALL_NAMES = True


def convert_synonyms_to_sapbert(synonym_filename_gz, sapbert_filename_gzipped):
    """
    Convert a synonyms file to the training format for SAPBERT (https://github.com/RENCI-NER/sapbert).

    Based on the code in https://github.com/TranslatorSRI/babel-validation/blob/f21b1b308e54ec0af616f2c24f7e2738ac4c261c/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L107-L207

    :param synonym_filename_gz: The compendium file to convert.
    :param sapbert_filename_gzipped: The SAPBERT training file to generate.
    """

    logger.info(f"convert_synonyms_to_sapbert({synonym_filename_gz}, {sapbert_filename_gzipped})")

    # For now, the simplest way to identify the DrugChemicalConflated file is by name.
    # In this case we still generate DrugChemicalConflated.txt, but we also generate
    # DrugChemicalConflatedSmaller.txt, which ignores cliques whose preferred label is
    # longer than config['demote_labels_longer_than'].
    generate_smaller_filename = None
    if GENERATE_DRUG_CHEMICAL_SMALLER_FILE and synonym_filename_gz.endswith('/DrugChemicalConflated.txt.gz'):
        generate_smaller_filename = sapbert_filename_gzipped.replace('.txt.gz', 'Smaller.txt.gz')

    # Make the output directories if they don't exist.
    os.makedirs(os.path.dirname(sapbert_filename_gzipped), exist_ok=True)

    # Open SmallerFile for writing if needed.
    generate_smaller_file = None
    if generate_smaller_filename:
        generate_smaller_file = gzip.open(generate_smaller_filename, 'wt', encoding='utf-8')

    # Go through all the synonyms in the input file.
    count_entry = 0
    count_training_rows = 0
    count_smaller_rows = 0
    with gzip.open(synonym_filename_gz, "rt", encoding="utf-8") as synonymf, gzip.open(sapbert_filename_gzipped, "wt", encoding="utf-8") as sapbertf:
        for input_line in synonymf:
            count_entry += 1
            entry = json.loads(input_line)

            # Read fields from the synonym.
            curie = entry['curie']
            preferred_name = entry.get('preferred_name', '').strip()
            if not preferred_name:
                logging.warning(f"Unable to convert synonym entry for curie {curie}, skipping: {entry}")
                continue

            # Is the preferred name small enough that we should ignore it from generate_smaller_file?
            is_preferred_name_short = (len(preferred_name) <= DRUG_CHEMICAL_SMALLER_MAX_LABEL_LENGTH)
            #if not is_preferred_name_short:
            #    logging.warning(f"CURIE {curie} (preferred name: {preferred_name}) will be excluded from the Smaller training file.")

            # Collect and process the list of names.
            names = entry['names']
            if LOWERCASE_ALL_NAMES:
                names = [name.lower() for name in names]

            # We use '||' as a delimiter, so any occurrences of more than one pipe character
            # should be changed to a single pipe character in the SAPBERT output, so we don't
            # confuse it up with our delimiter.
            names = [re.sub(r'\|\|+', '|', name) for name in names]

            # Figure out the Biolink type to report.
            types = entry['types']
            if len(types) == 0:
                biolink_type = 'NamedThing'
            else:
                biolink_type = types[0]

            # How many names do we have?
            if len(names) == 0:
                # This shouldn't happen, but let's anticipate this anyway.
                line = f"biolink:{biolink_type}||{curie}||{preferred_name}||{preferred_name.lower()}||{preferred_name.lower()}\n"
                sapbertf.write(line)
                count_training_rows += 1
                if generate_smaller_file and is_preferred_name_short:
                    generate_smaller_file.write(line)
                    count_smaller_rows += 1
            elif len(names) == 1:
                # If we have less than two names, we don't have anything to randomize.
                line = f"biolink:{biolink_type}||{curie}||{preferred_name}||{preferred_name.lower()}||{names[0]}\n"
                sapbertf.write(line)
                count_training_rows += 1
                if generate_smaller_file and is_preferred_name_short:
                    generate_smaller_file.write(line)
                    count_smaller_rows += 1
            else:
                name_pairs = list(itertools.combinations(set(names), 2))

                if len(name_pairs) > MAX_SYNONYM_PAIRS:
                    # Randomly select 50 pairs.
                    name_pairs = random.sample(name_pairs, MAX_SYNONYM_PAIRS)

                for name_pair in name_pairs:
                    line = f"biolink:{biolink_type}||{curie}||{preferred_name}||{name_pair[0]}||{name_pair[1]}\n"
                    sapbertf.write(line)
                    count_training_rows += 1

                    # As long as the preferred name is shorter than the right size, we should add this clique to the
                    # smaller file as well.
                    if generate_smaller_file and is_preferred_name_short:
                        generate_smaller_file.write(line)
                        count_smaller_rows += 1

    logger.info(f"Converted {synonym_filename_gz} to SAPBERT training file {synonym_filename_gz}: " +
                f"read {count_entry} entries and wrote out {count_training_rows} training rows.")

    # Close SmallerFile if needed.
    if generate_smaller_file:
        generate_smaller_file.close()
        percentage = count_smaller_rows / float(count_training_rows) * 100
        logger.info(f"Converted {synonym_filename_gz} to smaller SAPBERT training file {generate_smaller_filename}: " +
                    f"read {count_entry} entries and wrote out {count_smaller_rows} training rows ({percentage:.2f}%).")



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
from itertools import combinations

import logging
from src.util import LoggingUtil

# Default logger for this file.
logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

# Configuration options
# Include up to 50 synonym pairs for each synonym.
MAX_SYNONYM_PAIRS = 50


def convert_synonyms_to_sapbert(synonym_filename, sapbert_filename):
    """
    Convert a synonyms file to the training format for SAPBERT (https://github.com/RENCI-NER/sapbert).

    Based on the code in https://github.com/TranslatorSRI/babel-validation/blob/f21b1b308e54ec0af616f2c24f7e2738ac4c261c/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L107-L207

    :param synonym_filename: The compendium file to convert.
    :param sapbert_filename: The SAPBERT training file to generate.
    """

    logger.info(f"convert_synonyms_to_sapbert({synonym_filename}, {sapbert_filename})")

    # Make the output directories if they don't exist.
    os.makedirs(os.path.dirname(sapbert_filename), exist_ok=True)

    # Go through all the synonyms in the input file.
    count_entry = 0
    count_training_text = 0
    with open(synonym_filename, "r", encoding="utf-8") as synonymf, gzip.open(sapbert_filename, "wt", encoding="utf-8") as sapbertf:
        for line in synonymf:
            count_entry += 1
            entry = json.loads(line)

            # Read fields from the synonym.
            curie = entry['curie']
            preferred_name = entry['preferred_name']
            names = entry['names']
            types = entry['types']
            if len(types) == 0:
                biolink_type = 'NamedThing'
            else:
                biolink_type = types[0]

            # How many names do we have?
            if len(names) == 0:
                # This shouldn't happen, but let's anticipate this anyway.
                sapbertf.write(f"biolink:{biolink_type}||{curie}||{preferred_name}||{preferred_name}||{preferred_name}\n")
                count_training_text += 1
            elif len(names) == 1:
                # If we have less than two names, we don't have anything to randomize.
                sapbertf.write(f"biolink:{biolink_type}||{curie}||{preferred_name}||{preferred_name}||{names[0]}\n")
                count_training_text += 1
            else:
                name_pairs = list(itertools.combinations(set(names), 2))

                if len(name_pairs) > MAX_SYNONYM_PAIRS:
                    # Randomly select 50 pairs.
                    name_pairs = random.sample(name_pairs, MAX_SYNONYM_PAIRS)

                for name_pair in name_pairs:
                    sapbertf.write(f"biolink:{biolink_type}||{curie}||{preferred_name}||{name_pair[0]}||{name_pair[1]}\n")
                    count_training_text += 1

    logger.info(f"Converted {synonym_filename} to SAPBERT training file {synonym_filename}: " +
                f"read {count_entry} entries and wrote out {count_training_text} training rows.")
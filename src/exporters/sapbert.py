"""Export a synonyms file as SapBERT training data.

SapBERT (https://github.com/RENCI-NER/sapbert) requires input files in a particular pipe-delimited
format:

    biolink:Gene||NCBIGene:10554||AGPAT1||1-acylglycerol-3-phosphate o-acyltransferase 1||lysophosphatidic acid acyltransferase, alpha

i.e. ``biolink-type||preferred ID||preferred label||synonym 1||synonym 2``, with at most
``MAX_SYNONYM_PAIRS`` rows per preferred ID. Based on
https://github.com/TranslatorSRI/babel-validation/blob/f21b1b308e54ec0af616f2c24f7e2738ac4c261c/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L107-L207

The conversion runs in Rust (``rust/src/sapbert.rs``) when the extension is built, and in Python
otherwise. The Python is the specification: ``tests/exporters/test_sapbert.py`` diffs the two. Set
``BABEL_DISABLE_RUST=1`` to force the Python path (see ``src/accel.py``). The two differ in exactly
two documented ways: the Rust seeds its pair sampling per CURIE so a rerun is byte-identical, and
it writes gzip at flate2's default level rather than zlib level 9.
"""

import gzip
import itertools
import json
import logging
import random
import re

from src.accel import accel
from src.util import LoggingUtil, ensure_parent_dir

# Default logger for this file.
logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

# Configuration options. Both are mirrored as constants in rust/src/sapbert.rs; change both.
# Include up to 50 synonym pairs for each preferred ID -- SapBERT training saturates well before
# every pair of a 200-name clique.
MAX_SYNONYM_PAIRS = 50
# Lowercase all the names, since the model is trained case-insensitively.
LOWERCASE_ALL_NAMES = True


def convert_synonyms_to_sapbert(synonym_filename_gz, sapbert_filename_gzipped):
    """
    Convert a synonyms file to the training format for SAPBERT (https://github.com/RENCI-NER/sapbert).

    :param synonym_filename_gz: The gzipped synonyms JSONL file to convert.
    :param sapbert_filename_gzipped: The gzipped SAPBERT training file to generate.
    """
    logger.info(f"convert_synonyms_to_sapbert({synonym_filename_gz}, {sapbert_filename_gzipped})")
    ensure_parent_dir(sapbert_filename_gzipped)

    if accel is not None:
        counts = accel.convert_synonyms_to_sapbert(synonym_filename_gz, sapbert_filename_gzipped)
    else:
        counts = _convert_synonyms_to_sapbert_python(synonym_filename_gz, sapbert_filename_gzipped)
    count_entry, count_training_rows, count_skipped = counts

    logger.info(
        f"Converted {synonym_filename_gz} to SAPBERT training file {sapbert_filename_gzipped}: "
        f"read {count_entry} entries, wrote out {count_training_rows} training rows, "
        f"skipped {count_skipped} entries with no preferred name."
    )
    return counts


def _convert_synonyms_to_sapbert_python(synonym_filename_gz, sapbert_filename_gzipped):
    """The Python reference implementation. Returns (entries read, rows written, entries skipped)."""
    count_entry = 0
    count_training_rows = 0
    count_skipped = 0
    with (
        gzip.open(synonym_filename_gz, "rt", encoding="utf-8") as synonymf,
        gzip.open(sapbert_filename_gzipped, "wt", encoding="utf-8") as sapbertf,
    ):
        for input_line in synonymf:
            count_entry += 1
            entry = json.loads(input_line)

            # Read fields from the synonym.
            curie = entry["curie"]
            preferred_name = entry.get("preferred_name", "").strip()
            if not preferred_name:
                logger.warning(f"Unable to convert synonym entry for curie {curie}, skipping: {entry}")
                count_skipped += 1
                continue

            # Collect and process the list of names.
            names = entry["names"]
            if LOWERCASE_ALL_NAMES:
                names = [name.lower() for name in names]

            # We use '||' as a delimiter, so any occurrences of more than one pipe character
            # should be changed to a single pipe character in the SAPBERT output, so we don't
            # confuse it up with our delimiter.
            names = [re.sub(r"\|\|+", "|", name) for name in names]

            # Figure out the Biolink type to report.
            types = entry["types"]
            if len(types) == 0:
                biolink_type = "NamedThing"
            else:
                biolink_type = types[0]

            # How many names do we have?
            if len(names) == 0:
                # This shouldn't happen, but let's anticipate this anyway.
                sapbertf.write(
                    f"biolink:{biolink_type}||{curie}||{preferred_name}||{preferred_name.lower()}||{preferred_name.lower()}\n"
                )
                count_training_rows += 1
            elif len(names) == 1:
                # If we have less than two names, we don't have anything to randomize.
                sapbertf.write(
                    f"biolink:{biolink_type}||{curie}||{preferred_name}||{preferred_name.lower()}||{names[0]}\n"
                )
                count_training_rows += 1
            else:
                name_pairs = list(itertools.combinations(set(names), 2))

                if len(name_pairs) > MAX_SYNONYM_PAIRS:
                    # Randomly select 50 pairs.
                    name_pairs = random.sample(name_pairs, MAX_SYNONYM_PAIRS)

                for name_pair in name_pairs:
                    sapbertf.write(
                        f"biolink:{biolink_type}||{curie}||{preferred_name}||{name_pair[0]}||{name_pair[1]}\n"
                    )
                    count_training_rows += 1

    return count_entry, count_training_rows, count_skipped

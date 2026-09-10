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
import itertools
import json
import logging
import random
import re

from src.util import LoggingUtil, ensure_parent_dir

# Default logger for this file.
logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

# Configuration options
# Include up to 50 synonym pairs for each synonym.
MAX_SYNONYM_PAIRS = 50
# When sampling those pairs out of a large clique (see sample_name_pairs()), how many random draws
# are we willing to make per pair we want? Draws that repeat an earlier draw or land on a pair
# already written out don't count towards MAX_SYNONYM_PAIRS, so without a budget a clique whose
# pairs have nearly all been written out already would keep drawing to no purpose. Eight is enough
# that reaching the budget means the clique has very few usable pairs left, not that we were unlucky.
SYNONYM_PAIR_DRAWS_PER_PAIR = 8
# Should we lowercase all the names?
LOWERCASE_ALL_NAMES = True
# There was once a GENERATE_DRUG_CHEMICAL_SMALLER_FILE option here, which wrote a second, smaller
# DrugChemicalConflatedSmaller.txt alongside the full training file by keeping only the shortest
# labels; it was disabled before it was ever used, because both files drew from the one seen_pairs
# set (see convert_synonyms_to_sapbert()) and so the smaller file lost every pair the full file had
# already claimed. Removed in https://github.com/NCATSTranslator/Babel/pull/1084 (issue #1057);
# reviving it means giving each output file its own seen_pairs.


def pair_key(biolink_type, name_pair):
    """
    Return a 64-bit digest identifying a (Biolink type, name, name) triple, independent of the
    order of the two names.

    We remember digests rather than the names themselves because the set of pairs already written
    grows with the size of the output: GeneProteinConflated has hundreds of millions of cliques, so
    holding on to the name strings would need hundreds of gigabytes, while the digests need roughly
    a fifth of that. At 64 bits, the chance of even a single collision across a billion pairs is
    around 3%, and a collision costs us one correct training row.

    :param biolink_type: The Biolink type the pair was generated for (without the `biolink:` prefix).
    :param name_pair: The two names making up this synonym pair.
    :return: A hash of the type and the two names, in a canonical order.
    """
    return hash((biolink_type, *sorted(name_pair)))


def sample_name_pairs(names, max_pairs, biolink_type, seen_pairs):
    """
    Return up to max_pairs distinct pairs of names that have not already been written out, without
    building the full list of pairs unless it is small.

    A clique with n names has n*(n-1)/2 pairs, of which we keep at most max_pairs, so enumerating
    them all is wasted work that grows quadratically with the size of the clique: today's largest
    disease clique has 179 names, but nothing stops a conflated gene/protein clique from having
    thousands. Above a few times max_pairs we therefore draw random pairs directly and reject
    repeats, which needs a number of draws proportional to max_pairs rather than to n.

    Since a drawn pair may turn out to have been written out already, drawing continues past the
    rejected pairs until max_pairs of them survive or we run out of the draw budget (see
    SYNONYM_PAIR_DRAWS_PER_PAIR) -- otherwise a clique overlapping heavily with an earlier one would
    contribute fewer rows than a clique of the same size that happened to come first.

    :param names: The distinct names to pair up, in a stable order.
    :param max_pairs: The largest number of pairs to return.
    :param biolink_type: The Biolink type this clique is being written out as.
    :param seen_pairs: Digests (see pair_key()) of the pairs already written out, which are skipped.
    :return: A list of up to max_pairs (name, name) tuples, each pair appearing at most once.
    """
    count_names = len(names)
    total_pairs = count_names * (count_names - 1) // 2

    # Rejection sampling only pays off when repeat draws are rare, and it slows to a crawl as the
    # number of pairs we want approaches the number that exist. Below that point, enumerate them
    # instead: the list is at most a few times max_pairs long, so it is cheap either way.
    if total_pairs <= 4 * max_pairs:
        name_pairs = [
            name_pair
            for name_pair in itertools.combinations(names, 2)
            if pair_key(biolink_type, name_pair) not in seen_pairs
        ]
        if len(name_pairs) > max_pairs:
            name_pairs = random.sample(name_pairs, max_pairs)
        return name_pairs

    drawn_indices = set()
    name_pairs = []
    for _ in range(SYNONYM_PAIR_DRAWS_PER_PAIR * max_pairs):
        if len(name_pairs) >= max_pairs:
            break
        index_pair = tuple(sorted(random.sample(range(count_names), 2)))
        if index_pair in drawn_indices:
            continue
        drawn_indices.add(index_pair)
        name_pair = (names[index_pair[0]], names[index_pair[1]])
        if pair_key(biolink_type, name_pair) not in seen_pairs:
            name_pairs.append(name_pair)
    return name_pairs


def convert_synonyms_to_sapbert(synonym_filename_gz, sapbert_filename_gzipped):
    """
    Convert a synonyms file to the training format for SAPBERT (https://github.com/RENCI-NER/sapbert).

    Based on the code in https://github.com/TranslatorSRI/babel-validation/blob/f21b1b308e54ec0af616f2c24f7e2738ac4c261c/src/main/scala/org/renci/babel/utils/converter/Converter.scala#L107-L207

    :param synonym_filename_gz: The compendium file to convert.
    :param sapbert_filename_gzipped: The SAPBERT training file to generate.
    """

    logger.info(f"convert_synonyms_to_sapbert({synonym_filename_gz}, {sapbert_filename_gzipped})")

    # Make the output directories if they don't exist.
    ensure_parent_dir(sapbert_filename_gzipped)

    # Go through all the synonyms in the input file.
    count_entry = 0
    count_training_rows = 0
    # Digests (see pair_key()) of the synonym pairs already written out, so that we only write each
    # (Biolink type, name, name) triple once across the entire file. Global to this one output file
    # by design: sharing it with a second output starves that output (see the note on the removed
    # GENERATE_DRUG_CHEMICAL_SMALLER_FILE option at the top of this file).
    seen_pairs = set()
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
                logging.warning(f"Unable to convert synonym entry for curie {curie}, skipping: {entry}")
                continue

            # Collect and process the list of names.
            names = entry.get("names", [])
            # Strip whitespace and drop empty strings if any
            names = [name.strip() for name in names if name.strip()]

            if LOWERCASE_ALL_NAMES:
                names = [name.lower() for name in names]

            # We use '||' as a delimiter, so any occurrences of more than one pipe character
            # should be changed to a single pipe character in the SAPBERT output, so we don't
            # confuse it up with our delimiter.
            names = [re.sub(r"\|\|+", "|", name) for name in names]

            # The preferred name is written out as its own column, so it needs the same pipe cleanup
            # as the names, or a label containing '||' would split into extra columns.
            preferred_name = re.sub(r"\|\|+", "|", preferred_name)

            # Figure out the Biolink type to report.
            types = entry["types"]
            if len(types) == 0:
                biolink_type = "NamedThing"
            else:
                biolink_type = types[0]

            # How many names do we have?
            if len(names) == 0:
                # Not useful for training, so let's skip it.
                continue
            elif len(names) == 1:
                # If we have less than two names, we don't have anything to randomize.
                # Normalize the preferred name the same way the names were normalized above, so the
                # identity check compares like with like whatever LOWERCASE_ALL_NAMES is set to.
                preferred_name_normalized = preferred_name.lower() if LOWERCASE_ALL_NAMES else preferred_name
                if preferred_name_normalized == names[0]:
                    # no need to write the synonym pair if they are identical
                    continue
                name_pair = (preferred_name_normalized, names[0])
                # Skip the pair if we have already written it out for this Biolink type.
                name_pairs = [] if pair_key(biolink_type, name_pair) in seen_pairs else [name_pair]
            else:
                # sample_name_pairs() applies the same already-written check as it draws.
                name_pairs = sample_name_pairs(sorted(set(names)), MAX_SYNONYM_PAIRS, biolink_type, seen_pairs)

            for name_pair in name_pairs:
                seen_pairs.add(pair_key(biolink_type, name_pair))
                line = f"biolink:{biolink_type}||{curie}||{preferred_name}||{name_pair[0]}||{name_pair[1]}\n"
                sapbertf.write(line)
                count_training_rows += 1

    logger.info(
        f"Converted {synonym_filename_gz} to SAPBERT training file {sapbert_filename_gzipped}: "
        + f"read {count_entry} entries and wrote out {count_training_rows} training rows."
    )

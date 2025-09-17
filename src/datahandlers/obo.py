import json
import logging
from pathlib import Path

from src.ubergraph import UberGraph
from src.babel_utils import make_local_name, pull_via_ftp
from collections import defaultdict
import os, gzip
from json import loads,dumps

from src.util import Text, get_config


def pull_uber_icRDF(icrdf_filename):
    """
    Download the icRDF.tsv file that contains normalizedInformationContent for all the entities in UberGraph.
    """
    uber = UberGraph()
    _ = uber.write_normalized_information_content(icrdf_filename)

def pull_uber_labels(outputfile, prefix_labels_files_to_generate):
    """
    Pulls all labels from an UberGraph, organizes them by their IRI prefixes, and writes
    them into a single output file. Note that ALL IRI prefixes will be included in this
    file, whether it's actually used in Babel or not.

    This function retrieves all labels available in the UberGraph resource using the
    `get_all_labels` method. The labels are categorized based on their IRI prefixes,
    skipping certain prefixes like 'http', 'ro', prefixes starting with 't', or those
    containing the '#' character. The categorized labels are then written to the specified
    output file in a tab-delimited format.

    To keep Snakemake working, we need to put labels for some prefixes into their own directories.
    These will be listed as label files to create in prefix_dirs_to_generate.

    :param outputfile: The path to the output file where the categorized labels will be written.
    :type outputfile: str
    :param prefix_labels_files_to_generate: Prefixes we need to create directories for in babel_downloads.
    :type prefix_labels_files_to_generate: list[str]
    :return: None
    """

    # Load all the labels from UberGraph.
    uber = UberGraph()
    labels = uber.get_all_labels()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit['iri']
        p = iri.split(':')[0]
        ldict[p].add( ( unit['iri'], unit['label'] ) )

    # Write out the common output file.
    with open(outputfile, 'w') as outf:
        for p in ldict:
            if p not in ['http','ro'] and not p.startswith('t') and '#' not in p:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\n')

    # Create prefix directories and label files for some prefixes.
    for prefix_labels_file in prefix_labels_files_to_generate:
        prefix_dir = Path(prefix_labels_file).parent
        prefix = prefix_dir.name
        os.makedirs(prefix_dir, exist_ok=True)
        if prefix not in ldict:
            raise ValueError(f'Prefix {prefix} not found in UberGraph labels download.')
        with open(prefix_labels_file, 'w') as outf:
            for unit in ldict[prefix]:
                outf.write(f'{unit[0]}\t{unit[1]}\n')


def pull_uber_descriptions(jsonloutputfile):
    uber = UberGraph()
    descriptions = uber.get_all_descriptions()
    descriptions_by_curie = defaultdict(list)
    for unit in descriptions:
        descriptions_by_curie[unit['iri']].append(unit['description'])

    with open(jsonloutputfile, 'w') as outf:
        for curie in descriptions_by_curie.keys():
            try:
                prefix = Text.get_prefix(curie)
                if prefix not in ['http','ro'] and not prefix.startswith('t') and '#' not in prefix:
                    outf.write(json.dumps({ 'curie': curie, 'descriptions': descriptions_by_curie[curie] }) + '\n')
            except ValueError:
                # Couldn't extract a prefix for this CURIE, so let's ignore it.
                continue

def pull_uber_synonyms(jsonloutputfile, prefix_synonyms_files_to_generate):
    """
    Extracts synonyms from the UberGraph, structures them by identifiers (CURIEs), and writes
    them in JSON Lines format to the specified output file. The function filters synonyms
    based on certain prefix constraints and organizes them by predicates.

    :param jsonloutputfile: File path to write the output in JSON Lines format.
    :type jsonloutputfile: str
    :param prefix_synonyms_files_to_generate: A list of synonyms files to generate for certain prefixes.
    :type prefix_synonyms_files_to_generate: list[str]
    :return: None
    """
    # Load all the synonyms from UberGraph.
    uber = UberGraph()
    synonyms = uber.get_all_synonyms()
    ldict = defaultdict(dict)
    for unit in synonyms:
        curie = unit[0]
        predicate = unit[1]
        synonym = unit[2]
        if predicate not in ldict[curie]:
            ldict[curie][predicate] = []
        ldict[curie][predicate].append(synonym)

    # Write all the synonyms into a common synonym file.
    with open(jsonloutputfile, 'w') as outf:
        for curie in ldict.keys():
            try:
                prefix = Text.get_prefix(curie)
            except ValueError:
                continue

            if prefix not in ['http','ro'] and not prefix.startswith('t') and '#' not in prefix:
                for predicate in ldict[curie].keys():
                    for synonym in ldict[curie][predicate]:
                        outf.write(json.dumps({'curie': curie, 'predicate': predicate, 'synonym': synonym}) + '\n')

    # Create directories and synonyms files for some synonyms.
    for prefix_synonyms_file in prefix_synonyms_files_to_generate:
        prefix_dir = Path(prefix_synonyms_file).parent
        prefix = prefix_dir.name
        os.makedirs(prefix_dir, exist_ok=True)
        with open(prefix_synonyms_file, 'w') as outf:
            if prefix not in ldict:
                logging.warning(f'Prefix {prefix} not found in UberGraph synonyms download.')
                outf.write('')
            for unit in ldict[prefix]:
                outf.write(f'{unit[0]}\t{unit[1]}\n')

def pull_uber(expected_ontologies, icrdf_filename):
    pull_uber_icRDF(icrdf_filename)
    pull_uber_labels(expected_ontologies)
    pull_uber_descriptions(expected_ontologies)
    pull_uber_synonyms(expected_ontologies)


def write_obo_ids(irisandtypes,outfile,order,exclude=[]):
    uber = UberGraph()
    iris_to_types=defaultdict(set)
    for iri,ntype in irisandtypes:
        uberres = uber.get_subclasses_of(iri)
        for k in uberres:
            iris_to_types[k['descendent']].add(ntype)
    excludes = []
    for excluded_iri in exclude:
        excludes += uber.get_subclasses_of(excluded_iri)
    excluded_iris = set( [k['descendent'] for k in excludes ])
    prefix = Text.get_prefix_or_none(iri)
    with open(outfile, 'w') as idfile:
        for kd,typeset in iris_to_types.items():
            if kd not in excluded_iris and kd.startswith(prefix):
                l = list(typeset)
                l.sort(key=lambda k: order.index(k))
                idfile.write(f'{kd}\t{l[0]}\n')

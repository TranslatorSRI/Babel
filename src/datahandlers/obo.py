import json

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

def pull_uber_labels(outputfile):
    uber = UberGraph()
    labels = uber.get_all_labels()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit['iri']
        p = iri.split(':')[0]
        ldict[p].add( ( unit['iri'], unit['label'] ) )

    with open(outputfile, 'w') as outf:
        for p in ldict:
            if p not in ['http','ro'] and not p.startswith('t') and '#' not in p:
                for unit in ldict[p]:
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

def pull_uber_synonyms(jsonloutputfile):
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
    prefix = Text.get_curie(iri)
    with open(outfile, 'w') as idfile:
        for kd,typeset in iris_to_types.items():
            if kd not in excluded_iris and kd.startswith(prefix):
                l = list(typeset)
                l.sort(key=lambda k: order.index(k))
                idfile.write(f'{kd}\t{l[0]}\n')

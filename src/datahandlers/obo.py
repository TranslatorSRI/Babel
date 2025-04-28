from src.ubergraph import UberGraph
from src.babel_utils import make_local_name, pull_via_ftp
from src.node import get_config
from collections import defaultdict
import os, gzip
from json import loads,dumps

from src.util import Text

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

def pull_uber_descriptions(outputfile):
    uber = UberGraph()
    labels = uber.get_all_descriptions()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit['iri']
        p = iri.split(':')[0]
        ldict[p].add( ( unit['iri'], unit['description'] ) )

    with open(outputfile, 'w') as outf:
        for p in ldict:
            if p not in ['http','ro'] and not p.startswith('t') and '#' not in p:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\n')


def pull_uber_synonyms(outputfile):
    uber = UberGraph()
    labels = uber.get_all_synonyms()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit[0]
        p = iri.split(':')[0]
        ldict[p].add(  unit )

    with open(outputfile, 'w') as outf:
        for p in ldict:
            if p not in ['http','ro'] and not p.startswith('t') and '#' not in p:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\t{unit[2]}\n')

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

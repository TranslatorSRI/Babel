from src.ubergraph import UberGraph
from src.babel_utils import make_local_name, pull_via_ftp
from collections import defaultdict
import os, gzip
from json import loads,dumps

def pull_uber_labels(expected):
    uber = UberGraph()
    labels = uber.get_all_labels()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit['iri']
        p = iri.split(':')[0]
        ldict[p].add( ( unit['iri'], unit['label'] ) )
    for p in ldict:
        if p not in ['http','ro'] and not p.startswith('t') and not '#' in p:
            fname = make_local_name('labels',subpath=p)
            with open(fname,'w') as outf:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\n')

def pull_uber_synonyms(expected):
    uber = UberGraph()
    labels = uber.get_all_synonyms()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit[0]
        p = iri.split(':')[0]
        ldict[p].add(  unit )
    #There are some of the ontologies that we don't get synonyms for.   But this makes snakemake unhappy so
    # we are going to make some zero-length files for it
    for p in expected:
        if p not in ['http','ro'] and not p.startswith('t') and not '#' in p:
            fname = make_local_name('synonyms',subpath=p)
            with open(fname,'w') as outf:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\t{unit[2]}\n')

def pull_uber(expected_ontologies):
    pull_uber_labels(expected_ontologies)
    pull_uber_synonyms(expected_ontologies)


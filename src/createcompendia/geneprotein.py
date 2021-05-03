from src.prefixes import ENSEMBL, PR, UNIPROTKB, NCBIGENE
from src.categories import PROTEIN

from src.babel_utils import read_identifier_file,glom,write_compendium,Text

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def build_uniprotkb_ncbigene_relationships(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split()
            if x[1] == 'GeneID':
                uniprot_id = f'{UNIPROTKB}:{x[0]}'
                ncbigene_id = f'{NCBIGENE}:{x[2]}'
                outf.write(f'{uniprot_id}\trelated_to\t{ncbigene_id}\n')

def build_compendium(concordances, identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    uniques = [NCBIGENE]
    for ifile in identifiers:
        print(ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes= uniques)
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print('loading', infile)
        pairs = []
        with open(infile, 'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append(set([x[0], x[2]]))
        glom(dicts, pairs, unique_prefixes=uniques)
    gene_sets = set([frozenset(x) for x in dicts.values()])
    baretype = PROTEIN.split(':')[-1]
    write_compendium(gene_sets, f'{baretype}.txt', PROTEIN, {})


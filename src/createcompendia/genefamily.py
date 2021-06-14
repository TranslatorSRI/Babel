from src.categories import GENE_FAMILY

from src.babel_utils import read_identifier_file,glom,write_compendium

def build_compendia(identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    uniques = []
    for ifile in identifiers:
        print('loading',ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes= uniques)
        types.update(new_types)
    genefam_sets = set([frozenset(x) for x in dicts.values()])
    baretype = GENE_FAMILY.split(':')[-1]
    write_compendium(genefam_sets, f'{baretype}.txt', GENE_FAMILY, {})


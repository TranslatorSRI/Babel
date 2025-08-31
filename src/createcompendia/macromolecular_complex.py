from src.prefixes import COMPLEXPORTAL
from src.categories import MACROMOLECULAR_COMPLEX

import src.datahandlers.complexportal as complexportal
from src.babel_utils import read_identifier_file, glom, write_compendium

def build_compendia(identifiers, metadata_yamls, icrdf_filename):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    for ifile in identifiers:
        print('loading ', ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers)
        types.update(new_types)
    sets = set([frozenset(x) for x in dicts.values()])
    type = MACROMOLECULAR_COMPLEX.split(':')[-1]
    write_compendium(metadata_yamls, sets, f'{type}.txt', MACROMOLECULAR_COMPLEX, {}, extra_prefixes=[COMPLEXPORTAL], icrdf_filename=icrdf_filename)

from src.categories import CELL_LINE

from src.babel_utils import read_identifier_file,glom,write_compendium
from src.introspection import compendium_builder


@compendium_builder
def build_compendia(ifile, icrdf_filename):
    """:identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    uniques = []
    print('loading',ifile)
    new_identifiers, new_types = read_identifier_file(ifile)
    glom(dicts, new_identifiers, unique_prefixes= uniques)
    types.update(new_types)
    idsets = set([frozenset(x) for x in dicts.values()])
    baretype = CELL_LINE.split(':')[-1]
    write_compendium(idsets, f'{baretype}.txt', CELL_LINE, {}, icrdf_filename=icrdf_filename)


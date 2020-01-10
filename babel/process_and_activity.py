from babel.ubergraph import UberGraph
from src.LabeledID import LabeledID
from src.util import Text
from babel.babel_utils import write_compendium,glom,get_prefixes

def build_sets(iri, ignore_list = ['PMID']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    results = []
    for k,v in uberres.items():
        #if k[1] is not None and k[1].startswith('obsolete'):
        #    continue
        dbx = set([ x for x in v if not Text.get_curie(x) in ignore_list ])
        dbx.add(LabeledID(identifier=k[0],label=k[1]))
        results.append(dbx)
    return results

def load_one(starter,stype):
    sets = build_sets(starter)
    relabel_entities(sets)
    dicts = {}
    glom(dicts, sets)
    osets = set([frozenset(x) for x in dicts.values()])
    write_compendium(osets,f'{stype}.txt',stype)

def load():
    load_one('GO:0008150','biological_process')
    load_one('GO:0003674','molecular_activity')


def relabel_entities(sets):
    curie_to_labeledID = {}
    for s in sets:
        for si in s:
            if isinstance(si,LabeledID):
                curie_to_labeledID[si.identifier] = si
    for s in sets:
        for si in s:
            if si in curie_to_labeledID:
                s.remove(si)
                s.add(curie_to_labeledID[si])

if __name__ == '__main__':
    load()

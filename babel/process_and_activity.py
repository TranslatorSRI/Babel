from babel.ubergraph import UberGraph
from src.LabeledID import LabeledID
from src.util import Text
from babel.babel_utils import write_compendium,glom,get_prefixes

def build_sets(iri, ignore_list = ['PMID','EC']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    results = []
    for k,v in uberres.items():
        if k[0] == 'GO:0052859':
            print('hi')
        #if k[1] is not None and k[1].startswith('obsolete'):
        #    continue
        dbx = set([ x for x in v if not Text.get_curie(x) in ignore_list ])
        dbx.add(LabeledID(identifier=k[0],label=k[1]))
        results.append(dbx)
        if k[0] == 'GO:0052859':
            print(results[-1])
    return results

def load_one(starter,stype):
    sets = build_sets(starter)
    print(len(sets))
    relabel_entities(sets)
    print(len(sets))
    dicts = {}
    glom(dicts, sets)
    print(len(dicts))
    for k,v in dicts.items():
        if 'GO:0052859' == k[0]:
            print(k,v)
    osets = set([frozenset(x) for x in dicts.values()])
    print(len(osets))
    write_compendium(osets,f'{stype}.txt',stype)

def load():
    load_one('GO:0003674','molecular_activity')
    #load_one('GO:0008150','biological_process')


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

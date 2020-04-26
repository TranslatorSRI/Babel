from babel.ubergraph import UberGraph
#from src.LabeledID import LabeledID
from src.util import Text
from babel.babel_utils import write_compendium,glom,get_prefixes,clean_sets
from collections import defaultdict

def build_sets(iri, ignore_list = ['PMID','EC']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    results = []
    uberres = clean_sets(uberres)
    labels = {}
    for k,v in uberres.items():
        dbx = set([ x for x in v if not Text.get_curie(x) in ignore_list ])
        #prefixes = set([Text.get_curie(x) for x in dbx])
        #print(prefixes)
        dbx.add(k[0])
        results.append(dbx)
        labels[k[0]] = k[1]
    return results,labels

def load_one(starter,stype):
    sets,labels = build_sets(starter)
    #relabel_entities(sets)
    dicts = {}
    glom(dicts, sets,unique_prefixes=['GO'])
    osets = set([frozenset(x) for x in dicts.values()])
    write_compendium(osets,f'{stype}.txt',stype,labels=labels)

def load():
    load_one('GO:0003674','molecular_activity')
    load_one('GO:0008150','biological_process')


#def relabel_entities(sets):
#    curie_to_labeledID = {}
#    for s in sets:
#        for si in s:
#            if isinstance(si,LabeledID):
#                curie_to_labeledID[si.identifier] = si
#    for s in sets:
#        for si in s:
#            if si in curie_to_labeledID:
#                s.remove(si)
#                s.add(curie_to_labeledID[si])

if __name__ == '__main__':
    load()

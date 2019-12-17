from babel.babel_utils import glom, write_compendium
from babel.onto import Onto
from src.LabeledID import LabeledID
from src.util import Text
import os
from datetime import datetime as dt
from functools import reduce

def write_sets(sets,fname):
    with open(fname,'w') as outf:
        for s in sets:
            outf.write(f'{s}\n')

def write_dicts(dicts,fname):
    with open(fname,'w') as outf:
        for k in dicts:
            outf.write(f'{k}\t{dicts[k]}\n')

def filter_out_non_unique_ids(old_list):
    """
    filters out elements that exist accross rows 
    eg input [{'z', 'x', 'y'}, {'z', 'n', 'm'}] 
    output [{'x', 'y'}, {'m', 'n'}]    
    """
    bad_ids = []
    for index, terms in enumerate(old_list):                
        for other_terms in old_list[index +1:]:
            for term in terms:                
                if term not in bad_ids and term in other_terms:
                    bad_ids.append(term)
                    continue
    new_list = list(map(
        lambda term_list : \
        set(
            filter(
                lambda term: term not in bad_ids, 
                term_list                
            )), old_list))
    return new_list        

def load_diseases_and_phenotypes():
    print('disease/phenotype')
    print('get and write mondo sets')
    mondo_sets = build_exact_sets('MONDO')
    write_sets(mondo_sets,'mondo_sets.txt')
    print('get and write hp sets')
    hpo_sets = build_sets('HPO', ignore_list = ['ICD','NCIT'])
    hpo_sets = filter_out_non_unique_ids(hpo_sets)
    write_sets(hpo_sets,'hpo_sets.txt')
    print('get and write umls sets')
    meddra_umls = read_meddra()
    write_sets(hpo_sets,'meddra_umls_sets.txt')
    dicts = {}
    print('put it all together')
    glom(dicts,mondo_sets)
    write_dicts(dicts,'mondo_dicts.txt')
    glom(dicts,hpo_sets)
    write_dicts(dicts,'mondo_hpo_dicts.txt')
    glom(dicts,meddra_umls)
    write_dicts(dicts,'mondo_hpo_meddra_dicts.txt')
    print('dump it')
    write_compendium(set([frozenset(x) for x in dicts.values()]),'disease_phenotype.txt','disease_or_phenotypic_feature')

def build_exact_sets(ontoname):
    onto = Onto()
    sets = []
    mids = onto.get_ids(ontoname)
    n = 0
    now = dt.now()
    for mid in mids:
        if n % 100 == 0 and n > 0:
            later = dt.now()
            delt = (later-now).seconds
            f = n / len(mids)
            print(f'{n}/{len(mids)} = {f} in {delt} s')
            print(f'  estimated time remaining = {delt * (1-f)/(f)}')
        #FWIW, ICD codes tend to be mapped to multiple MONDO identifiers, leading to mass confusion. So we
        #just excise them here.  It's possible that we'll want to revisit this decision in the future.  If so,
        #then we probably will want to set a 'glommable' and 'not glommable' set.
        dbx = [ Text.upper_curie(x) for x in onto.get_exact_matches(mid) ]
        dbx = set( filter( lambda x: not x.startswith('ICD'), dbx ) )
        label = onto.get_label(mid)
        mid = Text.upper_curie(mid)
        dbx.add(LabeledID(mid,label))
        sets.append(dbx)
        n += 1
    return sets


def norm(curie):
    if Text.get_curie(curie) == 'MSH':
        return f'MESH:{Text.un_curie(curie)}'
    if Text.get_curie(curie) == 'SNOMEDCT_US':
        return f'SNOMEDCT:{Text.un_curie(curie)}'
    return curie

def build_sets(ontology_name, ignore_list = ['ICD']):
    onto = Onto()
    sets = []
    mids = onto.get_ids(ontology_name)
    for mid in mids:
        #FWIW, ICD codes tend to be mapped to multiple MONDO identifiers, leading to mass confusion. So we
        #just excise them here.  It's possible that we'll want to revisit this decision in the future.  If so,
        #then we probably will want to set a 'glommable' and 'not glommable' set.
        dbx = set([Text.upper_curie(x) for x in onto.get_xrefs(mid) if not reduce(lambda accumlator, ignore_prefix: accumlator or x.startswith(ignore_prefix) , ignore_list, False)])
        dbx = set([norm(x) for x in dbx])
        label = onto.get_label(mid)
        mid = Text.upper_curie(mid)
        dbx.add(LabeledID(mid,label))
        sets.append(dbx)
    return sets

#THIS is bad.
# We can't distribute MRCONSO.RRF, and dragging it out of UMLS is a manual process.
# It's possible we could rebuild using the services, but no doubt very slowly
def read_meddra():
    pairs = []
    mrcon = os.path.join(os.path.dirname(__file__),'input_data', 'MRCONSO.RRF')
    with open(mrcon,'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            if x[1] != 'ENG':
                continue
            pairs.append( (f'UMLS:{x[0]}',f'MEDDRA:{x[13]}'))
    return pairs

if __name__ == '__main__':
    load_diseases_and_phenotypes()
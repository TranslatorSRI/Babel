from babel.babel_utils import glom, write_compendium
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

def load_diseases_and_phenotypes(rosetta):
    print('disease/phenotype')
    print('get and write mondo sets')
    mondo_sets = build_exact_sets(rosetta.core.mondo,rosetta.core.uberongraph)
    write_sets(mondo_sets,'mondo_sets.txt')
    print('get and write hp sets')
    hpo_sets = build_sets(rosetta.core.hpo, ignore_list = ['ICD','NCIT'])
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
    with open('disease.txt','w') as outf:
        write_compendium(dicts,rosetta,outf)

def build_exact_sets(o,u):
    sets = []
    mids = o.get_ids()
    print(len(mids))
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
        print(mid)
        dbx = [ Text.upper_curie(x) for x in o.get_exact_matches(mid) ]
        print(dbx)
        dbx = set( filter( lambda x: not x.startswith('ICD'), dbx ) )
        label = u.get_label(mid)
        print(label)
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

def build_sets(o, ignore_list = ['ICD']):
    sets = []
    mids = o.get_ids()
    for mid in mids:
        #FWIW, ICD codes tend to be mapped to multiple MONDO identifiers, leading to mass confusion. So we
        #just excise them here.  It's possible that we'll want to revisit this decision in the future.  If so,
        #then we probably will want to set a 'glommable' and 'not glommable' set.
        dbx = set([Text.upper_curie(x) for x in o.get_xrefs(mid) if not reduce(lambda accumlator, ignore_prefix: accumlator or x.startswith(ignore_prefix) , ignore_list, False)])
        dbx = set([norm(x) for x in dbx])
        label = o.get_label(mid)
        mid = Text.upper_curie(mid)
        dbx.add(LabeledID(mid,label))
        sets.append(dbx)
    return sets

#THIS is bad.
# We can't distribute MRCONSO.RRF, and dragging it out of UMLS is a manual process.
# It's possible we could rebuild using the services, but no doubt very slowly
def read_meddra():
    pairs = []
    mrcon = os.path.join(os.path.dirname(__file__), 'MRCONSO.RRF')
    with open(mrcon,'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            if x[1] != 'ENG':
                continue
            pairs.append( (f'UMLS:{x[0]}',f'MEDDRA:{x[13]}'))
    return pairs

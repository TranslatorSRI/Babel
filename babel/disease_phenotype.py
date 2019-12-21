from babel.babel_utils import glom, write_compendium,dump_sets,dump_dicts,get_prefixes
from babel.onto import Onto
from babel.ubergraph import UberGraph
from src.LabeledID import LabeledID
from src.util import Text
import os
from datetime import datetime as dt
from functools import reduce

#def write_dicts(dicts,fname):
#    with open(fname,'w') as outf:
#        for k in dicts:
#            outf.write(f'{k}\t{dicts[k]}\n')

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
    print('get and write hp sets')
    hpo_sets = build_sets('HP:0000118', ignore_list = ['ICD','NCIT'])
    print('filter')
    hpo_sets = filter_out_non_unique_ids(hpo_sets)
    print('ok')
    dump_sets(hpo_sets,'hpo_sets.txt')
    print('get and write mondo sets')
    mondo_sets = build_exact_sets('MONDO:0000001')
    dump_sets(mondo_sets,'mondo_sets.txt')
    print('get and write umls sets')
    meddra_umls = read_meddra()
    dump_sets(hpo_sets,'meddra_umls_sets.txt')
    dicts = {}
    print('put it all together')
    glom(dicts,mondo_sets)
    dump_dicts(dicts,'mondo_dicts.txt')
    glom(dicts,hpo_sets)
    dump_dicts(dicts,'mondo_hpo_dicts.txt')
    glom(dicts,meddra_umls)
    dump_dicts(dicts,'mondo_hpo_meddra_dicts.txt')
    print('dump it')
    diseases,phenotypes = create_typed_sets(set([frozenset(x) for x in dicts.values()]))
    write_compendium(diseases,'disease.txt','disease')
    write_compendium(phenotypes,'phenotypes.txt','phenotypic_feature')


def create_typed_sets(eqsets):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has a mondo, it's a disease, no matter what else it is
    If it doesn't have a mondo, but it does have an HP, then it's a phenotype
    Otherwise, consult the UMLS to see what it might be
    """
    umls_types = read_umls_types()
    diseases = set()
    phenotypic_features = set()
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        if 'MONDO' in prefixes:
            diseases.add(equivalent_ids)
        elif 'HP' in prefixes:
            phenotypic_features.add(equivalent_ids)
        elif 'UMLS' in prefixes:
            umls_ids = [ Text.un_curie(x) for x in equivalent_ids if Text.get_curie(x) == 'UMLS']
            if len(umls_ids) > 1:
                print(umls_ids)
            try:
                semtype = umls_types[umls_ids[0]]
                if semtype in ['Disease or Syndrome','Neoplastic Process','Injury or Poisoning',
                               'Mental or Behavioral Dysfunction','Congenital Abnormality',
                               'Anatomical Abnormality']:
                    diseases.add(equivalent_ids)
                elif semtype in ['Finding', 'Pathologic Function', 'Sign or Symptom', 'Acquired Abnormality']:
                    phenotypic_features.add(equivalent_ids)
                else:
                    #Therapeutic or Preventive Procedure, Laboratory Procedure,Laboratory or Test Result
                    #Diagnostic Procedure
                    #print(semtype,umls_ids[0])
                    pass
            except Exception as e:
                print(f'Missing UMLS: {umls_ids[0]}')
                print(equivalent_ids)
    return diseases, phenotypic_features

def build_exact_sets(iri):
    uber = UberGraph()
    uberres = uber.get_subclasses_and_exacts(iri)
    results = []
    for k,v in uberres.items():
        if k[1] is not None and k[1].startswith('obsolete'):
            continue
        dbx = set([ norm(x) for x in v  ])
        dbx.add(LabeledID(identifier=k[0],label=k[1]))
        results.append(dbx)
    return results

def xbuild_exact_sets(ontoname):
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
        ems = onto.get_exact_matches(mid)
        eq_cures = []
        for em in ems:
            if em.startswith('http://'):
                eq_cures.append(Text.obo_to_curie(em))
            else:
                eq_cures.append(em)
        dbx = [ Text.upper_curie(x) for x in eq_cures ]
        dbx = set( filter( lambda x: not x.startswith('ICD'), dbx ) )
        label = onto.get_label(mid)
        mid = Text.upper_curie(mid)
        dbx.add(LabeledID(mid,label))
        sets.append(dbx)
        n += 1
    return sets


def norm(curie):
    curie = f'{Text.get_curie(curie).upper()}:{Text.un_curie(curie)}'
    if Text.get_curie(curie) == 'MSH':
        return f'MESH:{Text.un_curie(curie)}'
    if Text.get_curie(curie) == 'SNOMEDCT_US':
        return f'SNOMEDCT:{Text.un_curie(curie)}'
    return curie

def build_sets(iri, ignore_list = ['ICD']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    results = []
    for k,v in uberres.items():
        if k[1] is not None and k[1].startswith('obsolete'):
            continue
        dbx = set([ norm(x) for x in v if not Text.get_curie(x) in ignore_list ])
        dbx.add(LabeledID(identifier=k[0],label=k[1]))
        results.append(dbx)
    return results

def xbuild_sets(ontology_name, ignore_list = ['ICD']):
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

def read_umls_types():
    types = {}
    mrsty = os.path.join(os.path.dirname(__file__),'input_data','MRSTY.RRF')
    with open(mrsty,'r') as inf:
        for line in inf:
            x = line.split('|')
            types[x[0]] = x[3]
    return types

if __name__ == '__main__':
    load_diseases_and_phenotypes()
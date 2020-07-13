from babel.babel_utils import glom, write_compendium, dump_sets, dump_dicts, get_prefixes, filter_out_non_unique_ids, clean_sets
from babel.onto import Onto
from babel.ubergraph import UberGraph
from src.util import Text
import os
from datetime import datetime as dt
from functools import reduce
from ast import literal_eval
from collections import defaultdict

#def write_dicts(dicts,fname):
#    with open(fname,'w') as outf:
#        for k in dicts:
#            outf.write(f'{k}\t{dicts[k]}\n')

def read_bad_hp_mappings():
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)),'input_data','hpo_errors.txt')
    drops = defaultdict(set)
    with open(fn,'r') as infile:
        for line in infile:
            if line.startswith('-'):
                continue
            x = line.strip().split('\t')
            hps = x[0]
            commaindex = hps.index(',')
            curie = hps[1:commaindex]
            name = hps[commaindex+1:-1]
            badset = literal_eval(x[1])
            drops[curie].update(badset)
    return drops

def filter_umls(umls_pairs,sets_with_umls):
    # We've got a bunch of umls pairs, but we really only want to use them if they're not
    # already BOTH attached to a hp or mondo.
    #for s in sets_with_umls:
    #    if 'UMLS:C2931082' in s:
    #        print(s)
    #    if 'MESH:C536004' in s:
    #        print(s)
    with open('filtered.txt','w') as ff:
        used = set()
        for s in sets_with_umls:
            used.update(s)
        #if 'UMLS:C2931082' in used:
        #    print('umls in used')
        #if 'MESH:C536004' in used:
        #    print('mesh in used')
        ok_pairs = []
        for pair in umls_pairs:
            p=list(pair)
        #    print(p[0])
        #    print(p[1])
            ok = ((p[0] not in used) or (p[1] not in used))
            if ok:
                ok_pairs.append(pair)
            else:
                ff.write(f'{p[0]}\t{p[1]}\n')
        return ok_pairs

def combine_id_sets(l1,l2):
    """Given lists of sets, combine them, overlapping sets that are exactly the same"""
    #print(l1[0])
    #print(type(l1[0]))
    #print(l2[0])
    #print(type(l2[0]))
    s = set( [frozenset(x) for x in l1])
    s2 = set( [frozenset(x) for x in l2])
    s.update(s2)
    return [ set(x) for x in s ]

def read_badxrefs(pref):
    morebad = defaultdict(list)
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)),'input_data',f'{pref}_badxrefs.txt')
    with open(fn,'r') as inf:
        for line in inf:
            if line.startswith('#'):
                continue
            x = line.strip().split(' ')
            morebad[x[0]].append(x[1])
    return morebad

def load_diseases_and_phenotypes():
    print('disease/phenotype')
    print('get and write hp sets')
    bad_mappings = read_bad_hp_mappings()
    more_bad_mappings = read_badxrefs('hpo')
    for h,m in more_bad_mappings.items():
        bad_mappings[h].update(m)
    hpo_sets,labels = build_sets('HP:0000118', ignore_list = ['ICD','NCIT'], bad_mappings = bad_mappings)
    print('filter')
    hpo_sets = filter_out_non_unique_ids(hpo_sets)
    print('ok')
    dump_sets(hpo_sets,'hpo_sets.txt')
    print('get and write mondo sets')
    #MONDO has disease, and its sister disease susceptibility.  I'm putting both in disease.  Biolink q
    #But! this is a problem right now because there are some things that go in both, and they are getting filtered out
    bad_mondo_mappings = read_badxrefs('mondo')
    mondo_sets_1,labels_1 = build_exact_sets('MONDO:0000001',bad_mondo_mappings)
    mondo_sets_2,labels_2 = build_exact_sets('MONDO:0042489',bad_mondo_mappings)
    mondo_close = get_close_matches('MONDO:0000001')
    mondo_close2 = get_close_matches('MONDO:0042489')
    for k,v in mondo_close2.items():
        mondo_close[k] = v
    dump_sets(mondo_sets_1,'mondo1.txt')
    dump_sets(mondo_sets_2,'mondo2.txt')
    labels.update(labels_1)
    labels.update(labels_2)
    #if we just add these together, then any mondo in both lists will get filtered out in the next step.
    #so we need to put them into a set.  You can't put sets directly into a set, you have to freeze them first
    mondo_sets = combine_id_sets(mondo_sets_1,mondo_sets_2)
    mondo_sets = filter_out_non_unique_ids(mondo_sets)
    dump_sets(mondo_sets,'mondo_sets.txt')
    print('get and write umls sets')
    bad_umls = read_badxrefs('umls')
    meddra_umls = read_meddra(bad_umls)
    meddra_umls = filter_umls(meddra_umls,mondo_sets+hpo_sets)
    dump_sets(meddra_umls,'meddra_umls_sets.txt')
    dicts = {}
    #EFO has 3 parts that we want here:
    # Disease
    efo_sets_1,l = build_exact_sets('EFO:0000408')
    labels.update(l)
    #phenotype
    efo_sets_2,l = build_exact_sets('EFO:0000651')
    labels.update(l)
    #measurement
    efo_sets_3,l = build_exact_sets('EFO:0001444')
    labels.update(l)
    efo_sets_a = combine_id_sets(efo_sets_1,efo_sets_2)
    efo_sets = combine_id_sets(efo_sets_a, efo_sets_3)
    efo_sets = filter_out_non_unique_ids(efo_sets)
    dump_sets(efo_sets,'efo_sets.txt')
    print('put it all together')
    print('mondo')
    glom(dicts,mondo_sets,unique_prefixes=['MONDO'])
    dump_dicts(dicts,'mondo_dicts.txt')
    print('hpo')
    glom(dicts,hpo_sets,unique_prefixes=['MONDO'],pref='HP')
    dump_dicts(dicts,'mondo_hpo_dicts.txt')
    print('umls')
    glom(dicts,meddra_umls,unique_prefixes=['MONDO'],pref='UMLS',close={'MONDO':mondo_close})
    dump_dicts(dicts,'mondo_hpo_meddra_dicts.txt')
    print('efo')
    glom(dicts,efo_sets,unique_prefixes=['MONDO'],pref='EFO')
    dump_dicts(dicts,'mondo_hpo_meddra_efo_dicts.txt')
    print('dump it')
    diseases,phenotypes = create_typed_sets(set([frozenset(x) for x in dicts.values()]))
    write_compendium(diseases,'disease.txt','disease',labels)
    write_compendium(phenotypes,'phenotypes.txt','phenotypic_feature',labels)


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
    unknown_types = set()
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        if 'MONDO' in prefixes:
            diseases.add(equivalent_ids)
        elif 'HP' in prefixes:
            phenotypic_features.add(equivalent_ids)
        elif 'UMLS' in prefixes:
            umls_ids = [ Text.un_curie(x) for x in equivalent_ids if Text.get_curie(x) == 'UMLS']
            #if len(umls_ids) > 1:
            #    print(umls_ids)
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
                    if semtype not in unknown_types:
                        #print('What is this UMLS type?')
                        #print(semtype,umls_ids[0])
                        unknown_types.add(semtype)
                    pass
            except Exception as e:
                #print(f'Missing UMLS: {umls_ids[0]}')
                #print(equivalent_ids)
                #Calling it a phenotype
                phenotypic_features.add(equivalent_ids)
        elif 'EFO' in prefixes:
            phenotypic_features.add(equivalent_ids)
        #else:
        #    print(prefixes)
    return diseases, phenotypic_features

def build_exact_sets(iri,bad_mappings = defaultdict(set)):
    prefix = Text.get_curie(iri)
    uber = UberGraph()
    uberres = uber.get_subclasses_and_exacts(iri)
    results = []
    labels = {}
    for k,v in uberres.items():
        #Don't hop ontologies here.
        subclass_prefix = Text.get_curie(k[0])
        if subclass_prefix != prefix:
            continue
        if k[1] is not None and k[1].startswith('obsolete'):
            continue
        dbx = set([ norm(x) for x in v ])
        for bm in bad_mappings[k[0]]:
            if bm in dbx:
                dbx.remove(bm)
        dbx.add(k[0])
        labels[k[0]] = k[1]
        results.append(dbx)
    return results,labels

def get_close_matches(iri):
    prefix = Text.get_curie(iri)
    uber = UberGraph()
    uberres = uber.get_subclasses_and_close(iri)
    close = {}
    for k,v in uberres.items():
        #Don't hop ontologies here.
        subclass_prefix = Text.get_curie(k[0])
        if subclass_prefix != prefix:
            continue
        if k[1] is not None and k[1].startswith('obsolete'):
            continue
        dbx = set([ norm(x) for x in v ])
        close[k[0]] = dbx
    return close


def norm(curie):
    curie = f'{Text.get_curie(curie).upper()}:{Text.un_curie(curie)}'
    if Text.get_curie(curie) == 'MSH':
        return Text.recurie(curie,'MESH')
    if Text.get_curie(curie) in ['SNOMEDCT_US','SCTID']:
        return Text.recurie(curie,'SNOMEDCT')
    return curie

def build_sets(iri, ignore_list = ['ICD'], bad_mappings = {}):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    results = []
    labels = {}
    for k,v in uberres.items():
        if k[1] is not None and k[1].startswith('obsolete'):
            continue
        dbx = set([ norm(x) for x in v if not Text.get_curie(x) in ignore_list ])
        labels[k[0]] = k[1]
        head = k[0]
        dbx.add(head)
        bad_guys = bad_mappings[head]
        dbx.difference_update(bad_guys)
        results.append(dbx)
    return results,labels


#THIS is bad.
# We can't distribute MRCONSO.RRF, and dragging it out of UMLS is a manual process.
# It's possible we could rebuild using the services, but no doubt very slowly
def read_meddra(bad_maps):
    pairs = set()
    mrcon = os.path.join(os.path.dirname(__file__),'input_data', 'MRCONSO.RRF')
    nothandled = set()
    with open(mrcon,'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            if x[1] != 'ENG':
                continue
            if x[2] == 'S':
                continue
            #There is a suppress column.  Only go forward if it is  'N' (it can be 'O', 'E', 'Y', all mean suppress)
            if x[16] != 'N':
                continue
            oid = x[10]
            if oid == '':
                oid = x[9]
            source = x[11]
            if source == 'HPO':
                otherid = oid
            elif source == 'MDR':
                otherid = f'MEDDRA:{oid}'
            elif source == 'NCI':
                otherid = f'NCIT:{oid}'
            elif source == 'SNOMEDCT_US':
                otherid = f'SNOMEDCT:{oid}'
            elif source == 'MSH':
                otherid = f'MESH:{oid}'
            elif source in ['LNC','SRC']:
                continue
            else:
                if source not in nothandled:
                    #print('not handling source:',source)
                    nothandled.add(source)
                continue
            uid = f'UMLS:{x[0]}'
            if uid in bad_maps and otherid == bad_maps[uid]:
                continue
            pairs.add( frozenset({uid,otherid}) )
    return list(pairs)

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

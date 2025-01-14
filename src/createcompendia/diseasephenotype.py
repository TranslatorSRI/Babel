from os import path
from collections import defaultdict

import src.datahandlers.obo as obo

from src.prefixes import MESH, NCIT, MONDO, OMIM, HP, SNOMEDCT, MEDDRA, EFO, ORPHANET, ICD0, ICD9, ICD10, UMLS, KEGGDISEASE
from src.categories import DISEASE, PHENOTYPIC_FEATURE
from src.ubergraph import build_sets
import src.datahandlers.umls as umls
import src.datahandlers.doid as doid
import src.datahandlers.mesh as mesh
import src.datahandlers.efo as efo

from src.babel_utils import read_identifier_file, glom, remove_overused_xrefs, get_prefixes, write_compendium

def write_obo_ids(irisandtypes,outfile,exclude=[]):
    order = [DISEASE, PHENOTYPIC_FEATURE]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])

def write_ncit_ids(outfile):
    disease_id = f'{NCIT}:C2991'
    phenotypic_feature_id = f'{NCIT}:C3367'
    write_obo_ids([(disease_id, DISEASE), (phenotypic_feature_id, PHENOTYPIC_FEATURE)], outfile, exclude=[])

def write_mondo_ids(outfile):
    disease_id = f'{MONDO}:0000001'
    disease_sus_id = f'{MONDO}:0042489'
    write_obo_ids([(disease_id, DISEASE),(disease_sus_id,DISEASE)],outfile)

def write_efo_ids(outfile):
    disease_id='EFO:0000408'
    phenotype_id='EFO:0000651'
    measurement_id='EFO:0001444'
    efos = [(disease_id, DISEASE), (phenotype_id, PHENOTYPIC_FEATURE), (measurement_id, PHENOTYPIC_FEATURE)]
    efo.make_ids(efos,outfile)

def write_hp_ids(outfile):
    #Phenotype
    phenotype_id = 'HP:0000118'
    write_obo_ids([(phenotype_id,PHENOTYPIC_FEATURE)],outfile)

def write_omim_ids(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            if line.startswith('#'):
                continue
            chunks = line.split('\t')
            if 'phenotype' in chunks[1]:
                outf.write(f'{OMIM}:{chunks[0]}\t{DISEASE}\n')

def write_mesh_ids(outfile):
    dcodes = ['C01','C04','C05','C06','C07','C08','C09','C10','C11','C12','C13','C14','C15','C16','C17','C18','C19','C20','C21','C22','C24','C25','C26']
    meshmap = { i:DISEASE for i in dcodes}
    meshmap['C23'] = PHENOTYPIC_FEATURE
    mesh.write_ids(meshmap,outfile,order=[DISEASE,PHENOTYPIC_FEATURE])

def write_umls_ids(mrsty, outfile,badumlsfile):
    badumls=set()
    with open(badumlsfile,'r') as inf:
        for line in inf:
            if line.startswith('#'):
                continue
            umlscui = line.split()[0]
            badumls.add(umlscui)
    #Disease
    #B2.2.1.2.1 Disease or Syndrome
    #A1.2.2.1 Congenital Abnormality
    #A1.2.2.2 Acquired Abnormality
    #B2.3 Injury or Poisoning
    #B2.2.1.2 Pathologic Function
    #B2.2.1.2.1.1 Mental or Behavioral Dysfunction
    #B2.2.1.2.2ell or Molecular Dysfunction
    #A1.2.2 Anatomical Abnormality
    #B2.2.1.2.1.2 Neoplastic Process
    umlsmap = {x: DISEASE for x in ['B2.2.1.2.1', 'A1.2.2.1', 'A1.2.2.2', 'B2.3', 'B2.2.1.2', 'B2.2.1.2.1.1','B2.2.1.2.2','A1.2.2','B2.2.1.2.1.2']}
    #A2.2 Finding
    # Compared groupings with and without finding.  Finding includes a lot of stuff like "Negative" or whatever and it causes some extra globbing up.
    # For instance, the Alzheimer node starts to grab in some nonsense.
    umlsmap['A2.2'] = PHENOTYPIC_FEATURE
    #A2.2.1 Laboratory or Test Result
    #A2.2.2 Sign or Symptom
    umlsmap['A2.2.1'] = PHENOTYPIC_FEATURE
    umlsmap['A2.2.2'] = PHENOTYPIC_FEATURE
    #A2.3 Organism Attribute
    # Includes things like "Age" which will merge with EFOs
    umlsmap['A2.3'] = PHENOTYPIC_FEATURE
    umls.write_umls_ids(mrsty, umlsmap, outfile, blacklist=badumls)


def build_disease_obo_relationships(outdir):
    #Create the equivalence pairs
    with open(f'{outdir}/{HP}', 'w') as outfile:
        build_sets(f'{HP}:0000118', {HP:outfile},
                   ignore_list=['ICD'],
                   other_prefixes={'MSH':MESH,'SNOMEDCT_US':SNOMEDCT,'SNOMED_CT': SNOMEDCT, 'ORPHANET':ORPHANET, 'ICD-9':ICD9, 'ICD-10':ICD10, 'ICD-0':ICD0, 'ICD-O':ICD0 },
                   set_type='xref')
    with open(f'{outdir}/{MONDO}', 'w') as outfile:
        #Orphanet here is confusing.  In mondo it comes out mixed case like "Orphanet" and we want to cap it.  We have a normer
        # in build sets, but it is based on the UPPERCASED prefix.  So we're passing in that we want to change uppercase orphanet to uppercase
        # orphanet.  In actuality that matching key will pick up any case orphanet, including the one that actually occurs.
        build_sets('MONDO:0000001', {MONDO:outfile}, set_type='exact', other_prefixes={'ORPHANET':ORPHANET})
        build_sets('MONDO:0042489', {MONDO:outfile}, set_type='exact', other_prefixes={'ORPHANET':ORPHANET})
    with open(f'{outdir}/{MONDO}_close', 'w') as outfile:
        build_sets('MONDO:0000001', {MONDO:outfile}, set_type='close', other_prefixes={'ORPHANET':ORPHANET})
        build_sets('MONDO:0042489', {MONDO:outfile}, set_type='close', other_prefixes={'ORPHANET':ORPHANET})

def build_disease_efo_relationships(idfile,outfile):
    efo.make_concords(idfile, outfile)


def build_disease_umls_relationships(mrconso, idfile, outfile, omimfile, ncitfile):
    #UMLS contains xrefs between a disease UMLS and a gene OMIM. So here we are saying: if you are going to link to
    # an omim identifier, make sure it's a disease omim, not some other thing.
    good_ids = {}
    for prefix,prefixidfile in [(OMIM,omimfile),(NCIT,ncitfile)]:
        good_ids[prefix] = set()
        with open(prefixidfile,'r') as inf:
            for line in inf:
                x = line.split()[0]
                good_ids[prefix].add(x)
    umls.build_sets(mrconso, idfile, outfile, {'SNOMEDCT_US':SNOMEDCT,'MSH': MESH, 'NCI': NCIT, 'HPO': HP, 'MDR':MEDDRA, 'OMIM': OMIM},acceptable_identifiers=good_ids)

def build_disease_doid_relationships(idfile,outfile):
    doid.build_xrefs(idfile, outfile, other_prefixes={'ICD10CM':ICD10, 'ICD9CM':ICD9, 'ICDO': ICD0, 'NCI': NCIT,
                                                      'SNOMEDCT_US_2018_03_01': SNOMEDCT, 'SNOMEDCT_US_2019_09_01': SNOMEDCT,
                                                      'SNOMEDCT_US_2020_03_01': SNOMEDCT, 'SNOMEDCT_US_2020_09_01': SNOMEDCT,
                                                      'UMLS_CUI': UMLS, 'KEGG': KEGGDISEASE})

def build_compendium(concordances, identifiers, mondoclose, badxrefs, icrdf_filename):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers,new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=[MONDO, HP])
        types.update(new_types)
    #Load close Mondos
    with open(mondoclose, 'r') as inf:
        close_mondos = defaultdict(set)
        for line in inf:
            x = tuple(line.strip().split('\t'))
            close_mondos[x[0]].add(x[1])
    #Load and glom concords
    for infile in concordances:
        print(infile)
        pairs = []
        pref = path.basename(infile)
        if pref in badxrefs:
            print('reading bad xrefs',pref)
            bad_pairs = read_badxrefs(badxrefs[pref])
        else:
            print('no bad pairs', pref)
            bad_pairs = set()
        with open(infile,'r') as inf:
            for line in inf:
                stuff = line.strip().split('\t')
                if len(stuff) != 3:
                    raise RuntimeError('Line "', line.strip(), '" is not a valid concord: ', stuff)
                x = tuple([stuff[0].strip(), stuff[2].strip()])
                if x not in bad_pairs:
                    pairs.append( x )
        if pref in ['MONDO','HP','EFO']:
            newpairs = remove_overused_xrefs(pairs)
        else:
            newpairs = pairs
        glom(dicts, newpairs, unique_prefixes=[MONDO, HP], close={MONDO:close_mondos})
        try:
            print(dicts['OMIM:607644'])
        except:
            print('notyet')
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]),types)
    for biotype,sets in typed_sets.items():
        baretype = biotype.split(':')[-1]
        write_compendium(sets,f'{baretype}.txt',biotype,{}, icrdf_filename=icrdf_filename)

def create_typed_sets(eqsets,types):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has MONDO trust the MONDO's type
                  If it has a HP trust the HP's type
                   If it has an UBERON trust the UBERON's type
    After that, check the types dict to see if we know anything.
    """
    order = [DISEASE, PHENOTYPIC_FEATURE]
    typed_sets = defaultdict(set)
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found  = False
        for prefix in [MONDO, HP]:
            if prefix in prefixes and not found:
                try:
                    mytype = types[prefixes[prefix][0]]
                    typed_sets[mytype].add(equivalent_ids)
                    found = True
                except:
                    #This can happen if the concords are out of sync. Typically, e,g there might be an HP that
                    # doesn't exist anymroe but ist still in UMLS
                    pass
        if not found:
            typecounts = defaultdict(int)
            for eid in equivalent_ids:
                if eid in types:
                    typecounts[types[eid]] += 1
            if len(typecounts) == 0:
                print('how did we not get any types?')
                print(equivalent_ids)
                exit()
            elif len(typecounts) == 1:
                t = list(typecounts.keys())[0]
                typed_sets[t].add(equivalent_ids)
            else:
                #First attempt is majority vote, and after that by most specific
                otypes = [ (-c, order.index(t), t) for t,c in typecounts.items()]
                otypes.sort()
                t = otypes[0][2]
                typed_sets[t].add(equivalent_ids)
    return typed_sets

def read_badxrefs(fn):
    morebad = set()
    with open(fn,'r') as inf:
        for line in inf:
            if line.startswith('#'):
                continue
            x = line.strip().split(' ')
            morebad.add( (x[0],x[1]) )
    return morebad

def load_diseases_and_phenotypes(concords,idlists,badhpos,badhpoxrefs, icrdf_filename):
    #print('disease/phenotype')
    #print('get and write hp sets')
    #bad_mappings = read_bad_hp_mappings(badhpos)
    #more_bad_mappings = read_badxrefs(badhpoxrefs)
    #for h,m in more_bad_mappings.items():
    #    bad_mappings[h].update(m)
    #hpo_sets,labels = build_sets('HP:0000118', ignore_list = ['ICD','NCIT'], bad_mappings = bad_mappings)
    #print('filter')
    hpo_sets = filter_out_non_unique_ids(hpo_sets)
    #print('ok')
    #dump_sets(hpo_sets,'hpo_sets.txt')
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
    meddra_umls,secondary_meddra_umls = read_meddra(bad_umls)
    meddra_umls = filter_umls(meddra_umls,mondo_sets+hpo_sets,'filtered.txt')
    secondary_meddra_umls = filter_umls(secondary_meddra_umls,mondo_sets+hpo_sets,'filtered_secondary.txt')
    #Now, if we just use all the secondary links, things get too agglommed.
    # So instead, lets filter these again.
    meddra_umls += filter_secondaries(secondary_meddra_umls,'double_filter.txt')
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
    glom(dicts,meddra_umls,unique_prefixes=['MONDO','HP'],pref='UMLS',close={'MONDO':mondo_close})
    dump_dicts(dicts,'mondo_hpo_meddra_dicts.txt')
    print('efo')
    glom(dicts,efo_sets,unique_prefixes=['MONDO','HP'],pref='EFO')
    dump_dicts(dicts,'mondo_hpo_meddra_efo_dicts.txt')
    print('dump it')
    fs = set([frozenset(x) for x in dicts.values()])
    diseases,phenotypes = create_typed_sets(fs)
    write_compendium(diseases,'disease.txt','biolink:Disease',labels, icrdf_filename=icrdf_filename)
    write_compendium(phenotypes,'phenotypes.txt','biolink:PhenotypicFeature',labels, icrdf_filename=icrdf_filename)

if __name__ == '__main__':
    with open('crapfile','w') as crapfile:
        build_sets('MONDO:0000001', {MONDO: crapfile}, set_type='exact', other_prefixes={'Orphanet': ORPHANET})

from collections import defaultdict

import src.datahandlers.obo as obo

from src.prefixes import MESH, NCIT, MONDO, OMIM
from src.categories import DISEASE, PHENOTYPIC_FEATURE
import src.datahandlers.umls as umls

import src.datahandlers.mesh as mesh

#def build_sets(iri, concordfiles, ignore_list = ['PMID','BTO','BAMS','FMA','CALOHA','GOC','WIKIPEDIA.EN','CL','GO','NIF_SUBCELLULAR','HTTP','OPENCYC']):
#    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
#    is a set for each subclass of the input iri.  Write these lists to concord files, indexed by the prefix"""
#    uber = UberGraph()
#    uberres = uber.get_subclasses_and_xrefs(iri)
#    #this is getting handled when we input the xrefs, because different methods for compendium building may
#    # have a smart way of handling them
#    #remove_overused_xrefs(uberres)
#    for k,v in uberres.items():
#        for x in v:
#            if Text.get_curie(x) not in ignore_list:
#                p = Text.get_curie(k)
#                if p in concordfiles:
#                    concordfiles[p].write(f'{k}\txref\t{x}\n')

def write_obo_ids(irisandtypes,outfile,exclude=[]):
    order = [DISEASE, PHENOTYPIC_FEATURE]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])

def write_ncit_ids(outfile):
    disease_id = f'{NCIT}:C2991'
    phenotypic_feature_id = f'{NCIT}:C3367'
    write_obo_ids([(disease_id, DISEASE), (phenotypic_feature_id, PHENOTYPIC_FEATURE)], outfile, exclude=[])

def write_mondo_ids(outfile):
    disease_id = f'{MONDO}:0000001'
    write_obo_ids([(disease_id, DISEASE)],outfile)

def write_efo_ids(outfile):
    # Disease
    disease_id='EFO:0000408'
    phenotype_id='EFO:0000651'
    measurement_id='EFO:0001444'
    write_obo_ids([(disease_id, DISEASE),(phenotype_id,PHENOTYPIC_FEATURE),(measurement_id,PHENOTYPIC_FEATURE)],outfile)

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

def write_umls_ids(outfile):
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
    umlsmap['A2.2.2'] = PHENOTYPIC_FEATURE
    umls.write_umls_ids(umlsmap,outfile)

def build_anatomy_obo_relationships(outdir):
    #Create the equivalence pairs
    with open(f'{outdir}/{UBERON}', 'w') as uberon, open(f'{outdir}/{GO}', 'w') as go, open(f'{outdir}/{CL}', 'w') as cl:
        build_sets(f'{UBERON}:0001062', {UBERON:uberon, GO:go, CL:cl})
        build_sets(f'{GO}:0005575', {UBERON:uberon, GO:go, CL:cl})

def build_anatomy_umls_relationships(idfile,outfile):
    umls.build_sets(idfile, outfile, {'SNOMEDCT_US':SNOMEDCT,'MSH': MESH, 'NCI': NCIT})

def build_compendia(concordances, identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers,new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=[UBERON, GO])
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print('loading',infile)
        pairs = []
        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append( set([x[0], x[2]]))
        newpairs = remove_overused_xrefs(pairs)
        glom(dicts, newpairs, unique_prefixes=[UBERON, GO])
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]),types)
    for biotype,sets in typed_sets.items():
        baretype = biotype.split(':')[-1]
        write_compendium(sets,f'{baretype}.txt',biotype,{})

def create_typed_sets(eqsets,types):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has GO trust the GO's type
                   If it has a CL trust the CL's type
                   If it has an UBERON trust the UBERON's type
    After that, check the types dict to see if we know anything.
    """
    order = [CELLULAR_COMPONENT, CELL, GROSS_ANATOMICAL_STRUCTURE, ANATOMICAL_ENTITY]
    typed_sets = defaultdict(set)
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found  = False
        for prefix in [GO, CL, UBERON]:
            if prefix in prefixes and not found:
                mytype = types[prefixes[prefix][0]]
                typed_sets[mytype].add(equivalent_ids)
                found = True
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


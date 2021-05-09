from collections import defaultdict

import src.datahandlers.obo as obo

from src.prefixes import MESH, NCIT, MONDO
from src.categories import DISEASE, PHENOTYPIC_FEATURE
#from src.ubergraph import UberGraph
#from src.util import Text
#from src.babel_utils import write_compendium,glom,get_prefixes,read_identifier_file
#import src.datahandlers.umls as umls
#import src.datahandlers.mesh as mesh

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

#def write_ncit_ids(outfile):
#    #For NCIT, there are some branches of the subhiearrchy that we don't want, like this one for genomic locus
#    anatomy_id = f'{NCIT}:C12219'
#    cell_id = f'{NCIT}:C12508'
#    component_id = f'{NCIT}:C34070'
#    genomic_location_id = f'{NCIT}:C64389'
#    chromosome_band_id = f'{NCIT}:C13432'
#    macromolecular_structure_id = f'{NCIT}:C14134' #protein domains
#    ostomy_site_id = f'{NCIT}:C122638'
#    chromosome_structure_id =f'{NCIT}:C13377'
#    anatomic_site_id=f'{NCIT}:C13717' #the site of procedures like injections etc
#    write_obo_ids([(anatomy_id, ANATOMICAL_ENTITY), (cell_id, CELL), (component_id, CELLULAR_COMPONENT)], outfile, exclude=[genomic_location_id, chromosome_band_id, macromolecular_structure_id, ostomy_site_id, chromosome_structure_id, anatomic_site_id])

def write_mondo_ids(outfile):
    disease_id = f'{MONDO}:0000001'
    write_obo_ids([(disease_id, DISEASE)])


def write_mesh_ids(outfile):
    meshmap = { f'A{str(i).zfill(2)}': ANATOMICAL_ENTITY for i in range(1, 21)}
    meshmap['A11'] = CELL
    meshmap['A11.284'] = CELLULAR_COMPONENT
    mesh.write_ids(meshmap,outfile)

def write_umls_ids(outfile):
    #UMLS categories:
    #A1.2 Anatomical Structure
    #A1.2.1 Embryonic Structure
    #A1.2.3 Fully Formed Anatomical Structure
    #A1.2.3.1 Body Part, Organ, or Organ Component
    #A1.2.3.2 Tissue
    #A1.2.3.3 Cell
    #A1.2.3.4 Cell Component
    #A2.1.4.1 Body System
    #A2.1.5.1 Body Space or Junction
    #A2.1.5.2 Body Location or Region
    umlsmap = {x: ANATOMICAL_ENTITY for x in ['A1.2', 'A1.2.1', 'A1.2.3.1', 'A1.2.3.2', 'A2.1.4.1', 'A2.1.5.1', 'A2.1.5.2']}
    umlsmap['A1.2.3.3'] = CELL
    umlsmap['A1.2.3.4'] = CELLULAR_COMPONENT
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


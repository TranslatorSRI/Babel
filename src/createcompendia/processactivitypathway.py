from os import path
from collections import defaultdict

import src.datahandlers.obo as obo
import src.datahandlers.reactome as reactome
import src.datahandlers.rhea as rhea
import src.datahandlers.ec as ec
import src.datahandlers.umls as umls

from src.prefixes import GO, REACT, WIKIPATHWAYS, RHEA, SMPDB, EC, PANTHERPATHWAY, TCDB
from src.categories import BIOLOGICAL_PROCESS, MOLECULAR_ACTIVITY, PATHWAY
from src.ubergraph import build_sets

from src.babel_utils import read_identifier_file, glom, remove_overused_xrefs, get_prefixes, write_compendium

def write_obo_ids(irisandtypes,outfile,exclude=[]):
    order = [PATHWAY, BIOLOGICAL_PROCESS, MOLECULAR_ACTIVITY]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])

def write_go_ids(outfile):
    # Disease
    pathway_id='GO:0007165'
    process_id='GO:0008150'
    activity_id='GO:0003674'
    gos = [(pathway_id, PATHWAY), (process_id, BIOLOGICAL_PROCESS), (activity_id, MOLECULAR_ACTIVITY)]
    write_obo_ids(gos,outfile)

def write_react_ids(infile,outfile):
    reactome.write_ids(infile,outfile)

def write_ec_ids(outfile):
    ec.make_ids(outfile)


def write_umls_ids(mrsty, outfile):
    umlsmap = { 'B2.2.1.1.4': MOLECULAR_ACTIVITY, # Molecular Function
                'B2.2.1.1': BIOLOGICAL_PROCESS, # Physiologic Function
                'B2.2.1.1.1': BIOLOGICAL_PROCESS, # Organism Function
                'B2.2.1.1.2': BIOLOGICAL_PROCESS, # Organ or Tissue Function
                'B2.2.1.1.3': BIOLOGICAL_PROCESS, #  Cell Function
                'B2.2.1.1.4.1': BIOLOGICAL_PROCESS, # Genetic Function
                }
    umls.write_umls_ids(mrsty, umlsmap, outfile)

def build_process_umls_relationships(mrconso, idfile,outfile):
    umls.build_sets(mrconso, idfile, outfile, {'GO': GO})

def build_process_obo_relationships(outdir):
    #Create the equivalence pairs
    #op={'MSH':MESH,'SNOMEDCT_US':SNOMEDCT,'SNOMED_CT': SNOMEDCT, 'ORPHANET':ORPHANET, 'ICD-9':ICD9, 'ICD-10':ICD10, 'ICD-0':ICD0, 'ICD-O':ICD0 }
    op={'WIKIPEDIA': WIKIPATHWAYS, 'REACTOME':REACT, 'TC':TCDB }
    with open(f'{outdir}/{GO}', 'w') as outfile:
        build_sets(f'{GO}:0007165', {GO:outfile}, set_type='xref', other_prefixes=op )
        build_sets(f'{GO}:0008150', {GO:outfile}, set_type='xref', other_prefixes=op )
        build_sets(f'{GO}:0003674', {GO:outfile}, set_type='xref', other_prefixes=op )

def build_process_rhea_relationships(outfile):
    rhea.make_concord(outfile)


def build_compendia(concordances, identifiers, icrdf_filename):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    #These are concords that cause problems and are being special cased out.  In disease/process we put these in some
    # files, and maybe we should here too?
    #GO:0034227/EC:2.8.1.4 is because that go term is a biological process, but EC is not a valid prefix for that,
    #  leading to a loss of the EC term (and a unified RHEA) on output.
    bad_concords = set( frozenset(['GO:0034227','EC:2.8.1.4']))
    dicts = {}
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers,new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=[GO])
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print('loading',infile)
        # We have a concordance problem with UMLS - it is including GO terms that are obsolete and we don't want
        # them added. So we want to limit concordances to terms that are already in the dicts. But that's ONLY for the
        # UMLS concord.  We trust the others to retrieve decent identifiers.
        pairs = []
        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                if infile.endswith("UMLS"):
                    use = True
                    for xi in (x[0], x[2]):
                        if xi not in dicts:
                            print(f"Skipping pair {x} from {infile} because {xi} is not in dicts")
                            use = False
                    if not use:
                        continue
                pair = ([x[0], x[2]])
                fspair = frozenset(pair)
                if fspair not in bad_concords:
                    pairs.append( pair )
        #one kind of error is that GO->Reactome xrefs are freqently more like subclass relations. So
        # GO:0004674 (protein serine/threonine kinase) has over 400 Reactome xrefs
        # remove_overused_xrefs assumes that we want to remove pairs where the second pair is overused
        # but this case it's the first, so we use the bothways optoin
        newpairs = remove_overused_xrefs(pairs,bothways=True)
        setpairs = [ set(x) for x in newpairs]
        glom(dicts, setpairs, unique_prefixes=[GO])
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]),types)
    for biotype,sets in typed_sets.items():
        baretype = biotype.split(':')[-1]
        write_compendium(sets,f'{baretype}.txt',biotype,{}, icrdf_filename=icrdf_filename)

def create_typed_sets(eqsets,types):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has GO trust the GO's type
    After that, check the types dict to see if we know anything.
    """
    order = [PATHWAY, BIOLOGICAL_PROCESS, MOLECULAR_ACTIVITY]
    typed_sets = defaultdict(set)
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found  = False
        for prefix in [GO]:
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



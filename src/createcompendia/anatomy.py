from collections import defaultdict
from src.ubergraph import UberGraph
from src.util import Text
from src.babel_utils import write_compendium,glom,get_prefixes,read_identifier_file
import src.datahandlers.umls as umls
import src.datahandlers.mesh as mesh

ANATOMICAL_ENTITY = 'biolink:AnatomicalEntity'
GROSS_ANATOMICAL_STRUCTURE = 'biolink:GrossAnatomicalStructure'
CELL = 'biolink:Cell'
CELLULAR_COMPONENT = 'biolink:CellularComponent'


def remove_overused_xrefs(kv):
    """Given a dict of iri->list of xrefs, look through them for xrefs that are in more than one list.
    Remove those anywhere they occur, as they will only lead to pain further on."""
    used_xrefs = set()
    overused_xrefs = set()
    for k, v in kv.items():
        for x in v:
            if x in used_xrefs:
                overused_xrefs.add(x)
            used_xrefs.add(x)
    print(f'There are {len(overused_xrefs)} overused xrefs')
    for k,v in kv.items():
        kv[k] = list( set(v).difference(overused_xrefs) )

#The BTO and BAMs and HTTP (braininfo) identifiers promote over-glommed nodes
#FMA is a specific problem where in CL they use FMA xref to mean 'part of'
#CALOHA is a specific problem where in CL they use FMA xref to mean 'part of'
#GOC is a specific problem where in CL they use FMA xref to mean 'part of'
#wikipedia.en is a specific problem where in CL they use FMA xref to mean 'part of'
#NIF_Subcellular leads to a weird mashup between a GO term and a bunch of other stuff.
#CL only shows up as an xref once in uberon, and it's a mistake.  It doesn't show up in anything else.
#GO only shows up as an xref once in uberon, and it's a mistake.  It doesn't show up in anything else.
#PMID is just wrong
def build_sets(iri, concordfiles, ignore_list = ['PMID','BTO','BAMS','FMA','CALOHA','GOC','WIKIPEDIA.EN','CL','GO','NIF_SUBCELLULAR','HTTP','OPENCYC']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri.  Write these lists to concord files, indexed by the prefix"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    ##TODO:
    ## I'm not completely sure that we should do this here.  Perhaps the concordance should be raw, so that the glommer can figure it out (like
    #  when to go to a kboom approach.  Though I think getting rid of dupes here is right.
    # 1. for some reason, we're writing dups.
    # 2. Worse, we are writing the same xref for multiple uberons, which just leads to shit when glom tries to clean up.
    # check for and remove re-used xrefs
    remove_overused_xrefs(uberres)
    for k,v in uberres.items():
        for x in v:
            if Text.get_curie(x) not in ignore_list:
                p = Text.get_curie(k[0])
                if p in concordfiles:
                    concordfiles[p].write(f'{k[0]}\txref\t{x}\n')

def write_obo_ids(irisandtypes,outfile,exclude=[]):
    uber = UberGraph()
    iris_to_types=defaultdict(set)
    for iri,ntype in irisandtypes:
        uberres = uber.get_subclasses_of(iri)
        for k in uberres:
            iris_to_types[k['descendent']].add(ntype)
    excludes = []
    for excluded_iri in exclude:
        excludes += uber.get_subclasses_of(excluded_iri)
    excluded_iris = set( [k['descendent'] for k in excludes ])
    prefix = Text.get_curie(iri)
    order = [CELLULAR_COMPONENT, CELL, GROSS_ANATOMICAL_STRUCTURE, ANATOMICAL_ENTITY]
    with open(outfile, 'w') as idfile:
        for kd,typeset in iris_to_types.items():
            if kd not in excluded_iris and kd.startswith(prefix):
                l = list(typeset)
                l.sort(key=lambda k: order.index(k))
                idfile.write(f'{kd}\t{l[0]}\n')

def write_ncit_ids(outfile):
    #For NCIT, there are some branches of the subhiearrchy that we don't want, like this one for genomic locus
    anatomy_id = 'NCIT:C12219'
    cell_id = 'NCIT:C12508'
    component_id = 'NCIT:C34070'
    genomic_location_id = 'NCIT:C64389'
    chromosome_band_id = 'NCIT:C13432'
    macromolecular_structure_id = 'NCIT:C14134' #protein domains
    ostomy_site_id = 'NCIT:C122638'
    chromosome_structure_id ='NCIT:C13377'
    anatomic_site_id='NCIT:C13717' #the site of procedures like injections etc
    write_obo_ids([(anatomy_id, ANATOMICAL_ENTITY), (cell_id, CELL), (component_id, CELLULAR_COMPONENT)], outfile, exclude=[genomic_location_id, chromosome_band_id, macromolecular_structure_id, ostomy_site_id, chromosome_structure_id, anatomic_site_id])

def write_uberon_ids(outfile):
    anatomy_id = 'UBERON:0001062'
    gross_id   = 'UBERON:0010000'
    write_obo_ids([(anatomy_id,ANATOMICAL_ENTITY),(gross_id,GROSS_ANATOMICAL_STRUCTURE)],outfile)

def write_cl_ids(outfile):
    cell_id   = 'CL:0000000'
    write_obo_ids([(cell_id,CELL)],outfile)

def write_go_ids(outfile):
    component_id = 'GO:0005575'
    write_obo_ids([(component_id,CELLULAR_COMPONENT)],outfile)


def write_mesh_ids(outfile):
    meshmap = { f'A{str(i).zfill(2)}':ANATOMICAL_ENTITY for i in range(1,21) }
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
    umlsmap = { x:ANATOMICAL_ENTITY for x in ['A1.2','A1.2.1','A1.2.3.1','A1.2.3.2','A2.1.4.1','A2.1.5.1','A2.1.5.2']}
    umlsmap['A1.2.3.3'] = CELL
    umlsmap['A1.2.3.4'] = CELLULAR_COMPONENT
    umls.write_umls_ids(umlsmap,outfile)

def build_anatomy_obo_relationships(outdir):
    #Create the equivalence pairs
    with open(f'{outdir}/UBERON','w') as uberon, open(f'{outdir}/GO','w') as go, open(f'{outdir}/CL','w') as cl:
        build_sets('UBERON:0001062',{'UBERON':uberon, 'GO':go, 'CL':cl })
        build_sets('GO:0005575',{'UBERON':uberon, 'GO':go, 'CL':cl })

def build_anatomy_umls_relationships(idfile,outfile):
    umls.build_sets(idfile,outfile,{'SNOMEDCT_US':'SNOMEDCT','MSH':"MESH",'NCI':'NCIT'})

def build_compendia(concordances, identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers,new_types = read_identifier_file(ifile)
        glom(dicts,new_identifiers, unique_prefixes=['UBERON','GO'])
        types.update(new_types)
        print(f'Has it? {"MESH:D000009" in dicts}')
    for infile in concordances:
        print(infile)
        print('loading',infile)
        newpairs = []
        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                newpairs.append( set([x[0], x[2]]))
        glom(dicts,newpairs, unique_prefixes=['UBERON','GO'])
        print(f'Has it? {"MESH:D000009" in dicts}')
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
    order = [CELLULAR_COMPONENT,CELL,GROSS_ANATOMICAL_STRUCTURE,ANATOMICAL_ENTITY]
    typed_sets = defaultdict(set)
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found  = False
        for prefix in ['GO','CL','UBERON']:
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


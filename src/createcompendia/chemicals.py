from collections import defaultdict
import jsonlines
import requests

import src.datahandlers.obo as obo

from src.prefixes import MESH, CHEBI, UNII, DRUGBANK, INCHIKEY, PUBCHEMCOMPOUND,GTOPDB, KEGGCOMPOUND
from src.categories import CHEMICAL_SUBSTANCE
from src.sdfreader import read_sdf

from src.datahandlers.unichem import data_sources as unichem_data_sources
from src.babel_utils import write_compendium, glom, get_prefixes, read_identifier_file, remove_overused_xrefs

import src.datahandlers.mesh as mesh

def write_mesh_ids(outfile):
    #Get the D tree,
    # D01	Inorganic Chemicals
    # D02	Organic Chemicals
    # D03	Heterocyclic Compounds
    # D04	Polycyclic Compounds
    # D05	Macromolecular Substances  NO
    # D06	Hormones, Hormone Substitutes, and Hormone Antagonists
    # D08	Enzymes and Coenzymes
    # D09	Carbohydrates
    # D10	Lipids
    # D12	Amino Acids, Peptides, and Proteins
    # D12.125 AA yes
    # D12.644 Peptides yes
    # D12.776 proteins  NO
    # D13	Nucleic Acids, Nucleotides, and Nucleosides
    # D20	Complex Mixtures
    # D23	Biological Factors
    # D25	Biomedical and Dental Materials
    # D26	Pharmaceutical Preparations
    # D27	Chemical Actions and Uses
    meshmap = { f'D{str(i).zfill(2)}': CHEMICAL_SUBSTANCE for i in range(1, 28)}
    meshmap['D05'] = 'EXCLUDE'
    meshmap['D12.776'] = 'EXCLUDE'
    meshmap['D12.125'] = CHEMICAL_SUBSTANCE
    meshmap['D12.644'] = CHEMICAL_SUBSTANCE
    #Also add anything from SCR_Chemical, if it doesn't have a tree map
    mesh.write_ids(meshmap,outfile,order=['EXCLUDE',CHEMICAL_SUBSTANCE],extra_vocab={'SCR_Chemical':CHEMICAL_SUBSTANCE})

def write_obo_ids(irisandtypes,outfile,exclude=[]):
    order = [CHEMICAL_SUBSTANCE]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])

def write_chebi_ids(outfile):
    chemical_entity_id = f'{CHEBI}:24431'
    write_obo_ids([(chemical_entity_id, CHEMICAL_SUBSTANCE)], outfile)

def write_unii_ids(infile,outfile):
    """UNII contains a bunch of junk like leaves.   We are going to try to clean it a bit to get things
    that are actually chemicals.  In biolink 2.0 we cn revisit exactly what happens here."""
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline().strip().split('\t')
        bad_cols = ['NCBI','PLANTS','GRIN','MPNS']
        bad_colnos = [ h.index(bc) for bc in bad_cols ]
        for line in inf:
            x = line.strip().split('\t')
            for bcn in bad_colnos:
                if len(x[bcn]) > 0:
                    #This is a plant or an eye of newt or something
                    continue
            outf.write(f'{UNII}:{x[0]}\t{CHEMICAL_SUBSTANCE}\n')

def write_drugbank_ids(infile,outfile):
    """We don't have a good drugbank source, so we're going to dig through unichem and get out drugbank ids."""
    #doublecheck so that we know we're getting the right value
    drugbank_id = '2'
    assert unichem_data_sources[drugbank_id] == DRUGBANK
    # The columns are: [0'uci_old', 1'src_id', 2'src_compound_id', 3'assignment', 4'last_release_u_when_current', 5 'created ',
    # 6'lastupdated', 7'userstamp', 8'aux_src', 9'uci'])
    written = set()
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.split('\t')
            if x[1] == drugbank_id:
                if x[2] in written:
                    continue
                dbid = f'{DRUGBANK}:{x[2]}'
                outf.write(f'{dbid}\t{CHEMICAL_SUBSTANCE}\n')
                written.add(x[2])

def write_unichem_concords(structfile,reffile,outdir):
    inchikeys = read_inchikeys(structfile)
    concfiles = {}
    for num,name in unichem_data_sources.items():
        concname = f'{outdir}/UNICHEM_{name}'
        print(concname)
        concfiles[num] = open(concname,'w')
    with open(reffile,'r') as inf:
        # The columns are: [0'uci_old', 1'src_id', 2'src_compound_id', 3'assignment', 4'last_release_u_when_current', 5 'created ',
        # 6'lastupdated', 7'userstamp', 8'aux_src', 9'uci'])
        for line in inf:
            x = line[:-1].split('\t')
            if len(x) < 10:
                print(line)
                continue
            outf = concfiles[x[1]]
            outf.write(f'{unichem_data_sources[x[1]]}:{x[2]}\toio:equivalent\t{inchikeys[x[9]]}\n')
    for outf in concfiles.values():
        outf.close()

def read_inchikeys(struct_file):
    #struct header [0'uci_old', 1'standardinchi', 2'standardinchikey', 3'created', 4'username', 5'fikhb', 6'uci', 'parent_smiles'],
    inchikeys = {}
    with open(struct_file,'r') as inf:
        for sline in inf:
            line = sline[:-1].split('\t')
            if len(line) == 0:
                continue
            if len(line) < 7:
                print(line)
            uci = line[6]
            inchikeys[uci] = f'{INCHIKEY}:{line[2]}'
    return inchikeys

def combine_unichem(concordances,output):
    dicts = {}
    for infile in concordances:
        print(infile)
        print('loading',infile)
        pairs = []
        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append( set([x[0], x[2]]))
        newpairs = remove_overused_xrefs(pairs)
        glom(dicts, newpairs, unique_prefixes=[INCHIKEY])
    chem_sets = set([frozenset(x) for x in dicts.values()])
    with jsonlines.open(output, mode='w') as writer:
        for chemset in chem_sets:
            writer.write(list(chemset))

def read_partial_unichem(unichem_partial):
    chem_sets = {}
    with jsonlines.open(unichem_partial) as reader:
        for chemlist in reader:
            chemset = set(chemlist)
            for element in chemset:
                chem_sets[element] = chemset
    return chem_sets

def is_cas(thing):
    #The last digit in a CAS is a checksum. We could use, but are not atm.
    x = thing.split('-')
    if len(x) != 3:
        return False
    if len(x[-1]) != 1:
        return False
    for xi in x:
        if not xi.isnumeric():
            return False
    return True

def make_pubchem_cas_concord(pubchemsynonyms, outfile):
    with open(pubchemsynonyms,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split('\t')
            if is_cas(x[1]):
                outf.write(f'{x[0]}\txref\tCAS:{x[1]}\n')

def make_pubchem_mesh_concord(pubcheminput,meshlabels,outfile):
    mesh_label_to_id={}
    with open(meshlabels,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            mesh_label_to_id[x[1]] = x[0]
    #The pubchem - mesh pairs are supposed to be ordered in this file such that the
    # first mapping is the 'best' i.e. the one most frequently reported.
    # We will only use the first one
    used_pubchem = set()
    with open(pubcheminput,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split('\t') # x[0] = puchemid (no prefix), x[1] = mesh label
            if x[0] in used_pubchem:
                continue
            try:
                mesh_id = mesh_label_to_id[x[1]]
            except:
                print(f'no mesh for label {x[1]}')
                continue
            outf.write(f'{PUBCHEMCOMPOUND}:{x[0]}\txref\t{mesh_id}\n')
            used_pubchem.add(x[0])

def make_gtopdb_relations(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline().strip().split('\t')
        gid_index = h.index('"Ligand id"')
        inchi_index = h.index('"InChIKey"')
        for line in inf:
            x = line.strip().split('\t')
            if x[inchi_index] == '""':
                continue
            gid = f'{GTOPDB}:{x[gid_index][1:-1]}'
            inchi = f'{INCHIKEY}:{x[inchi_index][1:-1]}'
            outf.write(f'{gid}\txref\t{inchi}\n')

def make_chebi_relations(sdf,dbx,outfile):
    """CHEBI contains relations both about chemicals with and without inchikeys.  You might think that because
    everything is based on unichem, we could avoid the with structures part, but history has shown that we lose
    links in that case, so we will use both the structured and unstructured chemical entries."""
    #THE SDF and XREF stuff are handled in the same function because knowing what we found in the SDF impacts
    # what we want to get out of the xrefs. But the function is quite unwieldy
    #READ SDF
    ikeys = { x:x for x in ['chebiname', 'chebiid', 'secondarychebiid','inchikey','smiles', 'keggcompounddatabaselinks', 'pubchemdatabaselinks'] }
    chebi_sdf_dat = read_sdf(sdf,ikeys)
    #CHEBIs in the sdf by definition have structure (the sdf is a structure file)
    structured_chebi = set(chebi_sdf_dat.keys())
    #READ xrefs
    with open(dbx,'r') as inf:
        dbxdata = inf.read()
    kk = 'keggcompounddatabaselinks'
    pk = 'pubchemdatabaselinks'
    with open(outfile,'w') as outf:
        #Write SDF structured things
        for cid,props in chebi_sdf_dat.items():
            if kk in props:
                outf.write(f'{cid}\txref\t{KEGGCOMPOUND}:{props[kk]}\n')
            if pk in props:
                #Apparently there's a lot of structure here?
                v = props[pk]
                parts = v.split('SID: ')
                for p in parts:
                    if 'CID' in p:
                        mapped = True
                        x = p.split('CID: ')[1]
                        outf.write(f'{cid}\txref\t{PUBCHEMCOMPOUND}:{x}\n')
        #DO THE xref stuff
        lines = dbxdata.split('\n')
        for line in lines[1:]:
            x = line.strip().split('\t')
            if len(x) < 4:
                continue
            cid = f'{CHEBI}:{x[1]}'
            if cid in structured_chebi:
                continue
            if x[3] == 'KEGG COMPOUND accession':
                outf.write(f'{cid}\txref\t{KEGGCOMPOUND}:{x[4]}\n')
            if x[3] == 'Pubchem accession':
                outf.write(f'{cid}\txref\t{PUBCHEMCOMPOUND}:{x[4]}\n')




def get_mesh_relationships(cas_out, unii_out):
    #perhaps I should filter by the chemical mesh ids...
    regis = mesh.pull_mesh_registry()
    with open(cas_out,'w') as casout, open(unii_out,'w') as uniiout:
        for meshid,reg in regis:
            if reg.startswith('EC'):
                continue
            if is_cas(reg):
                casout.write(f'{meshid}\txref\tCAS:{reg}\n')
            else:
                #is a unii?
                uniiout.write(f'{meshid}\txref\tUNII:{reg}\n')

def get_wikipedia_relationships(outfile):
    url = 'https://query.wikidata.org/sparql?format=json&query=SELECT ?chebi ?mesh WHERE { ?compound wdt:P683 ?chebi . ?compound wdt:P486 ?mesh. }'
    results = requests.get(url).json()
    pairs = [(f'{MESH}:{r["mesh"]["value"]}', f'{CHEBI}:{r["chebi"]["value"]}')
             for r in results['results']['bindings']
             if not r['mesh']['value'].startswith('M')]
    #Wikidata is great, except when it sucks.   One thing it likes to do is to
    # have multiple CHEBIs for a concept, say ignoring stereochemistry or
    # the like.  No good.   It's easy enough to filter these out, but then
    # we wouldn't have the mesh associated with anything. A spot check makes it seem like
    # cases of this type usually also have a UNII.  So we can perhaps remove ugly pairs without
    # a problem. We leave them in at this point, and they will get filtered out on reading
    with open(outfile,'w') as outf:
        m2c = defaultdict(list)
        for m,c in pairs:
            outf.write(f'{m}\txref\t{c}\n')

def build_compendia(concordances, identifiers,unichem_partial):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = read_partial_unichem(unichem_partial)
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers,new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=[INCHIKEY])
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
        glom(dicts, newpairs, unique_prefixes=[INCHIKEY])
    chem_sets = set([frozenset(x) for x in dicts.values()])
    baretype = CHEMICAL_SUBSTANCE.split(':')[-1]
    write_compendium(chem_sets, f'{baretype}.txt', CHEMICAL_SUBSTANCE, {})



###TRASH VVVVVVVV TRASH###

def remove_overused_xrefs_dict(kv):
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




def write_ncit_ids(outfile):
    #For NCIT, there are some branches of the subhiearrchy that we don't want, like this one for genomic locus
    anatomy_id = f'{NCIT}:C12219'
    cell_id = f'{NCIT}:C12508'
    component_id = f'{NCIT}:C34070'
    genomic_location_id = f'{NCIT}:C64389'
    chromosome_band_id = f'{NCIT}:C13432'
    macromolecular_structure_id = f'{NCIT}:C14134' #protein domains
    ostomy_site_id = f'{NCIT}:C122638'
    chromosome_structure_id =f'{NCIT}:C13377'
    anatomic_site_id=f'{NCIT}:C13717' #the site of procedures like injections etc
    write_obo_ids([(anatomy_id, ANATOMICAL_ENTITY), (cell_id, CELL), (component_id, CELLULAR_COMPONENT)], outfile, exclude=[genomic_location_id, chromosome_band_id, macromolecular_structure_id, ostomy_site_id, chromosome_structure_id, anatomic_site_id])

def write_uberon_ids(outfile):
    anatomy_id = f'{UBERON}:0001062'
    gross_id   = f'{UBERON}:0010000'
    write_obo_ids([(anatomy_id, ANATOMICAL_ENTITY), (gross_id, GROSS_ANATOMICAL_STRUCTURE)], outfile)

def write_cl_ids(outfile):
    cell_id   = f'{CL}:0000000'
    write_obo_ids([(cell_id, CELL)], outfile)

def write_go_ids(outfile):
    component_id = f'{GO}:0005575'
    write_obo_ids([(component_id, CELLULAR_COMPONENT)], outfile)



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

#Ignore list notes:
#The BTO and BAMs and HTTP (braininfo) identifiers promote over-glommed nodes
#FMA is a specific problem where in CL they use FMA xref to mean 'part of'
#CALOHA is a specific problem where in CL they use FMA xref to mean 'part of'
#GOC is a specific problem where in CL they use FMA xref to mean 'part of'
#wikipedia.en is a specific problem where in CL they use FMA xref to mean 'part of'
#NIF_Subcellular leads to a weird mashup between a GO term and a bunch of other stuff.
#CL only shows up as an xref once in uberon, and it's a mistake.  It doesn't show up in anything else.
#GO only shows up as an xref once in uberon, and it's a mistake.  It doesn't show up in anything else.
#PMID is just wrong
def build_anatomy_obo_relationships(outdir):
    ignore_list = ['PMID','BTO','BAMS','FMA','CALOHA','GOC','WIKIPEDIA.EN','CL','GO','NIF_SUBCELLULAR','HTTP','OPENCYC']
    #Create the equivalence pairs
    with open(f'{outdir}/{UBERON}', 'w') as uberon, open(f'{outdir}/{GO}', 'w') as go, open(f'{outdir}/{CL}', 'w') as cl:
        build_sets(f'{UBERON}:0001062', {UBERON:uberon, GO:go, CL:cl},ignore_list=ignore_list)
        build_sets(f'{GO}:0005575', {UBERON:uberon, GO:go, CL:cl},ignore_list=ignore_list)

def build_anatomy_umls_relationships(idfile,outfile):
    umls.build_sets(idfile, outfile, {'SNOMEDCT_US':SNOMEDCT,'MSH': MESH, 'NCI': NCIT})


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


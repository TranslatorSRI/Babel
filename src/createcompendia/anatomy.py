from collections import defaultdict
import requests

import src.datahandlers.obo as obo
from src.util import Text

from src.prefixes import MESH, NCIT, CL, GO, UBERON, SNOMEDCT, WIKIDATA, UMLS, FMA
from src.categories import ANATOMICAL_ENTITY, GROSS_ANATOMICAL_STRUCTURE, CELL, CELLULAR_COMPONENT
from src.ubergraph import build_sets
from src.babel_utils import write_compendium, glom, get_prefixes, read_identifier_file, remove_overused_xrefs
import src.datahandlers.umls as umls
import src.datahandlers.mesh as mesh

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


def write_obo_ids(irisandtypes,outfile,exclude=[]):
    order = [CELLULAR_COMPONENT, CELL, GROSS_ANATOMICAL_STRUCTURE, ANATOMICAL_ENTITY]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])


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


def write_mesh_ids(outfile):
    meshmap = { f'A{str(i).zfill(2)}': ANATOMICAL_ENTITY for i in range(1, 21)}
    meshmap['A11'] = CELL
    meshmap['A11.284'] = CELLULAR_COMPONENT
    mesh.write_ids(meshmap,outfile)

def write_umls_ids(mrsty, outfile):
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
    umls.write_umls_ids(mrsty, umlsmap, outfile)

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
        build_sets(f'{UBERON}:0001062', {UBERON:uberon, GO:go, CL:cl},'xref', ignore_list=ignore_list)
        build_sets(f'{GO}:0005575', {UBERON:uberon, GO:go, CL:cl},'xref', ignore_list=ignore_list)

def build_wikidata_cell_relationships(outdir):
    #This sparql returns all the wikidata items that have a UMLS identifier and a CL identifier
    sparql = """PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>
        SELECT * WHERE {
          ?wd wdtn:P7963 ?cl .
          ?wd wdt:P2892 ?umls .
        }"""
    frink_wikidata_url = "https://frink.apps.renci.org/federation/sparql"
    response = requests.post(frink_wikidata_url, data={'query': sparql})
    if not response.ok:
        raise RuntimeError(f"Could not query {frink_wikidata_url}: {response.status_code} {response.reason}")
    try:
        results = response.json()
    except Exception as e:
        raise RuntimeError(f"Could not parse {frink_wikidata_url}: {e} raised when parsing response {response.content}.")
    rows = results["results"]["bindings"]
    # If one wikidata entry has either more than one CL or more than one UMLS, then we end up with problems
    # (It could also be possible that the same CL is on more than one wikidata entry, but haven't seen that yet)
    # Loop over the rows, transform each row into curies, and filter out any wikidata entry that occurs more than once.
    # Double check that the UMLS and CL are unique.  Then write out the now-unique UMLS/CL mappings
    counts = defaultdict(int)
    pairs = []
    for row in rows:
        umls_curie = f'{UMLS}:{row["umls"]["value"]}'
        wd_curie = f'{WIKIDATA}:{row["wd"]["value"]}'
        cl_curie = Text.obo_to_curie(row["cl"]["value"])
        pairs.append( (umls_curie, cl_curie) )
        counts[umls_curie] += 1
        counts[cl_curie] += 1
    with open(f'{outdir}/{WIKIDATA}', 'w') as wd:
        for pair in pairs:
            if (counts[pair[0]] == 1) and (counts[pair[1]] == 1):
                wd.write(f'{pair[0]}\teq\t{pair[1]}\n')
            else:
                print(f'Pair {pair} is not unique {counts[pair[0]]} {counts[pair[1]]}')

def build_anatomy_umls_relationships(mrconso, idfile,outfile):
    umls.build_sets(mrconso, idfile, outfile, {'SNOMEDCT_US':SNOMEDCT,'MSH': MESH, 'NCI': NCIT, 'GO': GO, 'FMA': FMA})

def build_compendia(concordances, identifiers, icrdf_filename):
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
        # We have a concordance problem with UMLS - it is including GO terms that are obsolete and we don't want
        # them added. So we want to limit concordances to terms that are already in the dicts. But that's ONLY for the
        # UMLS concord.  We trust the others to retrieve decent identifiers.
        bs = frozenset( [UMLS, GO])
        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                prefixes = frozenset( [xi.split(':')[0] for xi in x[0:3:2]]) #leave out the predicate
                if prefixes == bs:
                    use = True
                    for xi in (x[0], x[2]):
                        if xi not in dicts:
                            print(f"Skipping pair {x} from {infile}: terms with prefixes {bs} are skipped unless they are already in the concords.")
                            use = False
                    if not use:
                        continue
                pairs.append( ([x[0], x[2]]) )
        newpairs = remove_overused_xrefs(pairs)
        setpairs = [ set(x) for x in newpairs]
        glom(dicts, setpairs, unique_prefixes=[UBERON, GO])
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]),types)
    for biotype,sets in typed_sets.items():
        baretype = biotype.split(':')[-1]
        write_compendium(sets,f'{baretype}.txt',biotype,{}, icrdf_filename=icrdf_filename)

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
            if prefix in prefixes and prefixes[prefix][0] in types and not found:
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


import itertools
import logging
import os
import pickle
from ast import literal_eval

from src.util import LoggingUtil
from babel.babel_utils import pull_via_ftp, dump_dict, ThrottledRequester, make_local_name, StateDB, dump_sets
from src.LabeledID import LabeledID

#logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def parse_mesh(data):
    """We want things from the B Tree in MESH"""
    taxon_mesh = set()
    unmapped_mesh = set()
    term_to_concept = {}
    concept_to_txid  = {}
    concept_to_label = {}
    for line in data.split('\n'):
        if line.startswith('#'):
            continue
        triple = line[:-1].strip().split('\t')
        try:
            s,v,o = triple
        except:
            #print(line)
            #print( triple )
            continue
        if v == '<http://id.nlm.nih.gov/mesh/vocab#treeNumber>':
            treenum = o.split('/')[-1]
            if treenum.startswith('B'):
                meshid = f"MESH:{s[:-1].split('/')[-1]}"
                taxon_mesh.add(meshid)
        elif o == '<http://id.nlm.nih.gov/mesh/vocab#SCR_Organism>':
            meshid = f"MESH:{s[:-1].split('/')[-1]}"
            taxon_mesh.add(meshid)
        elif v == '<http://id.nlm.nih.gov/mesh/vocab#preferredConcept>':
            meshid = f"MESH:{s[:-1].split('/')[-1]}"
            concept = o
            term_to_concept[meshid] = o
        elif v == '<http://id.nlm.nih.gov/mesh/vocab#registryNumber>':
            o = o[1:-1] #Strip quotes
            if o == '0':
                continue
            elif o.startswith('txid'):
                concept_to_txid[s] = o
        elif v == '<http://www.w3.org/2000/01/rdf-schema#label>':
            meshid = f"MESH:{s[:-1].split('/')[-1]}"
            concept_to_label[meshid] = o.strip().split('"')[1]
    term_to_txid={}
    for term,concept in term_to_concept.items():
        if concept in concept_to_txid:
            term_to_txid[term] = f'NCBITaxon:{concept_to_txid[concept][4:]}'
    return taxon_mesh,  term_to_txid, concept_to_label

def chunked(it, size):
    """Wraps an iterable, returning it in chunks of size: size"""
    it = iter(it)
    while True:
        p = tuple(itertools.islice(it, size))
        if not p:
            break
        yield p

def lookup(meshes):
    apikey = get_api_key()
    print("Looking up by mesh")
    term_to_pubs = {}
    if apikey is None:
        print('Warning: not using API KEY for eutils, resulting in 3x slowdown')
        delta = 350 #milleseconds
    else:
        delta = 110 #milliseconds
    requester = ThrottledRequester(delta)
    chunksize=10
    backandforth={'C': '67', '67': 'C', 'D': '68', '68': 'D'}
    num = len(meshes)
    done = 0
    for xterms in chunked(meshes,chunksize):
        terms = [t.split(':')[1] for t in xterms]
        url='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?&dbfrom=mesh&db=taxonomy&retmode=json'
        if apikey is not None:
            url+=f'&api_key={apikey}'
        for term in terms:
            try:
                newterm = f'{backandforth[term[0]]}{term[1:]}'
            except KeyError:
                #Q terms get in here, which are things like "radiotherapy"
                continue
            url+=f'&id={newterm}'
        try:
            #returns a throttled flag also, but we don't need it
            result = requester.get_json(url)
            #result = response.json()
        except Exception as e:
            print('E1')
            print(url)
            print(result)
            print(e)
        if 'linksets' not in result:
            continue
        linksets = result['linksets']
        for ls in linksets:
            cids = None
            if 'linksetdbs' in ls:
                mesh=ls['ids'][0]
                for lsdb in ls['linksetdbs']:
                    if lsdb['linkname'] == 'mesh_taxonomy':
                        cids = lsdb['links']
            if cids is not None:
                # 5 or more is probably a group, not a compound
                if len(cids) >= 5:
                    continue
                smesh = str(mesh)
                remesh = f'{backandforth[smesh[0:2]]}{smesh[2:]}'
                if len(cids) == 1:
                    #There's no ambiguity
                    term_to_pubs[f'MESH:{remesh}'] = f'NCBITaxon:{cids[0]}'
                    continue
        done += chunksize
        if done % 1000 == 0:
            print(f' completed {done} / {num}')
    if len(term_to_pubs) == 0:
        'We found no MESH/NCBI links, which is wrong.  Stopping.'
        exit()
    print(f'mesh found {len(term_to_pubs)}')
    return term_to_pubs

def go_mesh():
    f = pull_via_ftp('ftp.nlm.nih.gov', '/online/mesh/rdf', 'mesh.nt.gz', decompress_data=True)
    mesh_taxon_set, mesh2ncbi, mesh_labels = parse_mesh(f)
    ecoli =  'D004926'
    #which taxa don't have an ncbi already?
    tolookup = set( [x for x in mesh_taxon_set if x not in mesh2ncbi ])
    maps = lookup(tolookup)
    mesh2ncbi.update(maps)
    taxons = []
    for mesh in mesh_taxon_set:
        tx = [ mesh ]
        if mesh in mesh2ncbi:
            tx.append( mesh2ncbi[mesh] )
        taxons.append(tx)
    return taxons, mesh_labels

def get_api_key():
    return os.environ.get('EUTILS_API_KEY',default=None)

if __name__ == '__main__':
    #refresh_mesh_pubchem(deep_refresh = False)
    #refresh_mesh_pubchem()
    go_mesh()

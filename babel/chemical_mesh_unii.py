import itertools
import logging
import os
import pickle

from src.util import LoggingUtil
from babel.babel_utils import pull_via_ftp, dump_dict, ThrottledRequester

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

def parse_mesh(data):
    """THERE are two kinds of mesh identifiers that correspond to chemicals.
    1. Anything in the D tree
    2. SCR_Chemicals from the appendices.
    Dig through and find anything like this"""
    chemical_mesh = set()
    unmapped_mesh = set()
    term_to_concept = {}
    concept_to_cas  = {}
    concept_to_unii  = {}
    concept_to_EC  = {}
    concept_to_label = {}
    for line in data.split('\n'):
        if line.startswith('#'):
            continue
        triple = line[:-1].strip().split('\t')
        try:
            s,v,o = triple
        except:
            print(line)
            print( triple )
            continue
        if v == '<http://id.nlm.nih.gov/mesh/vocab#treeNumber>':
            treenum = o.split('/')[-1]
            if treenum.startswith('D'):
                meshid = s[:-1].split('/')[-1]
                chemical_mesh.add(meshid)
        elif o == '<http://id.nlm.nih.gov/mesh/vocab#SCR_Chemical>':
            meshid = s[:-1].split('/')[-1]
            chemical_mesh.add(meshid)
        elif v == '<http://id.nlm.nih.gov/mesh/vocab#preferredConcept>':
            meshid = s[:-1].split('/')[-1]
            concept = o
            term_to_concept[meshid] = o
        elif v == '<http://id.nlm.nih.gov/mesh/vocab#registryNumber>':
            o = o[1:-1] #Strip quotes
            if o == '0':
                continue
            if '-' in o:
                concept_to_cas[s] = o
            elif o.startswith('EC'):
                concept_to_EC[s] = o
            else:
                concept_to_unii[s] = o
        elif v == '<http://www.w3.org/2000/01/rdf-schema#label>':
            meshid = s[:-1].split('/')[-1]
            concept_to_label[meshid] = o.strip().split('"')[1]
    term_to_cas={}
    term_to_unii={}
    term_to_EC={}
    for term,concept in term_to_concept.items():
        if concept in concept_to_cas:
            term_to_cas[term] = concept_to_cas[concept]
        elif concept in concept_to_unii:
            term_to_unii[term] = concept_to_unii[concept]
        elif concept in concept_to_EC:
            term_to_EC[term] = concept_to_EC[concept]
        else:
            unmapped_mesh.add(term)
    print ( f"Found {len(chemical_mesh)} compounds in mesh")
    print ( f"Found {len(term_to_cas)} compounds with CAS identifiers")
    print ( f"Found {len(term_to_unii)} compounds with UNII identifiers")
    print ( f"Found {len(unmapped_mesh)} compounds with NOTHING")
    print ( f"{len(term_to_cas) + len(term_to_unii) + len(unmapped_mesh)}")
    return unmapped_mesh, term_to_cas, term_to_unii, term_to_EC,concept_to_label


def chunked(it, size):
    """Wraps an iterable, returning it in chunks of size: size"""
    it = iter(it)
    while True:
        p = tuple(itertools.islice(it, size))
        if not p:
            break
        yield p

def lookup_by_mesh(meshes,apikey):
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
    for terms in chunked(meshes,chunksize):
        url='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?&dbfrom=mesh&db=pccompound&retmode=json'
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
            response,_ = requester.get(url)
            result = response.json()
        except Exception as e:
            print(url)
            print(result)
            print(e)
            exit()
        if 'linksets' not in result:
            continue
        linksets = result['linksets']
        for ls in linksets:
            cids = None
            if 'linksetdbs' in ls:
                mesh=ls['ids'][0]
                for lsdb in ls['linksetdbs']:
                    if lsdb['linkname'] == 'mesh_pccompound':
                        cids = lsdb['links']
            if cids is not None:
                smesh = str(mesh)
                remesh = f'{backandforth[smesh[0:2]]}{smesh[2:]}'
                if len(cids) <5:
                    # 5 or more is probably a group, not a compound
                    term_to_pubs[remesh] = cids
        done += chunksize
        if done % 1000 == 0:
            print(f' completed {done} / {num}')
    print(f'mesh found {len(term_to_pubs)}')
    return term_to_pubs

def lookup_by_cas(term_to_cas,apikey):
    print("Looking up by cas")
    term_to_pubs = {}
    if apikey is None:
        print('Warning: not using API KEY for eutils, resulting in 3x slowdown')
        delta = 400 #ms
    else:
        delta = 110 #ms
    requester = ThrottledRequester(delta)
    num = len(term_to_cas)
    done = 0
    for term in term_to_cas:
        cas = term_to_cas[term]
        url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pccompound&term={cas}&retmode=json'
        if apikey is not None:
            url+=f'&api_key={apikey}'
        try:
            response,_ = requester.get(url)
            result = response.json()
        except Exception as e:
            print(e)
            print(url)
            continue
        try:
            r = result['esearchresult']
            if 'errorlist' in r:
                if 'phrasesnotfound' in r['errorlist']:
                    if cas in r['errorlist']['phrasesnotfound']:
                        continue
            term_to_pubs[term] = result['esearchresult']['idlist']
        except Exception as e:
            print(e)
            break
        done += 1
        if done % 1000 == 0:
            print(f' completed {done} / {num}')
    print(f'cas found {len(term_to_pubs)}')
    return term_to_pubs

#this is the input function for the module
def refresh_mesh_pubchem(deep_refresh = True):
    """There are 3 possible ways to map mesh terms
    1. Sometimes the registry term in mesh will be a UNII id.  These are great, unichem can map them to everything else.
    2. Sometimes there is a CAS number. These are ok. It's a good way to get a less ambiguous mapping, but you have to
       use eutils to get at them.  Furthermore: A single CAS will map to multiple PUBCHEM compounds.  This is apparently
       because somebody is not paying attention to stereochemistry.  I'm not sure if it's CAS or PUBCHEM mapping to CAS
       but the upshot is that there is no way to choose which pubchem we want, so we will take all, and that will
       end up glomming together stereo and non-stereo versions  fo the structure. Oh well.
    3. Sometimes the registry term is 0.  Literally.  In this case, the only hope is to call eutils and see what you
       get back.  Here's what NLM support says about what to do with the results:
             Note that most will have multiple matches, but you may only be interested in the
             "one" record that is most appropriate.  In most cases they should be sorted properly,
             but not always (meaning the first ID from PC Compound is likely the one you want).
       Yikes.  I think that there may be a way to poke harder by looking at the mesh label and seeing if it's in the
       synonyms for the pubchem compound?  Looking at some of these, it looks like one way to get multiple pubchem
       cids is that the same mesh will map to different stereoisomers, and also different salt forms (or unsalted).
       The other (worse) thing that can happen is when there is a mesh term that is a higher level term like
       "Calcium Channel Agonists".  Then we get a pile of CIDs back, and none of them really map to the concept, but are
       instances of the concept.  I think that we'll put in a threshold. If we only see a couple or 3, we use them all,
       if we see more than that, we give up.
    4. There's actually another thing that can come back: EC numbers. These are useful, in that they are clean.
       But they're identifiers for enzymes.  Yes, an enzyme is a chemical_substance too, but it's not really what
       we're trying to do here.  Nevertheless, let's hang onto them. We dump them and then if we want to handle
       later we can. There are about 10000 that come back with EC...
       EC:   10000

       Of course, the usefulness of these approaches is inverse with the frequency of their occurence:
       UNII: 14545
       CAS:  60880
       0:    190966"""
    #This is just a way to cache some slow work so you can come back to it dig around without re-running things.
    umfname = os.path.join(os.path.dirname (__file__),'downloads', 'unmapped.pickle')
    mcfname = os.path.join(os.path.dirname (__file__),'downloads', 'meshcas.pickle')
    mufname = os.path.join(os.path.dirname (__file__),'downloads', 'meshunii.pickle')
    ecfname = os.path.join(os.path.dirname (__file__),'downloads', 'meschec.pickle')
    labelname = os.path.join(os.path.dirname(__file__),'downloads', 'meshlabels.pickle')
    if deep_refresh:
        unmapped_mesh, term_to_cas, term_to_unii, term_to_EC, labels = \
            parse_mesh(pull_via_ftp('ftp.nlm.nih.gov','/online/mesh/rdf', 'mesh.nt.gz',decompress_data=True))
        with open(umfname,'wb') as um:
            pickle.dump(unmapped_mesh,um)
        with open(mcfname,'wb') as mc:
            pickle.dump(term_to_cas,mc)
        with open(mufname,'wb') as mu:
            pickle.dump(term_to_unii,mu)
        with open(ecfname,'wb') as mec:
            pickle.dump(term_to_EC,mec)
        with open(labelname,'wb') as ml:
            pickle.dump(labels,ml)
    else:
        with open(umfname,'rb') as um:
            unmapped_mesh=pickle.load(um)
        with  open(mcfname,'rb') as mc:
            term_to_cas=pickle.load(mc)
        with open(mufname,'rb') as mu:
            term_to_unii=pickle.load(mu)
        with open(ecfname,'rb') as mec:
            term_to_EC=pickle.load(mec)

    #mesh_to_unii is one of the files read by chemicals.py
    dump_dict(term_to_unii,'mesh_to_unii.txt')
    dump_dict(term_to_EC,'mesh_to_EC.txt')
    #mesh_to_pubchem is one of the files that chemicals.py is looking for.
    api_key = get_api_key()
    term_to_pubchem_by_cas = lookup_by_cas(term_to_cas,api_key)
    term_to_pubchem_by_mesh = lookup_by_mesh(unmapped_mesh,api_key)
    term_to_pubchem = {**term_to_pubchem_by_cas, **term_to_pubchem_by_mesh}
    dump_dict(term_to_pubchem,'mesh_to_pubchem.txt')

def get_api_key():
    return os.environ.get('EUTILS_API_KEY',default=None)

if __name__ == '__main__':
    refresh_mesh_pubchem(deep_refresh = False)

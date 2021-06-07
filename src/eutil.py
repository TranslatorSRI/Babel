import itertools
import os
from src.babel_utils import ThrottledRequester

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
        print(xterms)
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
                    print('found one',remesh,cids[0])
                    continue
        done += chunksize
        if done % 1000 == 0:
            print(f' completed {done} / {num}')
    if len(term_to_pubs) == 0:
        'We found no MESH/NCBI links, which is wrong.  Stopping.'
        exit()
    print(f'mesh found {len(term_to_pubs)}')
    return term_to_pubs


def get_api_key():
    return os.environ.get('EUTILS_API_KEY',default=None)

if __name__ == '__main__':
    lookup(['MESH:D004926'])
from src.babel_utils import pull_via_urllib

def pull_omim():
    pull_via_urllib('https://www.omim.org/static/omim/data/','mim2gene.txt', subpath='OMIM', decompress=False)

#OMIM doesn't define labels or synonyms, but uses those from HGNC and other orgs.

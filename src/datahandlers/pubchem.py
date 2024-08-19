from src.prefixes import PUBCHEMCOMPOUND
from src.babel_utils import make_local_name, pull_via_ftp, pull_via_urllib
import gzip
import requests
import json

def pull_pubchem():
    files = ['CID-MeSH','CID-Synonym-filtered.gz','CID-Title.gz']
    pull(files)

def pull_pubchem_structures():
    files = ['CID-InChI-Key.gz','CID-SMILES.gz']
    pull(files)

def pull(files):
    for f in files:
<<<<<<< Updated upstream
        pull_via_urllib('https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras', f, subpath=PUBCHEMCOMPOUND)
=======
        outfile=f'{PUBCHEMCOMPOUND}/{f}'
        pull_via_urllib('https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/', f, outfilename=outfile)
>>>>>>> Stashed changes

def pull_rxnorm_annotations(outname):
    pagenum = 1
    base_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=NLM%20RxNorm%20Terminology&heading_type=Compound&heading=RXCUI&page={pagenum}&response_type=save&response_basename=PubChemAnnotations_NLM%20RxNorm%20Terminology_heading%3DRXCUI"
    response = requests.get(base_url)
    base_response = response.json()
    n_pages = base_response["Annotations"]["TotalPages"]
    for pagenum in range(2,n_pages+1):
        base_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON/?source=NLM%20RxNorm%20Terminology&heading_type=Compound&heading=RXCUI&page={pagenum}&response_type=save&response_basename=PubChemAnnotations_NLM%20RxNorm%20Terminology_heading%3DRXCUI"
        response = requests.get(base_url)
        new_results = response.json()
        base_response["Annotations"]["Annotation"] += new_results["Annotations"]["Annotation"]
    with open(outname,"w") as outf:
        outf.write(json.dumps(base_response,indent=4))

def make_labels_or_synonyms(infile,outfile):
    with gzip.open(infile, 'r') as inf, open(outfile,'w') as outf:
        for l in inf:
            line = l.decode('latin1')
            x = line.strip().split('\t')
            outf.write(f'{PUBCHEMCOMPOUND}:{x[0]}\t{x[1]}\n')


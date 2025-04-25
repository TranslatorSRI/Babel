from src.prefixes import PUBCHEMCOMPOUND
from src.babel_utils import pull_via_wget
from src.properties import PrefixPropertyStore
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
        pull_via_wget('https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/', f, decompress=False, subpath=PUBCHEMCOMPOUND)

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

def pull_pubchem_labels(infile, labelfile):
    with PrefixPropertyStore(prefix=PUBCHEMCOMPOUND, autocommit=False) as pps:
        with open(labelfile, 'w') as outf, gzip.open(infile,mode='rt',encoding='latin-1') as inf:
            pps.begin_transaction()
            for line in inf:
                x = line.strip().split('\t')
                pps.insert_values(curie=x[0], prop='label', values=[x[1]], source="datacollect.py:pull_pubchem_labels()")
            pps.commit_transaction()
            pps.to_tsv(outf)

def pull_pubchem_synonyms(infile, synonymfile):
    with PrefixPropertyStore(prefix=PUBCHEMCOMPOUND, autocommit=False) as pps:
        with open(synonymfile, 'w') as outf, gzip.open(infile,mode='rt',encoding='latin-1') as inf:
            pps.begin_transaction()
            for line in inf:
                x = line.strip().split('\t')
                if x[1].startswith('CHEBI'):
                    continue
                if x[1].startswith('SCHEMBL'):
                    continue
                pps.insert_values(curie=x[0], prop='hasRelatedSynonym', values=[x[1]], source="datacollect.py:pull_pubchem_synonyms()")
            pps.commit_transaction()
            pps.to_tsv(outf, include_properties=True)

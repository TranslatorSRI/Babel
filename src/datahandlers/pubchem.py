from src.babel_utils import make_local_name, pull_via_ftp
import json

def pull_pubchem():
    files = ['CID-MeSH','CID-Synonym-filtered.gz','CID-Title.gz']
    for f in files:
        outfile=f'PUBCHEM/{f}'
        pull_via_ftp('ftp.ncbi.nlm.nih.gov', '/pubchem/Compound/Extras', 'f', outfilename=outfile)

def get_pubchem_labels_and_synonyms(infile,labelfile,synfile):
    labels = {}
    with gzip.open(outfname, 'rt') as in_file:
        for line in in_file:
            # since the synonyms are weighted already will just pick the first one.
            l = line.strip()
            cid, label = l.split('\t')
            if f'PUBCHEM.COMPOUND:{cid}' in labels:
                continue
            labels[f'PUBCHEM.COMPOUND:{cid}'] = label
    label_compounds(concord, 'PUBCHEM.COMPOUND', partial(get_dict_label, labels= labels))

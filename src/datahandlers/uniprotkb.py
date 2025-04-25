import csv
import logging
import os

import requests
from src.babel_utils import make_local_name


def readlabels(which):
    swissname = make_local_name(f'UniProtKB/uniprot_{which}.fasta')
    swissprot_labels = {}
    with open(swissname,'r') as inf:
        for line in inf:
            if line.startswith('>'):
                #example fasta line:
                #>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1
                x = line.split('|')
                uniprotid = f'UniProtKB:{x[1]}'
                name = x[2].split(' OS=')[0]
                swissprot_labels[uniprotid] = f'{name} ({which})'
    return swissprot_labels

def pull_uniprot_labels(sprotfile,tremblfile,fname):
    slabels = readlabels('sprot')
    tlabels = readlabels('trembl')
    with open(fname,'w') as labelfile:
        for k,v in slabels.items():
            labelfile.write(f'{k}\t{v}\n')
        for k,v in tlabels.items():
            labelfile.write(f'{k}\t{v}\n')


def download_umls_gene_protein_mappings(umls_uniprotkb_raw_url, umls_uniprotkb_filename, umls_gene_concords, umls_protein_concords):
    """
    Chris Bizon generated a list of UMLS to NCBIGene/UniProtKB mappings in
    https://github.com/cbizon/UMLS_UniProtKB. This function downloads this file
    from that GitHub repository and generates concord files so that they can be
    incorporated into our gene and protein cliques.

    :param umls_uniprotkb_raw_url: The URL to download the UMLS/UniProtKB mapping file.
    :param umls_uniprotkb_filename: The UMLS/UniProtKB filename to save the UMLS/UniProtKB filename.
    :param umls_gene_concords: The file to write UMLS/NCBIGene gene concords to.
    :param umls_uniprotkb_protein_concords: The file to write UMLS/UniProtKB protein concords to.
    """

    RELATION = 'oio:closeMatch'

    # Step 1. Download the file.
    response = requests.get(umls_uniprotkb_raw_url)
    response.raise_for_status()
    with open(umls_uniprotkb_filename, 'w') as f:
        f.write(response.text)

    # Step 2. Read the file into memory.
    os.makedirs(os.path.dirname(umls_gene_concords), exist_ok=True)
    os.makedirs(os.path.dirname(umls_protein_concords), exist_ok=True)

    count_rows = 0
    with open(umls_uniprotkb_filename, 'r') as f, \
        open(umls_gene_concords, 'w') as genef, \
        open(umls_protein_concords, 'w') as proteinf:
        csv_reader = csv.DictReader(f, dialect='excel-tab')
        for row in csv_reader:
            count_rows += 1
            if row.keys() != {'UMLS_protein', 'UMLS_gene', 'NCBI_gene', 'UniProtKB'}:
                raise RuntimeError(f"Format of the UniProtKB download from {umls_uniprotkb_raw_url} has changed: {csv_reader.fieldnames}.")

            genef.write(f"{row['UMLS_gene']}\t{RELATION}\t{row['NCBI_gene']}\n")
            proteinf.write(f"{row['UMLS_protein']}\t{RELATION}\t{row['UniProtKB']}\n")

    logging.info(f"Downloaded UMLS file from {umls_uniprotkb_raw_url} and added {count_rows} to gene and protein concords.")
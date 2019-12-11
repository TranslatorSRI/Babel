from ftplib import FTP
from io import BytesIO
from json import loads
from gzip import decompress
import os
import logging

import jsonlines

from babel.node import create_node
from src.LabeledID import LabeledID
from src.util import LoggingUtil

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

def pull_via_ftp(ftpsite, ftpdir, ftpfile):
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    with BytesIO() as data:
        ftp.retrbinary(f'RETR {ftpfile}', data.write)
        binary = data.getvalue()
    ftp.quit()
    return binary

def pull_hgnc_json():
    """Get the HGNC json file & convert to python"""
    data = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/json', 'hgnc_complete_set.json')
    hgnc_json = loads( data.decode() )
    return hgnc_json

def pull_uniprot_kb():
    data = pull_via_ftp('ftp.ebi.ac.uk','/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/', 'HUMAN_9606_idmapping.dat.gz')
    tabs = decompress(data).decode()
    return tabs

def json_2_identifiers(gene_dict):
    symbol = gene_dict['symbol']
    hgnc_id = LabeledID(identifier=gene_dict['hgnc_id'], label=symbol)
    hgnc_symbol = LabeledID(identifier=f"HGNC.SYMBOL:{symbol}", label=symbol)
    idset = set([hgnc_id, hgnc_symbol])
    if 'entrez_id' in gene_dict:
        idset.add( LabeledID(identifier=f"NCBIGENE:{gene_dict['entrez_id']}", label=symbol))
    #There's a strong debate to be had about whether UniProtKB id's belong with genes
    # or with proteins.  In SwissProt, an identifier is meant to be 1:1 with a gene.
    # In my mind, that makes it a gene.  So we will continue to group UniProtKB with them
    #For individual protein sequences, or peptide sequences, we will make them gene_products.
    #Also generate a PR identifier for each from the uniprot id (PR uses uniprot ids for uniprot things)
    if 'uniprot_ids' in gene_dict:
        idset.update([LabeledID(identifier=f"UniProtKB:{uniprotkbid}", label=symbol) for uniprotkbid in gene_dict['uniprot_ids']])
        idset.update([LabeledID(identifier=f"PR:{uniprotkbid}", label=symbol) for uniprotkbid in gene_dict['uniprot_ids']])
    if 'ensembl_gene_id' in gene_dict:
        idset.add( LabeledID(identifier=f"ENSEMBL:{gene_dict['ensembl_gene_id']}", label=symbol))
    if 'iuphar' in gene_dict:
        if gene_dict['iuphar'].startswith('objectId'):
            gid = gene_dict['iuphar'].split(':')[1]
            idset.add( LabeledID(identifier=f'IUPHAR:{gid}', label=symbol) )
    #1. Enzymes aren't really genes
    #2. Even if they were, the mapping in this file is kind of crappy
    #if 'enzyme_id' in gene_dict:
    #    for eid in gene_dict['enzyme_id']:
    #        idset.add( LabeledID(identifier=f'EC:{eid}',label=symbol ) )
    return idset

def load_genes():
    """
    """
    synonyms = synonymize_genes()
    cdir = os.path.dirname(os.path.abspath(__file__))
    with jsonlines.open(os.path.join(cdir,'geneout.txt'),'w') as outf:
        for gene in synonyms:
            outf.write( create_node(identifiers=gene, node_type='gene') )
    logger.debug(f'Added {len(synonyms)} gene symbols to the cache')

def synonymize_genes():
    """
    """
    ids_to_synonyms = {}
    hgnc = pull_hgnc_json()
    hgnc_genes = hgnc['response']['docs']
    logger.debug(f' Found {len(hgnc_genes)} genes in HGNC')
    hgnc_identifiers = [ json_2_identifiers(gene) for gene in hgnc_genes ]
    return hgnc_identifiers
    #for idset in hgnc_identifiers:
    #    for lid in idset:
    #        ids_to_synonyms[lid.identifier] = idset
    #return ids_to_synonyms

    #This might get pushed into gene_product?
    #tabs = pull_uniprot_kb()
    #lines = tabs.split('\n')
    #logger.debug(f'Found {len(lines)} lines in the uniprot data')
    #uniprots = defaultdict(dict)
    #for line in lines:
    #    x = line.split('\t')
    #    if len(x) < 3:
    #        continue
    #    uniprots[x[0]][x[1]] = x[2]
    #premapped = 0
    #isoforms = 0
    #unpremapped = 0
    #hgnc_mapped = 0
    #entrez_mapped = 0
    #still_unmapped = 0
    #for up in uniprots:
    #    uniprot_id = f"UniProtKB:{up}"
    #    if uniprot_id in ids_to_synonyms:
    #        #great, we already know about this one
    #        premapped += 1
    #    elif '-' in up:
    #        isoforms += 1
    #    else:
    #        unpremapped += 1
    #        #Can we map it with HGNC?
    #        if ('HGNC' in uniprots[up]) and (uniprots[up]['HGNC'] in ids_to_synonyms):
    #            synonyms = ids_to_synonyms[uniprots[up]['HGNC']]
    #            synonyms.add(uniprot_id)
    #            ids_to_synonyms[uniprot_id] = synonyms
    #            hgnc_mapped += 1
    #        elif ('GeneID' in uniprots[up]) and (f"NCBIGENE:{uniprots[up]['GeneID']}" in ids_to_synonyms):
    #            #no? How about Entrez?
    #            entrez = f"NCBIGENE:{uniprots[up]['GeneID']}"
    #            synonyms = ids_to_synonyms[entrez]
    #            synonyms.add(uniprot_id)
    #            ids_to_synonyms[uniprot_id] = synonyms
    #            entrez_mapped += 1
    #        else:
    #            #Oh well.  There are a lot of other keys, but they don't overlap the HGNC Keys
    #            #We're still going to toss the uniprotkb in there, because, we're going to end up looking for
    #            # it later anyway
    #            still_unmapped += 1
    #            ids_to_synonyms[uniprot_id] = set([LabeledID(identifier=uniprot_id, label=None)])
    #logger.debug(f'There were {premapped} UniProt Ids already mapped in HGNC')
    #logger.debug(f'There were {isoforms} UniProt Ids that are just isoforms')
    #logger.debug(f'There were {unpremapped} UniProt Ids not already mapped in HGNC')
    #logger.debug(f'There were {hgnc_mapped} Mapped using HGNC notes in UniProt')
    #logger.debug(f'There were {entrez_mapped} Mapped using Entrez in UniProt')
    #logger.debug(f'There were {still_unmapped} UniProt Ids left that we are keeping as solos')
    #return ids_to_synonyms

#def load_annotations_genes(rosetta):
#    """
#    For now building annotation data using HGNC data.
#    """
#    hgnc_genes = pull_hgnc_json()['response']['docs']
#    gene_annotator = GeneAnnotator(rosetta)
#    logger.debug('Pulled hgnc data for gene annotations')
#    for gene in hgnc_genes:
#        extract = gene_annotator.extract_annotation_from_hgnc(gene, gene_annotator.get_prefix_config('HGNC')['keys'])
#        key = f"annotation({Text.upper_curie(gene['hgnc_id'])})"
#        logger.debug(f'Caching {key} {extract}')
#        rosetta.cache.set(key, extract)
#    logger.debug(f"There were {len(hgnc_genes)} HGNC Annotations")
#
#    ensembl_annotations = gene_annotator.get_all_ensembl_gene_annotations()
#    for ensembl_id, annotations in ensembl_annotations.items():
#        key = f"annotation(ENSEMBL:{ensembl_id})"
#        logger.debug(f'Caching {key} {annotations}')
#        rosetta.cache.set(key, annotations)
#    logger.debug(f"There were {len(ensembl_annotations)} Ensembl Annotations")

if __name__ == '__main__':
    load_genes()
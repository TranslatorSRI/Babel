from src.prefixes import OMIM,ENSEMBL,NCBIGENE,WORMBASE, MGI, ZFIN, DICTYBASE, FLYBASE, RGD, SGD, HGNC, UMLS, PR
from src.categories import GENE, PROTEIN

import src.datahandlers.umls as umls
import src.datahandlers.obo as obo

from src.babel_utils import read_identifier_file,glom,write_compendium

import os
import json
import gzip

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def write_ensembl_ids(ensembl_dir, outfile):
    with open(outfile,'w') as outf:
        #find all the ensembl directories
        dirlisting = os.listdir(ensembl_dir)
        for dl in dirlisting:
            dlpath = os.path.join(ensembl_dir,dl)
            if os.path.isdir(dlpath):
                infname = os.path.join(dlpath,'BioMart.tsv')
                if os.path.exists(infname):
                    #open each ensembl file, find the id column, and put it in the output
                    with open(infname,'r') as inf:
                        h = inf.readline()
                        x = h[:-1].split('\t')
                        column = x.index('Gene stable ID')
                        for line in inf:
                            x = line[:-1].split('\t')
                            outf.write(f'{ENSEMBL}:{x[column]}\n')

def write_hgnc_ids(infile,outfile):
    with open(infile,'r') as inf:
        hgnc_json = json.load(inf)
    with open(outfile,'w') as outf:
        for gene in hgnc_json['response']['docs']:
            outf.write(f"{gene['hgnc_id']}\n")


def write_omim_ids(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            if line.startswith('#'):
                continue
            chunks = line.split('\t')
            if chunks[1] == 'gene':
                outf.write(f'{OMIM}:{chunks[0]}\n')

def write_umls_ids(outfile):
    umlsmap = {}
    umlsmap['A1.2.3.5'] = GENE
    #Do I want this?  There are a bunch of things under here that we probably don't want.
    blacklist=set(['C0017361', #recessive genes
                   'C0017346', #Gag viral gene family
                    ])
    umls.write_umls_ids(umlsmap, outfile, blacklist)

def write_pr_ids(outfile):
    protein_id   = f'{PR}:000000001'
    obo.write_obo_ids([(protein_id, PROTEIN)], outfile, [PROTEIN])

def build_gene_ncbi_ensemble_relationships(infile,outfile):
    with gzip.open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline()
        last = ('','')
        for line in inf:
            x = line.strip().split()
            ncbigene_id = f'{NCBIGENE}:{x[2]}'
            ensembl_id = f'{ENSEMBL}:{x[3]}'
            new = (ncbigene_id,ensembl_id)
            if new == last:
                continue
            outf.write(f'{ncbigene_id}\teq\t{ensembl_id}\n')
            last=new

def build_gene_ncbigene_xrefs(infile,outfile):
    mappings = {'WormBase': WORMBASE, 'FLYBASE': FLYBASE, 'ZFIN': ZFIN,
                'HGNC': HGNC, 'MGI': MGI, 'RGD': RGD, 'dictyBase': DICTYBASE,
                'SGD': SGD }
    with gzip.open(infile, 'r') as inf, open(outfile, 'w') as outf:
        h = inf.readline()
        for line in inf:
            x = line.decode('utf-8').strip().split('\t')
            ncbigene_id = f'{NCBIGENE}:{x[1]}'
            xrefs = x[5].split('|')
            for xref in xrefs:
                if xref == '-':
                    continue
                xref_parts = xref.split(':')
                found_prefix=xref_parts[0]
                if found_prefix in mappings:
                    outf.write(f'{ncbigene_id}\txref\t{mappings[found_prefix]}:{xref_parts[-1]}\n')

def build_gene_medgen_relationships(infile,outfile):
    with open(infile, 'r') as inf, open(outfile, 'w') as outf:
        h = inf.readline()
        for line in inf:
            x = line.strip().split('\t')
            if not x[2] == 'gene':
                continue
            ncbigene_id = f'{NCBIGENE}:{x[1]}'
            omim_id = f'{OMIM}:{x[0]}'
            outf.write(f'{ncbigene_id}\teq\t{omim_id}\n')
            #It looks like this never gets invoked - these columns are only filled in for phenotypes
            if not x[4] == '-':
                umls_id = f'{UMLS}:{x[4]}'
                outf.write(f'{ncbigene_id}\teq\t{umls_id}\n')

def build_gene_umls_hgnc_relationships(umls_idfile,outfile):
    #Could also add MESH, if that were a valid gene prefix
    umls.build_sets(umls_idfile, outfile, {'HGNC':HGNC})

def build_gene_compendia(concordances, identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    uniques = [NCBIGENE,HGNC,ENSEMBL,OMIM]
    for ifile in identifiers:
        print(ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes= uniques)
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print('loading', infile)
        pairs = []
        with open(infile, 'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append(set([x[0], x[2]]))
        glom(dicts, pairs, unique_prefixes=uniques)
    gene_sets = set([frozenset(x) for x in dicts.values()])
    write_compendium(gene_sets, f'{GENE}.txt', 'biolink:Gene', {})


def pull_prot(which,refresh):
    #swissname = pull_via_ftplib('ftp.uniprot.org','/pub/databases/uniprot/current_release/knowledgebase/complete/',f'uniprot_{which}.fasta.gz',decompress_data=True,outfilename=f'uniprot_{which}.fasta')
    if refresh:
        swissname = pull_via_urllib('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/',f'uniprot_{which}.fasta.gz')
    else:
        swissname = make_local_name(f'uniprot_{which}.fasta')
    swissprot_labels = {}
    nlines = 0
    maxn = 1000
    with open(swissname,'r') as inf:
        for line in inf:
            nlines += 1
            if line.startswith('>'):
                #example fasta line:
                #>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1
                x = line.split('|')
                uniprotid = f'UniProtKB:{x[1]}'
                name = x[2].split(' OS=')[0]
                swissprot_labels[uniprotid] = f'{name} ({which})'
            #if nlines > maxn:
            #    break
    print('numlines',nlines)
    print('nl',len(swissprot_labels))
    swissies = [ (k,) for k in swissprot_labels.keys() ]
    print('s',len(swissies))
    return swissies, swissprot_labels

def pull_prots(refresh_swiss=False,refresh_trembl=False):
    swiss,slabels = pull_prot('sprot',refresh_swiss)
    tremb,tlabels = pull_prot('trembl',refresh_trembl)
    slabels.update(tlabels)
    return swiss+tremb,slabels


def json_2_identifiers(gene_dict):
    symbol = gene_dict['symbol']
    labels = {}
    hgnc_id = gene_dict['hgnc_id']
    hgnc_symbol = f"HGNC.SYMBOL:{symbol}"
    idset = set([hgnc_id, hgnc_symbol])
    labels[hgnc_id]=symbol
    labels[hgnc_symbol]=symbol
    if 'entrez_id' in gene_dict:
        idset.add( f"NCBIGENE:{gene_dict['entrez_id']}")
    #There's a strong debate to be had about whether UniProtKB id's belong with genes
    # or with proteins.  In SwissProt, an identifier is meant to be 1:1 with a gene.
    # In my mind, that makes it a gene.  So we will continue to group UniProtKB with them
    #For individual protein sequences, or peptide sequences, we will make them gene_products.
    #Also generate a PR identifier for each from the uniprot id (PR uses uniprot ids for uniprot things)
    if 'uniprot_ids' in gene_dict:
        idset.update([f"UniProtKB:{uniprotkbid}" for uniprotkbid in gene_dict['uniprot_ids']])
        idset.update([f"PR:{uniprotkbid}" for uniprotkbid in gene_dict['uniprot_ids']])
    if 'ensembl_gene_id' in gene_dict:
        idset.add( f"ENSEMBL:{gene_dict['ensembl_gene_id']}")
    if 'iuphar' in gene_dict:
        if gene_dict['iuphar'].startswith('objectId'):
            gid = gene_dict['iuphar'].split(':')[1]
            idset.add( f'IUPHAR:{gid}' )
    return idset,labels

def load_genes():
    """
    Pull information about genes, create a compendium, and save it out.
    Currently, we use only HGNC mappings.  This has the main problem that it limits us to human genes.
    Next step: Instead of HGNC as the mapping of record, move to either uniprot or NCBI.
    Include names from sources as well...
    """
    synonyms,labels = synonymize_genes()
    synset =set([frozenset(x) for x in synonyms.values()]) 
    print(len(synset))
    write_compendium(synset,'gene_compendium.txt','biolink:Gene',labels=labels)

def synonymize_genes():
    """
    """
    ids_to_synonyms = {}
    hgnc = pull_hgnc_json()
    hgnc_genes = hgnc['response']['docs']
    logger.debug(f' Found {len(hgnc_genes)} genes in HGNC')
    hgnc_identifiers_and_labels = [ json_2_identifiers(gene) for gene in hgnc_genes ]
    hgnc_identifiers = []
    labels = {}
    for x in hgnc_identifiers_and_labels:
        hgnc_identifiers.append(x[0])
        labels.update(x[1])
    print(len(hgnc_identifiers))
    gene_comp =  {}
    glom(gene_comp,hgnc_identifiers)
    sp,spl = pull_prots()
    print(len(sp))
    glom(gene_comp,sp)
    labels.update(spl)    
    return gene_comp,labels

if __name__ == '__main__':
    load_genes()


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


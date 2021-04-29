from src.prefixes import OMIM,ENSEMBL,NCBIGENE,WORMBASE, MGI, ZFIN, DICTYBASE, FLYBASE, RGD, SGD, HGNC, UMLS
from src.categories import GENE

import src.datahandlers.umls as umls

from src.babel_utils import read_identifier_file,glom,write_compendium

import os
import json
import gzip

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def write_ensembl_ids(ensembl_dir, outfile):
    """Loop over all the ensembl species.  Find any protein-coding gene"""
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
                        wrote=set()
                        h = inf.readline()
                        x = h[:-1].split('\t')
                        gene_column = x.index('Gene stable ID')
                        protein_column = x.index('Protein stable ID')
                        for line in inf:
                            x = line[:-1].split('\t')
                            #Is it protein coding?
                            if x[protein_column] == '':
                                continue
                            gid = f'{ENSEMBL}:{x[gene_column]}'
                            #The gid is not unique, so don't write the same one over again
                            if gid in wrote:
                                continue
                            wrote.add(gid)
                            outf.write(f'{gid}\n')

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

def read_ncbi_idfile(ncbi_idfile):
    ncbi_ids = set()
    with open(ncbi_idfile,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')[0]
            ncbi_ids.add(x)
    return ncbi_ids

def build_gene_ncbi_ensemble_relationships(infile,ncbi_idfile,outfile):
    ncbi_ids = read_ncbi_idfile(ncbi_idfile)
    with gzip.open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline()
        last = ('','')
        for line in inf:
            x = line.strip().split()
            ncbigene_id = f'{NCBIGENE}:{x[1]}'
            if ncbigene_id not in ncbi_ids:
                continue
            ensembl_id = f'{ENSEMBL}:{x[2]}'
            new = (ncbigene_id,ensembl_id)
            if new == last:
                continue
            outf.write(f'{ncbigene_id}\teq\t{ensembl_id}\n')
            last=new

def build_gene_ncbigene_xrefs(infile,ncbi_idfile,outfile):
    mappings = {'WormBase': WORMBASE, 'FLYBASE': FLYBASE, 'ZFIN': ZFIN,
                'HGNC': HGNC, 'MGI': MGI, 'RGD': RGD, 'dictyBase': DICTYBASE,
                'SGD': SGD }
    ncbi_ids = read_ncbi_idfile(ncbi_idfile)
    with gzip.open(infile, 'r') as inf, open(outfile, 'w') as outf:
        h = inf.readline()
        for line in inf:
            x = line.decode('utf-8').strip().split('\t')
            ncbigene_id = f'{NCBIGENE}:{x[1]}'
            if ncbigene_id not in ncbi_ids:
                continue
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
    baretype = GENE.split(':')[-1]
    write_compendium(gene_sets, f'{baretype}.txt', GENE, {})


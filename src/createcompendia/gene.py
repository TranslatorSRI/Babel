import re

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

def write_mods_ids(dd,id,modlist):
    for mod in modlist:
        with open(f'{dd}/{mod}/labels','r') as inf, open(f'{id}/gene/ids/{mod}','w') as outf:
            for line in inf:
                x = line.split('\t')[0]
                outf.write(f'{x}\n')

def build_gene_ensembl_relationships(ensembl_dir, outfile):
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
                        column_to_prefix = { 'NCBI gene (formerly Entrezgene) ID': {NCBIGENE},
                                             'ZFIN ID': {ZFIN},
                                             'SGD gene name ID': {SGD},
                                             'WormBase Gene ID': {WORMBASE},
                                             'FlyBase ID': {FLYBASE},
                                             'MGI ID': {MGI},
                                             'RGD ID': {RGD}
                                             }
                        protein_column = x.index('Protein stable ID')
                        columnno_to_prefix = {}
                        for i,v in enumerate(x):
                            if v in column_to_prefix:
                                columnno_to_prefix[i] = column_to_prefix[v]
                        for line in inf:
                            x = line[:-1].split('\t')
                            #Is it protein coding?
                            #Protein coding is not actually relevant.
                            #if x[protein_column] == '':
                            #    continue
                            gene_id = x[gene_column]
                            gid = f'{ENSEMBL}:{gene_id}'
                            for cno,pref in columnno_to_prefix.items():
                                value = x[cno]
                                if len(value) > 0:
                                    outf.write(f'{gid}\teq\t{pref}:{value}\n')

                                    # If the ENSEMBL ID is a version string (e.g. ENSEMBL:ENSP00000263368.3),
                                    # then we should indicate that this is identical to the non-versioned string
                                    # as well.
                                    # See https://github.com/TranslatorSRI/Babel/issues/72 for details.
                                    res = re.match(r"^([A-Z]+\d+)\.\d+", gene_id)
                                    if res:
                                        ensembl_id_without_version = res.group(1)
                                        outf.write(f'{ENSEMBL}:{ensembl_id_without_version}\teq\t{gid}\n')

def write_zfin_ids(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split()
            if 'GENE' in x[0]:
                outf.write(f'{ZFIN}:{x[0]}')

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
    """Find the UMLS entities that are genes.  This is complicated by the fact that UMLS  semantic type doesn't
    have a corresponding GENE class.  It has something (A1.2.3.5) which includes genes, but also includes genomes and
    variants and gene properties and gene families.  We can do some filtering by looking around in the MRCONSO as well
    as the MRSTY. In particular, if the term maps to an OMIM that has a period in it, then it's a variant. Good job
    UMLS, it's not like genes are central to biology or anything.
    Also, remove anything that in the label identifies itself as an Allele or Mutation
    It's possible in the future that we'd like to try to assign better classes to some of these things."""


    #Do I want this?  There are a bunch of things under here that we probably don't want.
    blacklist=set(['C0017361', #recessive genes
                   'C0017346', #Gag viral gene family
                    ])
    mrsty = os.path.join('input_data', 'private', 'MRSTY.RRF')
    umls_keepers = set()
    with open(mrsty, 'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            cat = x[2]
            if cat == 'A1.2.3.5':
                umls_keepers.add(x[0])
    umls_keepers.difference_update(blacklist)
    #Now filter out OMIM variants
    mrconso = os.path.join('input_data', 'private', 'MRCONSO.RRF')
    with open(mrconso,'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            cui = x[0]
            if cui not in umls_keepers:
                continue
            lang = x[1]
            #Only keep english terms
            if lang != 'ENG':
                continue
            #only keep unsuppressed rows
            suppress = x[16]
            if suppress == 'O' or suppress == 'E':
                continue
            #only keep sources we're looking for
            source = x[11]
            if source == 'OMIM':
                value = x[13]
                if "." in value:
                    umls_keepers.remove(x[0])
            if 'Allele' in x[14] or 'Mutation' in x[14]:
                umls_keepers.remove(x[0])
    with open(outfile,'w') as outf:
        for umls in umls_keepers:
            outf.write(f'{UMLS}:{umls}\t{GENE}\n')

def read_ncbi_idfile(ncbi_idfile):
    ncbi_ids = set()
    with open(ncbi_idfile,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')[0]
            ncbi_ids.add(x)
    return ncbi_ids

def build_gene_ncbi_ensembl_relationships(infile,ncbi_idfile,outfile):
    ncbi_ids = read_ncbi_idfile(ncbi_idfile)
    with gzip.open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline()
        last = ('','')
        for line in inf:
            x = line.decode('utf-8').strip().split('\t')
            ncbigene_id = f'{NCBIGENE}:{x[1]}'
            if ncbigene_id not in ncbi_ids:
                continue
            ensembl_id = f'{ENSEMBL}:{x[2]}'
            new = (ncbigene_id,ensembl_id)
            if new == last:
                continue
            outf.write(f'{ncbigene_id}\teq\t{ensembl_id}\n')
            last=new

            # If the ENSEMBL ID is a version string (e.g. ENSEMBL:ENSP00000263368.3),
            # then we should indicate that this is identical to the non-versioned string
            # as well.
            # See https://github.com/TranslatorSRI/Babel/issues/72 for details.
            res = re.match(r"^([A-Z]+\d+)\.\d+", x[2])
            if res:
                ensembl_id_without_version = res.group(1)
                outf.write(f'{ncbigene_id}\teq\t{ENSEMBL}:{ensembl_id_without_version}\n')

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
                            #if x[protein_column] == '':
                            #    continue
                            gid = f'{ENSEMBL}:{x[gene_column]}'
                            #The gid is not unique, so don't write the same one over again
                            if gid in wrote:
                                continue
                            wrote.add(gid)
                            outf.write(f'{gid}\n')


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
        print('loading',ifile)
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


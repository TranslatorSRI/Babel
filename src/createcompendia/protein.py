from src.prefixes import ENSEMBL, UMLS, PR, UNIPROTKB
from src.categories import PROTEIN

import src.datahandlers.umls as umls
import src.datahandlers.obo as obo
from src.ubergraph import UberGraph

from src.babel_utils import read_identifier_file,glom,write_compendium,Text

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

def write_umls_ids(outfile):
    umlsmap = {}
    umlsmap['A1.4.1.2.1.7'] = PROTEIN
    umls.write_umls_ids(umlsmap, outfile)

def write_pr_ids(outfile):
    protein_id   = f'{PR}:000000001'
    obo.write_obo_ids([(protein_id, PROTEIN)], outfile, [PROTEIN])


def write_ensembl_ids(ensembl_dir, outfile):
    """Loop over all the ensembl species.  Find any protein-coding gene"""
    with open(outfile, 'w') as outf:
        # find all the ensembl directories
        dirlisting = os.listdir(ensembl_dir)
        for dl in dirlisting:
            dlpath = os.path.join(ensembl_dir, dl)
            if os.path.isdir(dlpath):
                infname = os.path.join(dlpath, 'BioMart.tsv')
                if os.path.exists(infname):
                    # open each ensembl file, find the id column, and put it in the output
                    with open(infname, 'r') as inf:
                        wrote = set()
                        h = inf.readline()
                        x = h[:-1].split('\t')
                        protein_column = x.index('Protein stable ID')
                        for line in inf:
                            x = line[:-1].split('\t')
                            if x[protein_column] == '':
                                continue
                            pid = f'{ENSEMBL}:{x[protein_column]}'
                            # The pid is not unique, so don't write the same one over again
                            if pid in wrote:
                                continue
                            wrote.add(pid)
                            outf.write(f'{pid}\n')

def build_pr_uniprot_relationships(outfile, ignore_list = []):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri.  Write these lists to concord files, indexed by the prefix"""
    iri = 'PR:000000001'
    uber = UberGraph()
    pro_res = uber.get_subclasses_and_xrefs(iri)
    with open(outfile,'w') as concfile:
        for k,v in pro_res.items():
            for x in v:
                if Text.get_curie(x) not in ignore_list:
                    if k.startwith('PR'):
                        concfile.write(f'{k}\txref\t{x}\n')

def build_protein_uniprotkb_ensemble_relationships(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split()
            if x[1] == 'Ensembl_PRO':
                uniprot_id = f'{UNIPROTKB}:{x[0]}'
                ensembl_id = f'{ENSEMBL}:{x[2]}'
                outf.write(f'{uniprot_id}\teq\t{ensembl_id}\n')

def build_protein_compendia(concordances, identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    uniques = [UNIPROTKB,PR]
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
    baretype = PROTEIN.split(':')[-1]
    write_compendium(gene_sets, f'{baretype}.txt', PROTEIN, {})


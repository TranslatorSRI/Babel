import re

from src.prefixes import ENSEMBL, UMLS, PR, UNIPROTKB, NCIT, NCBITAXON
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
logger = LoggingUtil.init_logging(__name__, level=logging.WARNING)


def extract_taxon_ids_from_uniprotkb(idmapping_filename, uniprotkb_taxa_filename):
    """ Extract NCBIGene identifiers from the UniProtKB mapping file. """
    with open(idmapping_filename, 'r') as inf, open(uniprotkb_taxa_filename, 'w') as outf:
        for line in inf:
            x = line.strip().split('\t')
            if x[1] == 'NCBI_TaxID':
                if x[0] == '' or x[2] == '':
                    logger.warning(f'Line {x} is an NCBI_TaxID but has a blank UniProtKB ({x[0]}) or NCBITaxon ({x[2]}), skipping.')
                    continue
                outf.write(f'{UNIPROTKB}:{x[0]}\t{NCBITAXON}:{x[2]}\n')


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

def write_umls_ids(mrsty, outfile):
    umlsmap = {}
    umlsmap['A1.4.1.2.1.7'] = PROTEIN
    umls.write_umls_ids(mrsty, umlsmap, outfile)

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
                print(f'write_ensembl_ids for input filename {infname}')
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
                    if k.startswith('PR'):
                        concfile.write(f'{k}\txref\t{x}\n')

def build_protein_uniprotkb_ensemble_relationships(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split()
            if x[1] == 'Ensembl_PRO':
                uniprot_id = f'{UNIPROTKB}:{x[0]}'
                ensembl_id = f'{ENSEMBL}:{x[2]}'
                outf.write(f'{uniprot_id}\teq\t{ensembl_id}\n')

                # If the ENSEMBL ID is a version string (e.g. ENSEMBL:ENSP00000263368.3),
                # then we should indicate that this is identical to the non-versioned string
                # as well.
                # See https://github.com/TranslatorSRI/Babel/issues/72 for details.
                res = re.match(r"^([A-Z]+\d+)\.\d+", x[2])
                if res:
                    ensembl_id_without_version = res.group(1)
                    outf.write(f'{ensembl_id}\teq\t{ENSEMBL}:{ensembl_id_without_version}\n')


def build_ncit_uniprot_relationships(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            # These lines are sometimes empty (I think because the
            # input file can have DOS line endings). If so, we can
            # skip those.
            stripped_line = line.strip()
            if stripped_line == '':
                logger.info(f"Skipping empty line in {infile}")
                continue
            x = stripped_line.split()
            ncit_id = f'{NCIT}:{x[0]}'
            uniprot_id = f'{UNIPROTKB}:{x[1]}'
            outf.write(f'{ncit_id}\teq\t{uniprot_id}\n')

def build_umls_ncit_relationships(mrconso, idfile, outfile):
    umls.build_sets(mrconso, idfile, outfile, {'NCI': NCIT})

def build_protein_compendia(concordances, metadata_yamls, identifiers, icrdf_filename):
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
            for line_index, line in enumerate(inf):
                # if line_index % 10000 == 0:
                #     print("Loaded line count", line_index)
                x = line.strip().split('\t')
                pairs.append(set([x[0], x[2]]))
        # print("glomming", infile) # This takes a while, but doesn't add much to the memory
        glom(dicts, pairs, unique_prefixes=uniques)
        print("glommed", infile)
    # print("merging dicts") # This seems to increase memory usage slightly.
    gene_sets = set([frozenset(x) for x in dicts.values()])
    print("merged dicts", infile)
    #Try to preserve some memory here.
    dicts.clear()

    # Memory usage falls at some point; maybe here?
    # TODO: might be a good idea to write all of this out in one step and
    # only then generate the compendium from those input files.

    baretype = PROTEIN.split(':')[-1]
    write_compendium(metadata_yamls, gene_sets, f'{baretype}.txt', PROTEIN, {}, icrdf_filename=icrdf_filename)


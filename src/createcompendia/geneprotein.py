from src.prefixes import ENSEMBL, PR, UNIPROTKB, NCBIGENE
from src.categories import GENEORPROTEIN
from src.babel_utils import read_identifier_file,glom,write_compendium,Text

import jsonlines

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def build_uniprotkb_ncbigene_relationships(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split()
            if x[1] == 'GeneID':
                uniprot_id = f'{UNIPROTKB}:{x[0]}'
                ncbigene_id = f'{NCBIGENE}:{x[2]}'
                outf.write(f'{uniprot_id}\trelated_to\t{ncbigene_id}\n')

def merge(gene,protein):
    """We have two objects, one represents a gene, one a protein.  We want to create a combined something."""
    geneprotein = {}
    #Use the gene's ID.
    geneprotein['id'] = gene['id']
    #there shouldn't be any overlap here, so we can just concatenate
    geneprotein['equivalent_identifiers'] = gene['equivalent_identifiers'] + protein['equivalent_identifiers']
    #Now, we need to slightly modify the types. Not sure this is good, but maybe it is?
    geneprotein['type'] = ['biolink:Gene'] + protein['type']
    return geneprotein

def build_compendium(gene_compendium, protein_compendium, geneprotein_concord, outfile):
    """Gene and Protein are both pretty big, and we want this to happen somewhat easily.
    Fortunately our concord is in terms of the two preferred ids.
    So first we load in that concord.
    Then we load in the genes.  If we don't have the gene in our concord, we immediately just dump it to the outfile
    Then we load in the proteins.  If we don't have the protein in our concord we immediately dump it to the outfile
    If we do have the protein, then we merge it with the gene version and dump that to the file.
    So at most, we only have the genes that map to proteins in memory at the same time.
    """
    uniprot2ncbi={}
    with open(geneprotein_concord, 'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            uniprot2ncbi[x[0]] = x[2]
    mappable_gene_ids = set(uniprot2ncbi.values())
    mappable_genes={}
    with jsonlines.open(outfile,'w') as outf:
        with jsonlines.open(gene_compendium,'r') as infile:
            for gene in infile:
                best_id = gene['id']['identifier']
                if best_id not in mappable_gene_ids:
                    outf.write(gene)
                else:
                    mappable_genes[best_id] = gene
        with jsonlines.open(protein_compendium,'r') as infile:
            for protein in infile:
                best_id = protein['id']['identifier']
                if best_id not in uniprot2ncbi:
                    outf.write(protein)
                else:
                    #Found a match!
                    gene = mappable_genes[uniprot2ncbi[best_id]]
                    newnode = merge(gene,protein)
                    outfile.write(newnode)

from src.prefixes import UNIPROTKB, NCBIGENE
from collections import defaultdict

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

def merge(geneproteinlist):
    """We have a gene and one or more proteins.  We want to create a combined something."""
    geneprotein = {}
    #Use the gene's ID.
    #The gene should be first in the list by construction
    gene = geneproteinlist[0]
    geneprotein['id'] = gene['id']
    for protein in geneproteinlist[1:]:
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

    There is one complication- the gene/protein links are not 1:1.  There are multiple UniProts associated with
    the same gene.  So we need to read until we have all of the proteins for a gene before we merge/write.
    """
    uniprot2ncbi={}
    ncbi2uniprot = defaultdict(list)
    with open(geneprotein_concord, 'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            uniprot2ncbi[x[0]] = x[2]
            ncbi2uniprot[x[2]].append(x[0])
    mappable_gene_ids = set(uniprot2ncbi.values())
    mappable_genes = defaultdict(list)
    with jsonlines.open(outfile,'w') as outf:
        with jsonlines.open(gene_compendium,'r') as infile:
            for gene in infile:
                best_id = gene['id']['identifier']
                if best_id not in mappable_gene_ids:
                    outf.write(gene)
                else:
                    mappable_genes[best_id].append( gene )
        with jsonlines.open(protein_compendium,'r') as infile:
            for protein in infile:
                uniprot_id = protein['id']['identifier']
                if uniprot_id not in uniprot2ncbi:
                    outf.write(protein)
                else:
                    #Found a match!
                    try:
                        ncbi_id = uniprot2ncbi[uniprot_id]
                        mappable_genes[ncbi_id].append(protein)
                        if len(mappable_genes[ncbi_id]) == len(ncbi2uniprot[ncbi_id]) + 1:
                            newnode = merge(mappable_genes[ncbi_id])
                            outfile.write(newnode)
                    except:
                        #What can happen is that there is an NCBI that gets discontinued, but that information hasn't
                        # made its way into the gene/protein concord. So we might try to look up a gene record
                        # that no longer exists
                        pass

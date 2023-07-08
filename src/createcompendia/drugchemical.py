from src.prefixes import RXCUI
from src.babel_utils import glom
from collections import defaultdict
import os

import jsonlines

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

# RXNORM has lots of relationships.
# RXNREL contains both directions of each relationship, just to make the file bigger
# Here's the list:
#   54 reformulated_to
#   54 reformulation_of
#  132 entry_version_of
#  132 has_entry_version
#  255 has_sort_version
#  255 sort_version_of
# 1551 has_product_monograph_title
# 1551 product_monograph_title_of
# 1667 mapped_to
# 1668 mapped_from
# 1932 has_modification
# 1932 is_modification_of
# 2886 has_permuted_term
# 2886 permuted_term_of
# 3367 form_of
# 3367 has_form
# 5372 has_member
# 5372 member_of
# 5894 contained_in
# 5894 contains
# 5937 has_quantified_form
# 5937 quantified_form_of
# 6215 included_in
# 6215 includes
# 9112 basis_of_strength_substance_of
# 9112 has_basis_of_strength_substance
# 9112 has_precise_active_ingredient
# 9112 precise_active_ingredient_of
# 10389 has_part
# 10389 part_of
# 11323 has_precise_ingredient
# 11323 precise_ingredient_of
# 11562 has_ingredients
# 11562 ingredients_of
# 29427 has_print_name
# 29427 print_name_of
# 35466 doseformgroup_of
# 35466 has_doseformgroup
# 101449 has_tradename
# 101449 tradename_of
# 111137 consists_of
# 111137 constitutes
# 128330 dose_form_of
# 128330 has_dose_form
# 251454 inverse_isa
# 251454 isa
# 335789 has_ingredient
# 335789 ingredient_of
# 352829 active_moiety_of
# 352829 has_active_moiety
# 374599 active_ingredient_of
# 374599 has_active_ingredient
# 561937
# 1640618 has_inactive_ingredient
# 1640618 inactive_ingredient_of

# Note that there are a bunch that are blank
# There's a reasonable picture explaining a lot of these here:
# https://www.nlm.nih.gov/research/umls/rxnorm/RxNorm_Drug_Relationships.png

# We're going to choose one of the two directions for each relationship from that picture.
# We're going to ignore the others because they seem freaky - for instance reformulation seems to have
# a bunch (all?) where the subject is not in RXNCONSO anywhere...

useful_relationships = [
"has_form",
"has_precise_active_ingredient",
"has_precise_ingredient",
"has_tradename",
"consists_of",
"has_dose_form",
"isa",
"ingredient_of",
"has_active_ingredient"]

def get_aui_to_cui():
    """Get a mapping from AUI to CUI"""
    aui_to_cui = {}
    sdui_to_cui = defaultdict(set)
    consofile = os.path.join('input_data', 'private', "RXNCONSO.RRF")
    with open(consofile, 'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            aui = x[7]
            cui = x[0]
            sdui = (x[11],x[7])
            if aui in aui_to_cui:
                print("What the all time fuck?")
                print(aui,cui)
                print(aui_to_cui[aui])
                exit()
            aui_to_cui[aui] = cui
            if sdui[1]=="":
                continue
            sdui_to_cui[sdui].add(cui)
    return aui_to_cui, sdui_to_cui

def get_cui(x,indicator_column,cui_column,aui_column,aui_to_cui,sdui_to_cui):
    relation_column = 7
    source_column = 10
    if x[relation_column] in useful_relationships:
        if x[indicator_column] == "CUI":
            return x[cui_column]
        elif x[indicator_column] == "AUI":
            return aui_to_cui[x[aui_column]]
        elif x[indicator_column] == "SDUI":
            cuis = sdui_to_cui[(x[source_column],x[aui_column])]
            if len(cuis) == 1:
                return list(cuis)[0]
            print("sdui garbage hell")
            print(x)
            print(cuis)
            exit()
        print("cmon man")
        print(x)
        exit()

def build_rxnorm_relationships(outfile):
    """RXNREL is a lousy file.
    The subject and object can sometimes be a CUI and sometimes an AUI and you have to use
    CONSO to figure out how to go back and forth.
    Some of them are using SDUIs are you joking?"""
    aui_to_cui, sdui_to_cui = get_aui_to_cui()
    relfile = os.path.join('input_data', 'private', "RXNREL.RRF")
    with open(relfile, 'r') as inf, open(outfile, 'w') as outf:
        for line in inf:
            x = line.strip().split('|')
            object = get_cui(x,2,0,1,aui_to_cui,sdui_to_cui)
            subject = get_cui(x,6,4,5,aui_to_cui,sdui_to_cui)
            if subject is not None:
                if subject == object:
                    continue
                outf.write(f"{RXCUI}:{subject}\t{x[7]}\t{RXCUI}:{object}\n")

def merge(geneproteinlist):
    """We have a gene and one or more proteins.  We want to create a combined something."""
    geneprotein = {}
    #Use the gene's ID.
    #The gene should be first in the list by construction
    gene = geneproteinlist[0]
    geneprotein['id'] = gene['id']
    geneprotein['equivalent_identifiers'] = gene['equivalent_identifiers']
    for protein in geneproteinlist[1:]:
        #there shouldn't be any overlap here, so we can just concatenate
        geneprotein['equivalent_identifiers'] += protein['equivalent_identifiers']
    #Now, we need to slightly modify the types. Not sure this is good, but maybe it is?
    geneprotein['type'] = ['biolink:Gene'] + protein['type']
    return geneprotein

kl={'NCBIGene':0, 'UniProtKB':1}
def gpkey(curie):
    """There are only NCBIGene and UniProtKB.  I want all the NCBI first and UniProt second, and after that, lexically sorted"""
    pref = curie.split(':')[0]
    return (kl[pref], curie)

def collect_valid_ids(compendium_file, idset):
    with jsonlines.open(compendium_file,'r') as inf:
        for line in inf:
            ids = [x['i'] for x in line['identifiers']]
            idset.update(ids)

def build_conflation(geneprotein_concord, gene_compendium, protein_compendium, outfile):
    """
    Fortunately our concord is in terms of the two preferred ids.
    All we should have to do is load that in, glom it up, and write out the groups
    But, there are some things in the concord that don't exist in at least the gene (maybe in the protein as well)
    """
    all_ids = set()
    collect_valid_ids(gene_compendium,all_ids)
    collect_valid_ids(protein_compendium,all_ids)
    conf = {}
    pairs= []
    with open(geneprotein_concord, 'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            if (x[0] in all_ids) and (x[2] in all_ids):
                pairs.append( (x[0], x[2]) )
    glom(conf,pairs)
    conf_sets = set([frozenset(x) for x in conf.values()])
    with jsonlines.open(outfile,'w') as outf:
        for cs in conf_sets:
            lc = list(cs)
            lc.sort(key=gpkey)
            outf.write(lc)

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
                            outf.write(newnode)
                            #Remove once we've written so we can make sure to clean up at the end
                            del mappable_genes[ncbi_id]
                    except:
                        #What can happen is that there is an NCBI that gets discontinued, but that information hasn't
                        # made its way into the gene/protein concord. So we might try to look up a gene record
                        # that no longer exists
                        outf.write(protein)
            #It can happen that there is a protein-gene link where the gene doesn't exist in our data.
            #Then we end up with proteins left over in mappable_genes that need to be written out.
            for missing_gene in mappable_genes:
                for protein in mappable_genes[missing_gene]:
                    outf.write(protein)

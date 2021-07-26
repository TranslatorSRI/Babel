from src.prefixes import NCBITAXON,MESH
from src.categories import ORGANISM_TAXON

import src.datahandlers.mesh as mesh

from src.babel_utils import read_identifier_file,glom,write_compendium
import src.eutil as eutil

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def write_mesh_ids(outfile):
    #Get the B tree,
    # B01	Eukaryota
    # B02	Archaea
    # B03	Bacteria
    # B04	Viruses
    # B05	Organism Forms
    meshmap = { f'B{str(i).zfill(2)}': ORGANISM_TAXON for i in range(1, 6)}
    #Also add anything from SCR_Chemical, if it doesn't have a tree map
    mesh.write_ids(meshmap,outfile,order=[ORGANISM_TAXON],extra_vocab={'SCR_Organism':ORGANISM_TAXON})

def build_relationships(outfile,mesh_ids):
    regis = mesh.pull_mesh_registry()
    with open(mesh_ids,'r') as inf:
        lines = inf.read().strip().split('\n')
        all_mesh_taxa = set([x.split('\t')[0] for x in lines])
    with open(outfile,'w') as outf:
        for meshid,reg in regis:
            #The mesh->ncbi are in mesh as registration numbers that start with a "tx"
            if reg.startswith('txid'):
                ncbi_id=f'{NCBITAXON}:{reg[4:]}'
                outf.write(f'{meshid}\txref\t{ncbi_id}\n')
        #June 7, 2021.  We have previously found that not all mesh/ncbi links are in the mesh.nt
        # but as of today, it appears that they ARE all in there, so we are not hitting eutil any more (thank goodness)
        #left = list(all_mesh_taxa.difference( set([x[0] for x in regis]) ))
        #eutil.lookup(left)



def build_compendia(concordances, identifiers):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = {}
    types = {}
    uniques = [NCBITAXON,MESH]
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
    baretype = ORGANISM_TAXON.split(':')[-1]
    write_compendium(gene_sets, f'{baretype}.txt', ORGANISM_TAXON, {})


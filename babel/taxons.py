import tarfile
import logging

from src.LabeledID import LabeledID
from src.util import LoggingUtil
from babel.babel_utils import pull_via_ftp,write_compendium,glom
from babel.taxon_mesh import go_mesh

logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def pull_ncbi_taxa():
    fname = pull_via_ftp('ftp.ncbi.nih.gov','/pub/taxonomy','taxdump.tar.gz',decompress_data=True,outfilename='taxdump.tar')
    taxtar = tarfile.open(fname,'r')
    f = taxtar.extractfile('names.dmp')
    l = f.readlines()
    results = {}
    for line in l:
        sline = line.decode('utf-8').strip().split('|')
        parts = [x.strip() for x in sline]
        if 'scientific name' == parts[3]:
            results[f'NCBITaxon:{parts[0]}'] = parts[1]
    return results

def load_taxa():
    """
    Pull information about genes, create a compendium, and save it out.
    Currently, we use only HGNC mappings.  This has the main problem that it limits us to human genes.
    Next step: Instead of HGNC as the mapping of record, move to either uniprot or NCBI.
    Include names from sources as well...
    """
    ncbi_taxa_labels = pull_ncbi_taxa()
    meshes, mesh_labels = go_mesh()
    labels = {}
    labels.update(ncbi_taxa_labels)
    labels.update(mesh_labels)
    taxa = {x:[x] for x in ncbi_taxa_labels}
    glom(taxa,meshes)
    synset = set([frozenset(x) for x in taxa.values()])
    write_compendium(synset,'taxon_compendium.txt','biolink:OrganismTaxon',labels=labels)

if __name__ == '__main__':
    load_taxa()


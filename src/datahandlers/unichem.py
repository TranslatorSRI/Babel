import gzip
import os

from src.babel_utils import pull_via_urllib
from src.prefixes import CHEMBLCOMPOUND,DRUGCENTRAL,DRUGBANK,GTOPDB,KEGGCOMPOUND,CHEBI,UNII,HMDB,PUBCHEMCOMPOUND

#global for this file
data_sources: dict = {'1': CHEMBLCOMPOUND, '2': DRUGBANK, '4': GTOPDB, '6': KEGGCOMPOUND, '7': CHEBI, '14': UNII,
                      '18': HMDB, '22': PUBCHEMCOMPOUND, '34': DRUGCENTRAL}


def pull_unichem():
    """ Download UniChem files. """
    pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/table_dumps/', 'structure.tsv.gz', decompress=False, subpath='UNICHEM')
    pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/table_dumps/', 'reference.tsv.gz', decompress=False, subpath='UNICHEM')

def filter_unichem(ref_file, ref_filtered):
    """ Filter UniChem reference file to those sources we're interested in. """
    srclist = [str(k) for k in data_sources.keys()]
    with gzip.open(ref_file, "rt") as rf, open(ref_filtered, "wt") as ref_filtered:
        header_line = rf.readline()
        assert(header_line == "UCI\tSRC_ID\tSRC_COMPOUND_ID\tASSIGNMENT\n", f"Incorrect header line in {ref_file}: {header_line}")
        ref_filtered.write(header_line)
        for line in rf:
            x = line.rstrip().split('\t')
            if x[1] in srclist and x[3] == '1':
                # Only use rows with assignment == 1 (current), not 0 (obsolete)
                # As per https://chembl.gitbook.io/unichem/definitions/what-is-an-assignment
                ref_filtered.writelines([line])

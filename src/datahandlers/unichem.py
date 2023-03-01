import gzip
import os

from src.babel_utils import pull_via_urllib
from src.prefixes import CHEMBLCOMPOUND,DRUGCENTRAL,DRUGBANK,GTOPDB,KEGGCOMPOUND,CHEBI,UNII,HMDB,PUBCHEMCOMPOUND

#global for this file
data_sources: dict = {'1': CHEMBLCOMPOUND, '2': DRUGBANK, '4': GTOPDB, '6': KEGGCOMPOUND, '7': CHEBI, '14': UNII,
                      '18': HMDB, '22': PUBCHEMCOMPOUND, '34': DRUGCENTRAL}


def pull_unichem():
    pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/table_dumps/', 'structure.tsv.gz', decompress=False, subpath='UNICHEM')
    ref_file = pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/table_dumps/', 'reference.tsv.gz', decompress=False, subpath='UNICHEM')
    ref_path, filename = os.path.split(ref_file)
    ref_filtered = os.path.join(ref_path, 'reference.filtered.tsv.gz')

    srclist = [str(k) for k in data_sources.keys()]
    with gzip.open(ref_file, "bt") as rf, open(ref_filtered, "wt") as ref_filtered:
        header_line = rf.readline()
        assert(header_line == "UCI	SRC_ID	SRC_COMPOUND_ID	ASSIGNMENT")
        ref_filtered.writelines([header_line])
        for line in rf:
            x = line.split('\t')
            if x[1] in srclist and x[3] == '1':
                # Only use rows with assignment == 1 (current), not 0 (obsolete)
                # As per https://chembl.gitbook.io/unichem/definitions/what-is-an-assignment
                ref_filtered.writelines([line])

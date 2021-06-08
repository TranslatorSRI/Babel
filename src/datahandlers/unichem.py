import ftplib
import gzip

from src.babel_utils import pull_via_urllib
from src.prefixes import CHEMBLCOMPOUND,DRUGCENTRAL,DRUGBANK,GTOPDB,KEGGCOMPOUND,CHEBI,UNII,HMDB,PUBCHEMCOMPOUND

#global for this file
data_sources: dict = {'1': CHEMBLCOMPOUND, '2': DRUGBANK, '4': GTOPDB, '6': KEGGCOMPOUND, '7': CHEBI, '14': UNII,
                      '18': HMDB, '22': PUBCHEMCOMPOUND, '34': DRUGCENTRAL}


def pull_unichem():

    # declare the unichem ids for the target data

    target_uc_url: str = get_latest_unichem_url()
    xref_file = pull_via_urllib(target_uc_url, 'UC_XREF.txt.gz', decompress=False,subpath='UNICHEM')
    struct_file = pull_via_urllib(target_uc_url, 'UC_STRUCTURE.txt.gz',subpath='UNICHEM' )

def filter_xrefs_by_srcid(xref_file,outfile):
    srclist = [str(k) for k in data_sources.keys()]
    with gzip.open(xref_file, 'rt') as inf, open(outfile, 'w') as outf:
        for line in inf:
            x = line.split('\t')
            if x[1] in srclist and x[3] == '1':
                outf.write(line)

def get_latest_unichem_url() -> str:
    # get a handle to the ftp directory
    ftp = ftplib.FTP("ftp.ebi.ac.uk")

    # login
    ftp.login()

    # move to the target directory
    ftp.cwd('/pub/databases/chembl/UniChem/data/oracleDumps')

    # get the directory listing
    files: list = ftp.nlst()

    # close the ftp connection
    ftp.quit()

    # init the starting point
    target_dir_index = 0

    # parse the list to determine the latest version of the files
    for f in files:
        # is this file greater that the previous
        if "UDRI" in f:
            # convert the suffix into an int and compare it to the previous one
            if int(f[4:]) > target_dir_index:
                # save this as our new highest value
                target_dir_index = int(f[4:])

    # return the full url
    return f'ftp://ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI{target_dir_index}/'



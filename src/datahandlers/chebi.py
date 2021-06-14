from src.prefixes import CHEBI,INCHIKEY
from src.babel_utils import pull_via_ftp

def pull_chebi():
    pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/SDF/', 'ChEBI_complete.sdf.gz', decompress_data=True, outfilename='CHEBI/ChEBI_complete.sdf')
    pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/Flat_file_tab_delimited/', 'database_accession.tsv', outfilename='CHEBI/database_accession.tsv')

def x(inputfile,labelfile,synfile):
    pass

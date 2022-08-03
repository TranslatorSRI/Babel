from src.babel_utils import pull_via_urllib, make_local_name

def pull_complexportal():
    pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/intact/complex/current/complextab/',f'1235996.tsv', decompress=False)

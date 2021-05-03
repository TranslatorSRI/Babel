from src.babel_utils import pull_via_urllib

def pull_ncit():
    #Currently, just pull a mapping we need.
    pull_via_urllib('https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Mappings/', f'NCIt-SwissProt_Mapping.txt', subpath='NCIT', decompress=False)

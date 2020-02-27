import logging

from src.LabeledID import LabeledID
from src.util import LoggingUtil
from babel.babel_utils import pull_via_ftp,write_compendium

#logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def pull_hgnc_families():
    """Get the HGNC json file & convert to python"""
    data = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/csv/genefamily_db_tables', 'family.csv')
    lines = data.split('\n')
    #skip header
    hgnc_families=[]
    for line in lines[1:]:
        parts = line.split(',')
        if len(parts) < 10:
            continue
        i = f"HGNC.FAMILY:{parts[0][1:-1]}"
        l = parts[2][1:-1]
        hgnc_families.append( LabeledID(identifier=i, label=l))
    return hgnc_families

def pull_panther_families():
    data = pull_via_ftp('ftp.pantherdb.org','/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/','PTHR14.1_human_')
    lines = data.split('\n')
    panther_families=[]
    done = set()
    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) < 5:
            print(len(parts))
            continue
        sf = parts[2]
        mf = sf.split(':')[0]
        mfname = parts[3]
        sfname = parts[4]
        if mf not in done:
            main_family = LabeledID(identifier=f'PANTHER.FAMILY:{mf}',label=mfname)
            panther_families.append(main_family)
            done.add(mf)
        if sf not in done:
            sub_family = LabeledID(identifier=f'PANTHER.FAMILY:{sf}', label=sfname)
            panther_families.append(sub_family)
            done.add(sf)
    for f in panther_families[:10]:
        print(f)
    return panther_families

def load_gene_families():
    """
    Pull information about gene families.
    There are 2 sources, hgnc and panther.  There's no crossing them, so we're just going to turn each one into its
    own entity
    """
    hgnc = pull_hgnc_families()
    panther = pull_panther_families()
    #Write compendium wants a list of iterables
    synonyms = [ (x,) for x in hgnc+panther ]
    write_compendium(synonyms,'gene_family_compendium.txt','gene_family')

#def synonymize_genes():
#    """
#    """
#    ids_to_synonyms = {}
#    hgnc = pull_hgnc_json()
#    hgnc_genes = hgnc['response']['docs']
#    logger.debug(f' Found {len(hgnc_genes)} genes in HGNC')
#    hgnc_identifiers = [ json_2_identifiers(gene) for gene in hgnc_genes ]
#    return hgnc_identifiers

if __name__ == '__main__':
    load_gene_families()

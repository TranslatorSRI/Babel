from src.prefixes import GTOPDB
from src.babel_utils import pull_via_urllib

from bs4 import BeautifulSoup

def pull_gtopdb_ligands():
    pull_via_urllib('https://www.guidetopharmacology.org/DATA/','ligands.tsv',decompress=False,subpath='GTOPDB')


def strip_html_tags(name):
    """
    GtoPDB contains HTML tags, such as <sup>, </sup>, &Delta;, &beta; and others.

    (e.g. GTOPDB:4215 has a label of "Hg<sup>2+</sup>", see
    https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=4215)

    This function will strip all those HTML tags.

    :param name: The input label or synonym.
    :return: The same label or synonym, but with the HTML tags stripped.
    """
    return BeautifulSoup(name, 'html.parser').get_text()


def make_labels_and_synonyms(inputfile,labelfile,synfile):
    idcol = 0
    labelcol = 1
    syncol = 13
    with open(inputfile,'r') as inf, open(labelfile,'w') as lf, open(synfile,'w') as sf:
        h = inf.readline()
        #Everything in this file is double quoted, hence all the [1:-1] stuff
        for line in inf:
            parts = line.strip().split('\t')
            ident = f'{GTOPDB}:{parts[idcol][1:-1]}'
            label = parts[labelcol][1:-1]
            synstring = parts[syncol][1:-1]
            if len(label) > 0:
                lf.write(f'{ident}\t{strip_html_tags(label)}\n')
            if len(synstring) > 0:
                syns = synstring.split('|')
                for syn in syns:
                    sf.write(f'{ident}\toio:exact\t{strip_html_tags(syn)}\n')

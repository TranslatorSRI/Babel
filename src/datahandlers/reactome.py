from src.prefixes import OIO, ORPHANET
from src.babel_utils import pull_via_urllib
import json
from zipfile import ZipFile
#ugh XML
import xml.etree.ElementTree as ET

def pull_reactome(labelfile):
    outputfile=pull_via_urllib('https://reactome.org/download/current/','ReactomePathways.txt', subpath='REACT', decompress=False)
    make_labels(outputfile,labelfile)

def make_labels(infile,labelfile):
    with open(infile,'r') as inf, open(labelfile,'w') as labels:
        for line in inf:
            x = line.strip().split('\t')
            labels.write(f'REACT:{x[0]}\t{x[1]} ({x[2]})')

import shutil
from zipfile import ZipFile

import requests

from src.babel_utils import make_local_name, pull_via_ftp, pull_via_urllib
from src.prefixes import UMLS
from collections import defaultdict
import os
import re
import logging

def check_mrconso_line(line):
    """
    This function can be used to filter lines from MRCONSO.RRF with a
    standard set of criteria that we apply across all of Babel:
    1. We only use English terms (this skips terms that don't have any English labels).
    2. We skip obsolete content on the basis of the SUPPRESS flag.

    :param line: A line from MRCONSO.RRF.
    :return: True if the line passes checks and should be tested, false if the line fails checks and should be skipped.
    """

    x = line.strip().split('|')
    lang = x[1]

    # Note that this skips terms that don't have any English labels.
    #Only keep english terms
    if lang != 'ENG':
        return False

    # Use the SUPPRESS flag (https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/)
    # - O:  All obsolete content, whether they are obsolesced by the source or by NLM. These will include all atoms
    #       having obsolete TTYs, and other atoms becoming obsolete that have not acquired an obsolete TTY (e.g. RxNorm
    #       SCDs no longer associated with current drugs, LNC atoms derived from obsolete LNC concepts).
    # - E:  Non-obsolete content marked suppressible by an editor. These do not have a suppressible SAB/TTY combination.
    #only keep unsuppressed rows
    suppress = x[16]
    if suppress == 'O' or suppress == 'E':
        return False

    return True

def write_umls_ids(category_map,umls_output,blacklist=set()):
    categories = set(category_map.keys())
    mrsty = os.path.join('input_data', 'private', 'MRSTY.RRF')
    umls_keepers = set()
    with open(mrsty,'r') as inf, open(umls_output,'w') as outf:
        for line in inf:
            x = line.strip().split('|')
            cat = x[2]
            if cat in categories:
                if not x[0] in blacklist:
                    outf.write(f'{UMLS}:{x[0]}\t{category_map[cat]}\n')


#I've made this more complicated than it ought to be for 2 reasons:
# One is to keep from having to pass through the umls file more than once, but that's a bad reason
# The second is because I want to use the UMLS as a source for some terminologies (SNOMED) even if there's another
#  way.  I'm going to modify this to do one thing at a time, and if it takes a little longer, then so be it.
def build_sets(umls_input, umls_output , other_prefixes, bad_mappings=defaultdict(set), acceptable_identifiers={}):
    """Given a list of umls identifiers we want to generate all the concordances
    between UMLS and that other entity"""
    # On UMLS / MESH: we have been getting all UMLS / MESH relationships.   This has led to some clear mistakes
    # and logical impossibilities such as cyclical subclasses.   On further review, we can sharpen these relationships
    # by choosing the best match UMLS for each MESH.  We will make use of the TTY column (column 12) in MRCONSO.
    # This column can have a lot of values, but every MESH has one of (and only one of): MH, NM, HT, QAB.  These
    # will be the ones that we pull, as they correspond to the "main" name or heading of the mesh entry.
    umls_ids = set()
    with open(umls_input) as inf:
        for line in inf:
            u = line.strip().split('\t')[0].split(':')[1]
            umls_ids.add(u)
    lookfor = set(other_prefixes.keys())
    acceptable_mesh_tty = set(["MH","NM","HT","QAB"])
    mrconso = os.path.join('input_data', 'private', 'MRCONSO.RRF')
    pairs = set()
    #test_cui = 'C0026827'
    with open(mrconso,'r') as inf, open(umls_output,'w') as concordfile:
        for line in inf:
            if not check_mrconso_line(line):
                continue

            x = line.strip().split('|')
            cui = x[0]
            if cui not in umls_ids:
                continue

            #only keep sources we're looking for
            source = x[11]
            if source not in lookfor:
                continue
            tty = x[12]
            if (source == 'MSH') and (tty not in acceptable_mesh_tty):
                continue
            #For some dippy reason, in the id column they say "HGNC:76"
            pref = other_prefixes[source]
            if ':' in x[13]:
                other_id = f'{pref}:{x[13].split(":")[-1]}'
            else:
                other_id = f'{pref}:{x[13]}'
            #I don't know why this is in here, but it is not an identifier equivalent to anything
            if other_id == 'NCIT:TCGA':
                continue
            tup = (f'{UMLS}:{cui}',other_id)
            #Don't include bad mappings or bad ids
            if tup[1] in bad_mappings[tup[0]]:
                continue
            if (pref in acceptable_identifiers) and (not tup[1] in acceptable_identifiers[pref]):
                continue
            if tup not in pairs:
                concordfile.write(f'{tup[0]}\teq\t{tup[1]}\n')
                pairs.add(tup)

def read_umls_priority():
    mrp = os.path.join('input_data', 'umls_precedence.txt')
    pris = []
    with open(mrp,'r') as inf:
        h =inf.readline()
        for line in  inf:
            x = line.strip().split()
            if x[2] == 'No':
                pris.append( (x[0],x[1],'N'))
            elif x[2] == 'Yes':
                pris.append( (x[0],x[1],'Y'))
            else:
                pass
    prid = { x:i for i,x in enumerate(pris) }
    return prid


def download_umls(umls_version, download_dir):
    """
    Download the latest UMLS into the specified download directory. In addition to downloading
    and unzipping UMLS, this will move the files we use into the main directory.

    :param umls_version: The version of UMLS to download (e.g. `2023AA`).
    :param download_dir: The directory to download UMLS to (e.g. `babel_downloads/UMLS`)
    """
    umls_api_key = os.environ.get('UMLS_API_KEY')
    if not umls_api_key:
        print("The environmental variable UMLS_API_KEY needs to be set to a valid UMLS API key.")
        print("See instructions at https://documentation.uts.nlm.nih.gov/rest/authentication.html")
        exit(1)

    # Download umls-{umls_version}-metathesaurus-full.zip
    # As described at https://documentation.uts.nlm.nih.gov/automating-downloads.html
    umls_url = f"https://uts-ws.nlm.nih.gov/download"
    req = requests.get(umls_url, {
        "url": f"https://download.nlm.nih.gov/umls/kss/{umls_version}/umls-{umls_version}-metathesaurus-full.zip",
        "apiKey": umls_api_key
    }, stream=True)
    if not req.ok:
        print(f"Unable to download UMLS from ${umls_url}: ${req}")
        exit(1)

    # Write file to {download_dir}/umls-{umls_version}-metathesaurus-full.zip
    logging.info(f"Downloading umls-{umls_version}-metathesaurus-full.zip to {download_dir}")
    os.makedirs(download_dir, exist_ok=True)
    umls_download_zip = os.path.join(download_dir, f"umls-{umls_version}-metathesaurus-full.zip")
    with open(umls_download_zip, 'wb') as fd:
        for chunk in req.iter_content(chunk_size=128):
            fd.write(chunk)

    # Unzip file.
    logging.info(f"Uncompressing {umls_download_zip}")
    with ZipFile(umls_download_zip, 'r') as zipObj:
        zipObj.extractall(download_dir)

    # Move files we use to the main download directory.
    # - MRCONSO.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, 'MRCONSO.RRF'), download_dir)
    # - MRSTY.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, 'MRSTY.RRF'), download_dir)


def pull_umls(mrconso):
    """Run through MRCONSO.RRF creating label and synonym files for UMLS and SNOMEDCT"""
    mrcon = os.path.join('input_data', 'private', 'MRCONSO.RRF')
    rows = defaultdict(list)
    priority = read_umls_priority()
    snomed_label_name = make_local_name('labels', subpath='SNOMEDCT')
    snomed_syn_name = make_local_name('synonyms', subpath='SNOMEDCT')
    with open(mrcon, 'r') as inf, open(snomed_label_name,'w') as snolabels, open(snomed_syn_name,'w') as snosyns:
        for line in inf:
            if not check_mrconso_line(line):
                continue

            x = line.strip().split('|')
            cui = x[0]
            lang = x[1]
            suppress = x[16]
            source = x[11]
            termtype = x[12]
            term = x[14]
            #While we're here, if this thing is snomed, lets get it
            if source == 'SNOMEDCT_US':
                snomed_id = f'SNOMEDCT:{x[15]}'
                if termtype == 'PT':
                    snolabels.write(f'{snomed_id}\t{term}\n')
                snosyns.write(f'{snomed_id}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{term}\n')
            #UMLS is a collection of sources. They pick one of the names from these sources for a concept,
            # and that's based on a priority that they define. Here we get the priority for terms so we
            # can get the right one for the label
            pkey = (source,termtype,suppress)
            try:
                pri= priority[pkey]
            except:
                #print(pkey)
                pri = 1000000
            rows[cui].append( (pri,term,line) )
    lname = make_local_name('labels', subpath='UMLS')
    sname = make_local_name('synonyms', subpath='UMLS')
    re_numerical = re.compile(r"^\s*[+-]*[\d\.]+\s*$")
    with open(lname,'w') as labels, open(sname,'w') as synonyms:
        for cui,crows in rows.items():
            crows.sort()
            labels.write(f'{UMLS}:{cui}\t{crows[0][1]}\n')
            syns = set( [crow[1] for crow in crows])
            for s in syns:
                # Skip any synonyms that are purely numerical, since those are unlikely to be useful.
                if re_numerical.fullmatch(s):
                    logging.debug(f"Found numerical synonym '{s}' in UMLS, skipping")
                    continue
                synonyms.write(f'{UMLS}:{cui}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{s}\n')


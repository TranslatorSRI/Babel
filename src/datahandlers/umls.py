from src.prefixes import UMLS, RXCUI
from src.babel_utils import make_local_name
from src.categories import DRUG, CHEMICAL_ENTITY, MOLECULAR_MIXTURE

import shutil
from zipfile import ZipFile
import requests
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

def write_umls_ids(mrsty, category_map, umls_output, prefix=UMLS, blocklist_umls_ids=None, blocklist_umls_semantic_type_tree=None):
    """
    Write out UMLS IDs and categories (as per a category map) to a file.

    :param mrsty: The file path of the MRSTY.RRF file from the UMLS download.
    :param category_map: A dictionary mapping UMLS semantic types to Biolink types.
    :param umls_output: The file path of the output file.
    :param prefix: The prefix to use for the UMLS IDs. Defaults to UMLS.
    :param blocklist_umls_ids: A set of individual UMLS IDs to block. Defaults to None.
    :param blocklist_umls_semantic_type_tree: A set of UMLS semantic type trees to block. Defaults to None.
        Note that we strictly filter out the semantic type trees listed here: if e.g. A1.2.3 is on the blocklist,
        UMLS IDs with a semantic type tree of A1.2.3.4 will be allowed -- only UMLS IDs with a type of A1.2.3 will
        be blocked.
    :return: None.
    """

    if blocklist_umls_ids is None:
        blocklist_umls_ids = set()
    if blocklist_umls_semantic_type_tree is None:
        blocklist_umls_semantic_type_tree = set()

    # Fun fact: MRSTY has duplicate records for entities that have multiple types, e.g.
    #   CUI | TUI | STN | STY | ATUI | CVF
    #   C0000005|T116|A1.4.1.2.1.7|Amino Acid, Peptide, or Protein|AT17648347|256|
    #   C0000005|T121|A1.4.1.1.1|Pharmacologic Substance|AT17575038|256|
    #   C0000005|T130|A1.4.1.1.4|Indicator, Reagent, or Diagnostic Aid|AT17634323|256|
    #   C0000039|T109|A1.4.1.2.1|Organic Chemical|AT45562015|256|
    # (see https://github.com/TranslatorSRI/Babel/issues/200#issuecomment-1789550364 for another example and
    #  https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.Tf/ for column information.)
    #
    # This means that we can't blacklist UMLS types by just skipping those lines: instead, we will need to load
    # the type information on the selected CUI, and later filter out the blacklisted UMLS type trees.

    output_lines = defaultdict(list)
    semantic_type_trees = defaultdict(set)
    tree_names = defaultdict(set)
    with open(mrsty,'r') as inf, open(umls_output,'w') as outf:
        for line in inf:
            x = line.strip().split('|')
            cat = x[2]
            cat_name = x[3]

            # Is this on the UMLS ID blocklist? If so, skip it!
            if x[0] in blocklist_umls_ids:
                continue

            curie = f"{prefix}:{x[0]}"

            tree_names[cat].add(cat_name)
            semantic_type_trees[curie].add(cat)

            # Do we know what Biolink type (i.e. category) to assign this to?
            if cat in category_map:
                output_lines[curie].append(category_map[cat])

        if blocklist_umls_semantic_type_tree:
            # If we need to blacklist by UMLS semantic type trees, delete CURIEs whose semantic type trees overlap
            # with the blocklist.
            selected_curies = list(output_lines.keys())
            for curie in selected_curies:
                if semantic_type_trees[curie] & blocklist_umls_semantic_type_tree:
                    # Note that this only works if the UMLS semantic tree type is exactly identical to the semantic
                    # tree type on the blocklist: so if you try to block "A1.2.3", then UMLS IDs with a semantic tree
                    # type of "A1.2.3.4" will NOT be blocked.

                    # Write out a log message.
                    sty_trees_with_names = ", ".join(map(lambda sty_tree: f"{sty_tree}={tree_names[sty_tree]}", semantic_type_trees[curie]))
                    blocklist_sty_trees_with_names = ", ".join(map(lambda sty_tree: f"{sty_tree}={tree_names[sty_tree]}", blocklist_umls_semantic_type_tree))
                    logging.info(f"Deleted {curie} from UMLS IDs because its types ({sty_trees_with_names}) overlapped with the blocklist ({blocklist_sty_trees_with_names}).")

                    # Delete this CURIE from the output.
                    del output_lines[curie]

        for curie in output_lines:
            # We only write out the first type we found for this UMLS ID.
            types = output_lines[curie]
            outf.write(f"{curie}\t{types[0]}\n")


def write_rxnorm_ids(category_map, bad_categories, infile, outfile,prefix=RXCUI,styfile="RXNSTY.RRF",blacklist=set()):
    """It's surprising, but not everything in here has an RXCUI.
    Just because there's a row and it has an id in the first column, it doesn't mean pretty much anything.
    It's only ones that have an RXNORM in their row somewhere that count.   They are the ones that show up
    in MRCONSO.RRF.  It's not yet clear if there are relations that go through them though. So first we gotta
    go through RXNCONSO to find the ones that have an RXNORM, then back through STY to get the categories.

    Here is a response from the RXNORM folks about this (slightly reformatted):
    ---
    You are correct that some RXCUIs do not appear in the UMLS MRCONSO file.
    If there isn't a SAB='RXNORM' row in RxNorm for a particular RXCUI, there won't be one in the UMLS MRCONSO file.
    But you can still link RxNorm to the UMLS Metathesaurus in this case.
    Join RXNCONSO and MRCONSO on the the SAB fields (12th column in both RXNCONSO and MRCONSO) and CODE fields (14th column in both).
    So for your example of RXCUI 3, you can link RXNCONSO and MRCONSO on the SNOMEDCT_US code.

    Or you can also look up the RXCUI in the UMLS MRSAT file.
    For example, if you want to look up RXCUI 3, find rows where ATN (9th column) = 'RXCUI' and ATV (11th column) = '3'.

    Note that the UMLS is only updated twice a year, so new content in RxNorm will not appear in the UMLS until it is updated in May and November.

    Many RXCUIs do not have a SAB='RXNORM' row. There are a few reasons why an RXCUI would not have an associated RxNorm normalized name.
    Out of scope – Some information provided by source vocabularies is out of scope for RxNorm. While this information is grouped into concepts and given RXCUIs, RxNorm normalized names are not created for this information.
    Ambiguous – Some information provided by source vocabularies are too vague and for a specific RxNorm normalized name to be assigned.
    Base atom – A Base atom often lacks an RxNorm normalized name because a Base atom contains NDCs representing several different drug products. For more information about Base atoms, see Section 9: Duplicating Source Asserted Atoms (with NDC conflicts) of the RxNorm Technical Documentation.
    See our FAQ for more information: https://www.nlm.nih.gov/research/umls/rxnorm/faq.html
    ---
    Based on this response, I think that there are probably good reasons to leave out the RXCUIs without an RXNORM row.
    From what I can tell, RxNav also leaves these out.  So I'll leave them out for the moment.  If we find that we need
    them later, we can add them back in following the instructions above.

    After gorking around with STY for a while, I've realized that the best way to get the types is from RXNCONSO.
    If there is an IN or PIN TTY, then it's a ChemicalEntity, otherwise a Drug.
    """
    rxnconso = infile # os.path.join('input_data', 'private', "RXNCONSO.RRF")
    with open(rxnconso,'r') as inf, open(outfile,'w') as outf:
        current_id = None
        current_ttys = set()
        has_rxnorm = False
        for line in inf:
            #THis will remove obsolete and non-english lines
            if not check_mrconso_line(line):
                continue
            x = line.strip().split('|')
            if x[0] in blacklist:
                continue
            if x[0] != current_id:
                if (current_id is not None) and has_rxnorm:
                    if "DF" in current_ttys:
                        # These are dose forms.  Things like "Bottle" and "Tablet". Leads to all sorts of overglomming.
                        # Following is a hard pass.
                        pass
                    elif "IN" in current_ttys or "PIN" in current_ttys:
                        outf.write(f"{prefix}:{current_id}\t{CHEMICAL_ENTITY}\n")
                    elif "MIN" in current_ttys:
                        outf.write(f"{prefix}:{current_id}\t{MOLECULAR_MIXTURE}\n")
                    else:
                        outf.write(f"{prefix}:{current_id}\t{DRUG}\n")
                current_id = x[0]
                current_ttys = set()
                has_rxnorm = False
            if x[11] == 'RXNORM':
                has_rxnorm = True
                current_ttys.add(x[12])
        if has_rxnorm:
            if "DF" in current_ttys:
                #These are dose forms.  Things like "Bottle" and "Tablet". Leads to all sorts of overglomming.
                # Following is a hard pass.
                pass
            elif "IN" in current_ttys or "PIN" in current_ttys:
                outf.write(f"{prefix}:{current_id}\t{CHEMICAL_ENTITY}\n")
            elif "MIN" in current_ttys:
                outf.write(f"{prefix}:{current_id}\t{MOLECULAR_MIXTURE}\n")
            else:
                outf.write(f"{prefix}:{current_id}\t{DRUG}\n")


#I've made this more complicated than it ought to be for 2 reasons:
# One is to keep from having to pass through the umls file more than once, but that's a bad reason
# The second is because I want to use the UMLS as a source for some terminologies (SNOMED) even if there's another
#  way.  I'm going to modify this to do one thing at a time, and if it takes a little longer, then so be it.
def build_sets(mrconso,umls_input, umls_output , other_prefixes, bad_mappings=defaultdict(set), acceptable_identifiers={},
               cui_prefix = UMLS):
    """Given a list of umls identifiers we want to generate all the concordances
    between UMLS and that other entity"""
    # On UMLS / MESH: we have been getting all UMLS / MESH relationships.   This has led to some clear mistakes
    # and logical impossibilities such as cyclical subclasses.   On further review, we can sharpen these relationships
    # by choosing the best match UMLS for each MESH.  We will make use of the TTY column (column 12) in MRCONSO.
    # This column can have a lot of values, but every MESH has one of (and only one of): MH, NM, HT, QAB.  These
    # will be the ones that we pull, as they correspond to the "main" name or heading of the mesh entry.
    # Because drugbank IDs are for active ingredients, we only want the UMLS IDs that map to a TTY of IN (ingredient)
    # Otherwise, you get the same DBID mapping to multiple UMLS IDs in a loose way.
    umls_ids = set()
    with open(umls_input) as inf:
        for line in inf:
            u = line.strip().split('\t')[0].split(':')[1]
            umls_ids.add(u)
    lookfor = set(other_prefixes.keys())
    acceptable_mesh_tty = set(["MH","NM","HT","QAB"])
    acceptable_drugbank_tty = set(["IN","PIN","MIN"])
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
            if (source == 'DRUGBANK') and (tty not in acceptable_drugbank_tty):
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
            tup = (f'{cui_prefix}:{cui}',other_id)
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
    shutil.copy2(os.path.join(download_dir, umls_version, 'META', 'MRCONSO.RRF'), download_dir)
    # - MRSTY.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, 'META', 'MRSTY.RRF'), download_dir)
    # - MRREL.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, 'META', 'MRREL.RRF'), download_dir)


def download_rxnorm(rxnorm_version, download_dir):
    """
    Download the specified RxNorm version into the specified download directory. In addition to
    downloading and unzipping RxNorm, this will move the files we use into the main directory.

    :param rxnorm_version: The version of RxNorm to download (e.g. `07032023`).
        Look for the latest download at https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html
    :param download_dir: The directory to download UMLS to (e.g. `babel_downloads/RxNORM`)
    """
    umls_api_key = os.environ.get('UMLS_API_KEY')
    if not umls_api_key:
        print("The environmental variable UMLS_API_KEY needs to be set to a valid UMLS API key.")
        print("See instructions at https://documentation.uts.nlm.nih.gov/rest/authentication.html")
        exit(1)

    # Download RxNorm_full_{rxnorm_version}.zip
    # As described at https://documentation.uts.nlm.nih.gov/automating-downloads.html
    rxnorm_url = f"https://uts-ws.nlm.nih.gov/download"
    req = requests.get(rxnorm_url, {
        "url": f"https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_{rxnorm_version}.zip",
        "apiKey": umls_api_key
    }, stream=True)
    if not req.ok:
        print(f"Unable to download RxNorm from ${rxnorm_url}: ${req}")
        exit(1)

    # Write file to {download_dir}/RxNorm_full_{rxnorm_version}.zip
    logging.info(f"Downloading RxNorm_full_{rxnorm_version}.zip to {download_dir}")
    os.makedirs(download_dir, exist_ok=True)
    rxnorm_download_zip = os.path.join(download_dir, f"RxNorm_full_{rxnorm_version}.zip")
    with open(rxnorm_download_zip, 'wb') as fd:
        for chunk in req.iter_content(chunk_size=128):
            fd.write(chunk)

    # Unzip file.
    logging.info(f"Uncompressing {rxnorm_download_zip}")
    with ZipFile(rxnorm_download_zip, 'r') as zipObj:
        zipObj.extractall(download_dir)

    # Move files we use to the main download directory.
    # - RXNCONSO.RRF
    shutil.copy2(os.path.join(download_dir, 'rrf', 'RXNCONSO.RRF'), download_dir)
    # - RXNREL.RRF
    shutil.copy2(os.path.join(download_dir, 'rrf', 'RXNREL.RRF'), download_dir)



def pull_umls(mrconso):
    """Run through MRCONSO.RRF creating label and synonym files for UMLS and SNOMEDCT"""
    rows = defaultdict(list)
    priority = read_umls_priority()
    snomed_label_name = make_local_name('labels', subpath='SNOMEDCT')
    snomed_syn_name = make_local_name('synonyms', subpath='SNOMEDCT')
    with open(mrconso, 'r') as inf, open(snomed_label_name,'w') as snolabels, open(snomed_syn_name,'w') as snosyns:
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


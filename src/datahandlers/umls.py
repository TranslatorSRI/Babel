from src.babel_utils import make_local_name, pull_via_ftp
from src.prefixes import UMLS, RXCUI
from src.categories import DRUG, CHEMICAL_ENTITY, MOLECULAR_MIXTURE
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

def write_umls_ids(category_map,umls_output,prefix=UMLS,styfile="MRSTY.RRF",blacklist=set()):
    categories = set(category_map.keys())
    mrsty = os.path.join('input_data', 'private', styfile)
    with open(mrsty,'r') as inf, open(umls_output,'w') as outf:
        for line in inf:
            x = line.strip().split('|')
            cat = x[2]
            if cat in categories:
                if not x[0] in blacklist:
                    outf.write(f'{prefix}:{x[0]}\t{category_map[cat]}\n')

def write_rxnorm_ids(category_map, bad_categories, outfile,prefix=RXCUI,styfile="RXNSTY.RRF",blacklist=set()):
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
    rxnconso = os.path.join('input_data', 'private', "RXNCONSO.RRF")
    with open(rxnconso,'r') as inf, open(outfile,'w') as outf:
        current_id = None
        current_ttys = set()
        has_rxnorm = False
        for line in inf:
            x = line.strip().split('|')
            if x[0] in blacklist:
                continue
            if x[0] != current_id:
                if (current_id is not None) and has_rxnorm:
                    if "IN" in current_ttys or "PIN" in current_ttys:
                        outf.write(f"{prefix}:{current_id}\t{CHEMICAL_ENTITY}\n")
                    else:
                        outf.write(f"{prefix}:{current_id}\t{DRUG}\n")
                current_id = x[0]
                current_ttys = set()
                has_rxnorm = False
            if x[11] == 'RXNORM':
                has_rxnorm = True
                current_ttys.add(x[12])
        if has_rxnorm:
            if "IN" in current_ttys or "PIN" in current_ttys:
                outf.write(f"{prefix}:{current_id}\t{CHEMICAL_ENTITY}\n")
            elif "MIN" in current_ttys:
                outf.write(f"{prefix}:{current_id}\t{MOLECULAR_MIXTURE}\n")
            else:
                outf.write(f"{prefix}:{current_id}\t{DRUG}\n")


#I've made this more complicated than it ought to be for 2 reasons:
# One is to keep from having to pass through the umls file more than once, but that's a bad reason
# The second is because I want to use the UMLS as a source for some terminologies (SNOMED) even if there's another
#  way.  I'm going to modify this to do one thing at a time, and if it takes a little longer, then so be it.
def build_sets(umls_input, umls_output , other_prefixes, bad_mappings=defaultdict(set), acceptable_identifiers={},
               conso = "MRCONSO.RRF", cui_prefix = UMLS):
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
    acceptable_drugbank_tty = set(["IN"])
    mrconso = os.path.join('input_data', 'private', conso)
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

def pull_umls():
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


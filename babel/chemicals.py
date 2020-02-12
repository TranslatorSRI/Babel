from functools import partial
import logging
import os
import pickle
import requests
import asyncio
import ftplib
import pandas
import gzip
from collections import defaultdict

from src.util import LoggingUtil, Text
from src.LabeledID import LabeledID

from babel.chemical_mesh_unii import refresh_mesh_pubchem
from babel.babel_utils import glom, pull_via_ftp, write_compendium,pull_via_urllib,make_local_name
from babel.chemistry_pulls import pull_chebi, chebi_sdf_entry_to_dict, pull_uniprot, pull_iuphar, pull_kegg_sequences
from babel.big_gz_sort import batch_sort


logger = LoggingUtil.init_logging("chemicals", logging.DEBUG, format='medium', logFilePath=f'{os.path.dirname(os.path.abspath(__file__))}/logs/')

def make_mesh_id(mesh_uri):
    return f"mesh:{mesh_uri.split('/')[-1][:-1]}"

def pull_mesh_chebi():
    url = 'https://query.wikidata.org/sparql?format=json&query=SELECT ?chebi ?mesh WHERE { ?compound wdt:P683 ?chebi . ?compound wdt:P486 ?mesh. }'
    results = requests.get(url).json()
    pairs = [(f'MESH:{r["mesh"]["value"]}', f'CHEBI:{r["chebi"]["value"]}')
             for r in results['results']['bindings']
             if not r['mesh']['value'].startswith('M')]
    #Wikidata is great, except when it sucks.   One thing it likes to do is to
    # have multiple CHEBIs for a concept, say ignoring stereochemistry or 
    # the like.  No good.   It's easy enough to filter these out, but then 
    # we wouldn't have the mesh associated with anything. A spot check makes it seem like
    # cases of this type usually also have a UNII.  So we can perhaps remove ugly pairs without
    # a problem.
    m2c = defaultdict(list)
    for m,c in pairs:
        m2c[m].append(c)
    fpairs = []
    for m,clist in m2c.items():
        if len(clist) == 1:
            fpairs.append( (m,clist[0]) )
    mcname = make_local_name('mesh_chebi.txt')
    with open(mcname, 'w') as outf:
        for m, c in fpairs:
            outf.write(f'{m}\t{c}\n')
    return fpairs

def pull_uniprot_chebi():
    url = 'https://query.wikidata.org/sparql?format=json&query=SELECT DISTINCT ?c ?s WHERE { ?compound wdt:P683 ?c. ?compound p:P352 ?statement . ?statement pq:P2888 ?s. }'
    results = requests.get(url).json()
    pairs = [ (f'UniProtKB:{r["s"]["value"].split("/")[-1]}',f'CHEBI:{r["c"]["value"]}')
             for r in results['results']['bindings'] ]
    #with open('uniprot_chebi.txt','w') as outf:
    #    for m,c in pairs:
    #        outf.write(f'{m}\t{c}\n')
    return pairs

def filter_mesh_chebi(mesh_chebi,concord):
    """MESH/CHEBI is a real mess though.  wikidata has no principled way to connect identifiers.  It's just whatever
     somebody said.  We really should use as a last resort.  If we don't know much about it, then sure.  But if
     we've already got a chebi or a unii, then we should ignore this wiki stuff."""
    fmc = []
    for m,c in mesh_chebi:
        if m not in concord:
            fmc.append( (m,c) )
        else:
            equivs = concord[m]
            prefs = [ Text.get_curie(e) for e in equivs ]
            if ('CHEBI') in prefs:
                continue
            if ('UNII') in prefs:
                continue
            if ('INCHIKEY') in prefs:
                continue
            fmc.append( (m,c) )
    return fmc

##
# Here's a pointless rant about chemical synonymization.
#
#                         IT SHOULD BE EASY
#
# Chemicals are defined structures!  Inchikeys! SMILES! It isn't rocket science!
# If it has the same strucrture, it's the same! If it doesn't, it isn't!
# 
# Here's the problem - some vocabularies use chemicals, but not based on
# structures.  These are usually bullshit vocabularies like MeSH (It's, like,
# just a concept, dude) that occasionally just assert that they're the same
# as something that does have a structure, like a UNII.  On the whole if 
# one of these ding-dong vocabularies gives us that information, we should
# use it because it's the only objective statement about the identity of the
# term that will ever exist.
#
# Anybody who asserts that things with different structures are the same should
# be brought up on charges at the Hague.  I'm looking at you, wikidata, and your
# willingness to identify e.g. hydrous and anhydrous CHEBIs in the same entry
#
# Also, it would be great if UNII could figure out how to assign inchis
# correctly.  Both Stibine (antimony with hydrogen atoms) and Antimony 
# (elemental) end up wth the same inchikey erroneously, which causes all
# sorts of downstream problems, because other stuff links to them separately
# and sort of correctly if you don't pay attention to the keys and just to the 
# name, which apparently is what people do even in our advanced civilization.
# And we can't ignore UNII completely because that's one of the links that MeSH uses.
#
##

###
#
# Chemical synonymization includes both small molecules and large molecules (peptides and proteins)
# In many cases these don't intersect, but in some they do, and we need to handle that
#
# Chemicals can be described 4 ways:
# 1. InchiKey - the most specific.  For chemicals with ik's, UniChem has a concordance.
# 2. SMILES - everything with an IK has a smiles, but not vice versa: can handle things like R-groups
# 3. AA sequence - Peptides e.g. can be described with a smiles, but AA sequence is more succinct.  Sometimes
#                  this can get ugly, because something might be made up of 2 sequences hooked together.
# 4. Nothing - We can have a name for something without any information about the structure.
#
# Each source can contain a mix. So e.g. chebi contains some with inchi, some with smiles, and some with nothing
#
# Synonymization process:
#  1. Handle all the stuff that has an InchiKey using unichem
#  2. Mesh is all "no structure".  We try to use a variety of sources to hook mesh id's to anything else
#  3. Pull from chebi the sdf and db files, use them to link to things (KEGG) in the no inchi/no smiles cases
#  4. Go to KEGG, and get sequences for peptides.
#  5. Pull UniProt (swissprot) XML.  Calculate sequences for the sub-sequences (Uniprot_PRO)
#  6. Use the sequences to merge UniProt_PRO with KEGG.
#  7. Read IUPHAR, discard things with INCHI, use things with sequence to match UniProt_PRO/KEGG
#     Use the hand-curated version of IUPHAR to match the un-sequenced stuff left over
#  8. Use wikidata to get links between CHEBI and UniProt_PRO
#  9. glom across sequence and chemical stuff
# 10. Drop PRO only sequences.
#
# It would be good to completely redo this so that it was make-like.
def load_chemicals(refresh_mesh=False,refresh_uniprot=False,refresh_pubchem=False,refresh_chembl=False):
    # Build if need be
    if refresh_mesh:
        refresh_mesh_pubchem()
    #Get all the simple stuff
    # 1. Handle all the stuff that has an InchiKey using unichem
    # 2. Mesh is all "no structure".  We try to use a variety of sources to hook mesh id's to anything else
    print('UNICHEM')
    concord = load_unichem()
    #debugging
    #for c in concord:
    #    if c.startswith('CHEMBL:'):
    #       print('alert!')
    #       print(c)
    #       exit()
    #print('no chembls yet')
    #
    # 2. Mesh is all "no structure".  We try to use a variety of sources to hook mesh id's to anything else
    #DO MESH/UNII
    print('MESH/UNII')
    mesh_unii_file = make_local_name( 'mesh_to_unii.txt')
    mesh_unii_pairs = load_pairs(mesh_unii_file, 'UNII')
    glom(concord, mesh_unii_pairs,pref='MESH')
    # DO MESH/PUBCHEM
    print('MESH/PUBCHEM')
    mesh_pc_file = make_local_name('mesh_to_pubchem.txt')
    mesh_pc_pairs = load_pairs(mesh_pc_file, 'PUBCHEM')
    glom(concord, mesh_pc_pairs,pref='MESH')
    # DO MESH/CHEBI, but don't combine any chebi's into a set with it
    print('MESH/CHEBI')
    mesh_chebi = pull_mesh_chebi()
    #Merging CHEBIS can be ok because of primary/secondary chebis.  Really we 
    # don't want to merge INCHIs
    #MESH/CHEBI is a real mess though.  wikidata has no principled way to connect identifiers.  It's just whatever
    # somebody said.  We really should use as a last resort.  If we don't know much about it, then sure.  But if
    # we've already got a chebi or a unii, then we should ignore this wiki stuff.
    mesh_chebi_filter = filter_mesh_chebi(mesh_chebi,concord)
    print(f"Started with {len(mesh_chebi)} m/c pairs")
    print(f"filtered to {len(mesh_chebi_filter)} m/c pairs")
    glom(concord, mesh_chebi_filter,pref='MESH')
    # 3. Pull from chebi the sdf and db files, use them to link to things (KEGG) in the no inchi/no smiles cases
    print('chebi')
    pubchem_chebi_pairs, kegg_chebi_pairs, chebi_unmapped = pull_chebi()
    glom(concord, pubchem_chebi_pairs,pref= 'CHEBI')
    glom(concord, kegg_chebi_pairs,pref='CHEBI')
    glom(concord, chebi_unmapped, pref='CHEBI')
    # 4. Go to KEGG, and get sequences for peptides.
    print('kegg')
    sequence_concord = pull_kegg_sequences()
    # 5. Pull UniProt (swissprot) XML.
    # Calculate sequences for the sub-sequences (Uniprot_PRO)
    print('uniprot')
    sequence_to_uniprot = pull_uniprot(refresh_uniprot)
    # 6. Use the sequences to merge UniProt with KEGG
    for s,v in sequence_to_uniprot.items():
        sequence_concord[s].update(v)
    # 7. Read IUPHAR, discard things with INCHI, use things with sequence to match UniProt_PRO/KEGG
    #     Use the hand-curated version of IUPHAR to match the un-sequenced stuff left over
    print('iuphar')
    sequence_to_iuphar, iuphar_glom = pull_iuphar()
    for s,v in sequence_to_iuphar.items():
        sequence_concord[s].update(v)
    glom(concord,iuphar_glom,pref='GTOPDB')
    #  8. Use wikidata to get links between CHEBI and UniProt_PRO
    unichebi = pull_uniprot_chebi()
    glom(concord, unichebi)
    #  9. glom across sequence and chemical stuff
    new_groups = sequence_concord.values()
    glom(concord,new_groups,unique_prefixes=['GTOPDB','INCHI'])
    # 10. Drop PRO only sequences.
    to_remove = []
    for eq_id_set in concord:
        if len(eq_id_set) > 1:
            continue
        print(eq_id_set)
        item = iter(eq_id_set).next()
        if '#PRO_' in item:
            to_remove.add(eq_id_set)
    for eids in to_remove:
        concord.remove(eids)
    #Add labels to CHEBIs, CHEMBLs, MESHes
    print('LABEL')
    label_chebis(concord)
    label_chembls(concord, refresh_chembl = refresh_chembl )
    #label_meshes(concord)
#    label_pubchem(concord, refresh_pubchem = refresh_pubchem)
    print('dumping')
    #Dump
    write_compendium(set([ frozenset(x) for x in concord.values() ]),'chemconc.txt','chemical_substance')
    print('done')

def get_chebi_label(ident):
    res = requests.get(f'https://uberonto.renci.org/label/{ident}/').json()
    return res['label']

def get_chembl_label(ident):
    res = requests.get(f'https://www.ebi.ac.uk/chembl/api/data/molecule/{Text.un_curie(ident)}.json').json()
    return res['pref_name']

def get_dict_label(ident, labels):
    try:
        return labels[ident]
    except KeyError:
        return None

def get_mesh_label(ident, labels):
    try:
        return labels[Text.un_curie(ident)]
    except KeyError:
        return ""

###

def label_chebis(concord):
    print('READ CHEBI')
    chebiobo = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/ontology', 'chebi_lite.obo')
    lines = chebiobo.split('\n')
    chebi_labels = {}
    for line in lines:
        if line.startswith('[Term]'):
            tid = None
            label = None
        elif line.startswith('id:'):
            tid = line[3:].strip()
        elif line.startswith('name:'):
            label = line[5:].strip()
            chebi_labels[tid] = label
    print('LABEL CHEBI')
    label_compounds(concord, 'CHEBI', partial(get_dict_label, labels=chebi_labels))
    # label_compounds(concord,'CHEBI',get_chebi_label)


def process_chunk(lines, label_dict):
    if len(lines) == 0:
        return
    if not lines[0].startswith('chembl_molecule'):
        return
    chemblid = f"CHEMBL.COMPOUND:{lines[0].split()[0].split(':')[1]}"
    label = None
    for line in lines[1:]:
        s = line.strip()
        if s.startswith('rdfs:label'):
            label = s.split()[1]
            if label.startswith('"'):
                label = label[1:]
            if label.endswith('"'):
                label = label[:-1]
    if label is not None:
        label_dict[chemblid] = label


def label_chembls(concord, refresh_chembl = False):
    print('READ CHEMBL')
    fname = 'chembl_25.0_molecule.ttl.gz'
    localfile = make_local_name(fname[:-3])
    # uncomment if you need a new one
    if refresh_chembl:
        data=pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/25.0/',fname,decompress_data=True,outfilename=fname[:-3])
    chembl_labels = {}
    chunk = []
    with open(localfile, 'r') as inf:
        for line in inf:
            l = line.strip()
            if len(l) == 0:
                process_chunk(chunk, chembl_labels)
                chunk = []
            elif l.startswith('@'):
                pass
            else:
                chunk.append(l)
    print('LABEL CHEMBL', len(chembl_labels))
    label_compounds(concord, 'CHEMBL.COMPOUND', partial(get_dict_label, labels=chembl_labels))
    # label_compounds(concord,'CHEMBL',get_chembl_label)


def label_meshes(concord):
    print('LABEL MESH')
    #labelname = os.path.join(os.path.dirname(__file__), 'meshlabels.pickle')
    labelname = make_local_name('meshlabels.pickle')
    with open(labelname, 'rb') as inf:
        mesh_labels = pickle.load(inf)
    label_compounds(concord, 'MESH', partial(get_mesh_label, labels=mesh_labels))

def label_pubchem(concord, refresh_pubchem = False):
    print('LABEL PUBCHEM')
    f_name =  'CID-IUPAC.gz'
    if refresh_pubchem:
        outfname = pull_via_ftp('ftp.ncbi.nlm.nih.gov','/pubchem/Compound/Extras/', f_name, outfilename=f_name)
    else:
        outfname = make_local_name(f_name)
    labels = {}
    with gzip.open(outfname, 'rt') as in_file:
        for line in in_file:
            # since the synonyms are weighted already will just pick the first one.
            l = line.strip()
            cid, label = l.split('\t')
            if f'PUBCHEM:{cid}' in labels:
                continue
            labels[f'PUBCHEM:{cid}'] = label
    label_compounds(concord, 'PUBCHEM', partial(get_dict_label, labels= labels))


###

def label_compounds(concord, prefix, get_label):
    foundlabels = {}
    for k, v in concord.items():
        to_remove = []
        to_add = []
        for ident in v:
            if Text.get_curie(ident) == prefix:
                if not ident in foundlabels:
                    label = get_label(ident)
                    if label is not None:
                        lid = LabeledID(ident, get_label(ident))
                        foundlabels[ident] = lid
                    else:
                        foundlabels[ident] = None
                label = foundlabels[ident]
                if label is not None:
                    to_remove.append(ident)
                    to_add.append(foundlabels[ident])
        for r in to_remove:
            v.remove(r)
        for r in to_add:
            v.add(r)


def remove_ticks(s):
    if s.startswith("'"):
        s = s[1:]
    if s.endswith("'"):
        s = s[:-1]
    return s


def load_pairs(fname, prefix):
    pairs = []
    with open(fname, 'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            mesh = f"MESH:{x[0]}"
            if x[1].startswith('['):
                pre_ids = x[1][1:-1].split(',')
                pre_ids = [remove_ticks(pids.strip()) for pids in pre_ids]  # remove spaces and ' marks around ids
            else:
                pre_ids = [x[1]]
            ids = [f'{prefix}:{pid}' for pid in pre_ids]
            for identifier in ids:
                pairs.append((mesh, identifier))
    return pairs


def uni_glom(unichem_data, prefix1, prefix2, chemdict):
    print(f'{prefix1}/{prefix2}')
    n = unichem_data.split('\n')[1:]
    if len(n[-1]) == 0:
        n = n[:-1]
    pairs = [ni.split('\t') for ni in n]
    for p in pairs:
        if p[0].startswith("'") or p[1].startswith("'"):
            print('UNI_GLOM {prefix1} {prefix2} {p}')
    curiepairs = [(f'{prefix1}:{p[0]}', f'{prefix2}:{p[1]}') for p in pairs]
    glom(chemdict, curiepairs)



#Note that sometime between September and December 2019, the UCI moved in UNICHEM's files
#Which by the way, don't have a header in the file itself, but which are given an a readme :(
#So there's no computer only way to figure this out :( :( :(
def uci_key(row):
    try:
        return int(row.split(b'\t')[9])
    except Exception as e:
        print(row)
        exit()

#########################
# load_unichem() - Loads a dict object with targeted chemical substance curies for synonymization
#
# TODO: get the column header from the readme.  Unfortunately means that we need the readme not to change...
#
# The XREF file format from unichem
# ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI<the latest>/UC_XREF.txt.gz
# September 2019:
# cols: uci   src_id    src_compound_id   assignment   last_release_u_when_current   created   lastupdated   userstamp   aux_src
# December 2019:
# cols: uci_old , src_id , src_compound_id , assignment , last_release_u_when_current , created , lastupdated , userstamp , aux_src , uci
#
# The STRUCTURE file format from unichem
# ftp.ebi.ac.uk/pub/databases/chembl/UniChem/data/oracleDumps/UDRI<the latest>/UC_STRUCTURE.txt.gz
# September 2019:
# cols: uci   standardinchi   standardinchikey   created   username   fikhb
# December 2019:
# cols: uci_old , standardinchi , standardinchikey , created , username , fikhb , uci , parent_smiles
#
# working_dir: str - the working directory for the downloaded files
# xref_file: str - optional location of already downloaded and decompressed unichem XREF file
# struct_file: str - optional location of already downloaded and decompressed unichem STRUCTURE file
# return: dict - The cross referenced curies ready for inserting into the the redis cache
#########################
def load_unichem(working_dir: str = '', xref_file: str = None, struct_file: str = None) -> dict:
    #FOR TESTING
    upname = make_local_name('unichem.pickle')
    with open(upname,'rb') as up:
        synonyms=pickle.load(up)
    return synonyms
    #DONE TESTING
    logger.info(f'Start of Unichem loading. Working directory: {working_dir}')

    # init the returned list
    synonyms: dict = {}

    # init a chemicals counter
    chem_counter: int = 0

    try:
        # declare the unichem ids for the target data
        data_sources: dict = {1: 'CHEMBL.COMPOUND', 2: 'DRUGBANK', 4: 'GTOPDB', 6: 'KEGG.COMPOUND', 7: 'CHEBI', 14: 'UNII', 18: 'HMDB', 22: 'PUBCHEM'}

        # get the newest UniChem data directory name
        if xref_file is None or struct_file is None:
            # get the latest UC directory name
            target_uc_url: str = get_latest_unichem_url()
            logger.info(f'Target unichem FTP URL: {target_uc_url}')

            # get the files
            #xref_file = pull_via_urllib(target_uc_url, 'UC_XREF.txt.gz', decompress=False)
            #struct_file = pull_via_urllib(target_uc_url, 'UC_STRUCTURE.txt.gz' )
            #shortcut to local files.
            xref_file = make_local_name('UC_XREF.txt.gz')
            struct_file = make_local_name('UC_STRUCTURE.txt')

        logger.info(f'Using decompressed UniChem XREF file: {xref_file} and STRUCTURE file: {struct_file}')
        logger.info(f'Start of data pre-processing.')

        logger.debug('filter xrefs by srcid')
        srcfiltered_xref_file=make_local_name('UC_XREF.srcfiltered.txt')
        srclist = [ str(k) for k in data_sources.keys()]
        with gzip.open(xref_file,'rt') as inf, open(srcfiltered_xref_file,'w') as outf:
            for line in inf:
                x = line.split('\t')
                if x[1] in srclist and x[3] == '1':
                    outf.write(line)

        sorted_xref_file=make_local_name('UC_XREF.sorted.txt')
        logger.debug(f'sort xrefs {xref_file}=>{sorted_xref_file}')
        with open(srcfiltered_xref_file,'rb',64*1024) as inf, open(sorted_xref_file,'wb') as outf:
            batch_sort(inf,outf,key=uci_key,tempdirs='.')
        logger.debug('.. done ..')

        logger.debug('remove singletons')
        filtered_xref_file=make_local_name('UC_XREF.filtered.txt')
        srclist = [ str(k) for k in data_sources.keys()]
        #There's a particular problem with UNII (src id 14). Because they can't
        #seem to generate inchikeys nicely, there are sometimes 2 UNIIs per key
        # if we leave them, it's bad.  And we don't know which we should use, so
        # take them out. 
        with open(sorted_xref_file,'r') as inf, open(filtered_xref_file,'w') as outf:
            lines = []
            uniilines = []
            lastuci = ''
            for line in inf:
                x = line.split('\t')
                if x[0] != lastuci:
                    if len(uniilines) == 1:
                        lines.append(uniilines[0])
                    if len(lines) > 1:
                        for wline in lines:
                            outf.write(wline)
                    lines=[]
                    uniilines=[]
                    lastuci=x[0]
                if x[1] == '14':
                    uniilines.append(line)
                else:
                    lines.append(line)
        logger.debug('.. done ..')

        logger.debug('read filtered')
        #column 9 seems like a good place for the PK
        df_filtered_xrefs = pandas.read_csv(filtered_xref_file, dtype={"uci": int, "src_id": int, "src_compound_id": str},
                                            sep='\t', header=None, usecols=['uci','src_id','src_compound_id'],
                                            names=['uci_old','src_id','src_compound_id','assignment','last_release_u_when_current','created ','lastupdated','userstamp','aux_src','uci'])
        logger.debug('..done..')

        # note: this is an alternate way to add a curie column to each record in one shot. takes about 10 minutes.
        df_filtered_xrefs = df_filtered_xrefs.assign(curie=df_filtered_xrefs[['src_id', 'src_compound_id']].apply(lambda x: f'{data_sources[x[0]]}:{x[1]}', axis=1))
        logger.debug(f'Curie column addition complete. Creating STRUCTURE iterator...')

        # get an iterator to loop through the xref data
        structure_iter = pandas.read_csv(struct_file, dtype={"uci": int, "standardinchikey": str},
                                         sep='\t', header=None, usecols=['uci', 'standardinchikey'],
                                         names=['uci_old','standardinchi','standardinchikey','created','username','fikhb','uci','parent_smiles'],
                                         iterator=True, chunksize=100000)
        logger.debug(f'STRUCTURE iterator created. Loading structure data frame, filtering by targeted XREF unichem ids...')

        # load it into a data frame
        df_structures = pandas.concat(struct_element[struct_element['uci'].isin(df_filtered_xrefs.uci)] for struct_element in structure_iter)
        logger.debug(f'STRUCTURE data frame created with filtered with XREF unichem ids. {len(df_structures)} records loaded.')

        # group the records by the unichem identifier
        xref_grouped = df_filtered_xrefs.groupby(by=['uci'])
        logger.debug(f'STRUCTURE data frame grouped by XREF unichem ids.')

        logger.info('Data pre-processing complete. Start of final data processing...')

        # for each of the structured records use the uci to get the xref records
        for name, group in xref_grouped:
            # get the synonym group into a list
            syn_list: list = group.curie.tolist()

            # add the inchikey to the list
            syn_list.append('INCHIKEY:' + df_structures[df_structures.uci == name]['standardinchikey'].values[0])

            # create a dict of all the curies. each element gets equated with the whole list
            syn_dict: dict = dict.fromkeys(syn_list, set(syn_list))

            # add it to the returned list
            synonyms.update(syn_dict)

            # increment the counter
            chem_counter += 1

            # output some feedback for the user
            if (chem_counter % 250000) == 0:
                logger.info(f'Processed {chem_counter} unichem chemicals...')
    except KeyError as e:
    #except Exception as e:
        logger.error(f'Exception caught. Exception: {e}')

    logger.info(f'Load complete. Processed a total of {chem_counter} unichem chemicals.')
    upname = make_local_name('unichem.pickle')
    with open(upname,'wb') as up:
        pickle.dump(synonyms,up)

    # return the resultant list set to the caller
    return synonyms

#########################
# get_latest_unichem_url() - gets the latest UniChem data directory url
#
# return: str - the unichem FTP URL
#########################
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

async def make_uberon_role_queries(chebi_ids, chemical_annotator):
    tasks = []
    for id in chebi_ids:
        tasks.append(chemical_annotator.get_chemical_roles(id))
    results = await asyncio.gather(*tasks)

    reformatted_result = {}
    for result in results:
        for chebi_id in result:
            reformatted_result[chebi_id] = list(map(lambda x: x['role_label'], result[chebi_id]))
    return reformatted_result


def merge_roles_and_annotations(chebi_role_data, chebi_annotation_data):
    """
    Merges roles into the bigger annotation dict as roles key.
    """
    for chebi_id in chebi_role_data:
        for key in chebi_role_data[chebi_id]:
            chebi_annotation_data[chebi_id][key] = True
        yield (chebi_id, chebi_annotation_data[chebi_id])


def annotate_from_chebi(rosetta):
    chebisdf = pull_and_decompress('ftp.ebi.ac.uk', '/pub/databases/chebi/SDF/', 'ChEBI_complete_3star.sdf.gz')
    chunk = []
    logger.debug('caching chebi annotations')
    # grab a bunch of them to make use of concurrent execution for fetching roles from Uberon
    result_buffer = {}
    num_request_per_round = 500
    loop = asyncio.new_event_loop()
    chemical_annotator = ChemicalAnnotator(rosetta)
    interesting_keys = chemical_annotator.config['CHEBI']['keys']
    lines = chebisdf.split('\n')
    count = 0
    for line in lines:
        if '$$$$' in line:
            chebi_set = chebi_sdf_entry_to_dict(chunk, interesting_keys=interesting_keys)
            chunk = []
            result_buffer[chebi_set[0]] = chebi_set[1]
            if len(result_buffer) == num_request_per_round:
                chebi_role_data = loop.run_until_complete(make_uberon_role_queries(result_buffer.keys(), chemical_annotator))
                for entry in merge_roles_and_annotations(chebi_role_data, result_buffer):
                    # entry[0] is the chebi id
                    rosetta.cache.set(f'annotation({Text.upper_curie(entry[0])})', entry[1])
                    # clear buffer
                    count += 1
                result_buffer = {}
                logger.debug(f'cached {count} entries... ')
        else:
            if line != '\n':
                line = line.strip('\n')
                chunk += [line]
        
    if len(result_buffer) != 0 :
        #deal with the last pieces left in the buffer
        chebi_role_data = loop.run_until_complete(make_uberon_role_queries(result_buffer.keys(),chemical_annotator))
        for entry in merge_roles_and_annotations(chebi_role_data, result_buffer):
            rosetta.cache.set(f'annotation({Text.upper_curie(entry[0])})', entry[1])
    logger.debug('done caching chebi annotations...')
    loop.close()

def chebi_sdf_entry_to_dict(sdf_chunk, interesting_keys={}):
    """
    Converts each SDF entry to a dictionary
    """
    final_dict = {}
    current_key = 'mol_file'
    chebi_id = ''
    for line in sdf_chunk:
        if len(line):
            if '>' == line[0]:
                current_key = line.replace('>', '').replace('<', '').strip().replace(' ', '').lower()
                current_key = 'formula' if current_key == 'formulae' else current_key
                if current_key in interesting_keys:
                    final_dict[interesting_keys[current_key]] = ''
                continue
            if current_key == 'chebiid':
                chebi_id = line
            if current_key in interesting_keys:
                final_dict[interesting_keys[current_key]] += line
    return (chebi_id, final_dict)


async def make_multiple_chembl_requests(num_requests=100, start=0):
    """
    Fetches 1000 records per request beginning from 'start' till 'num_requests' * 1000
    """
    tasks = []
    for i in range(0, num_requests):
        offset = i * 1000 + start  # chebml api returns 1000 records max
        url = f"https://www.ebi.ac.uk/chembl/api/data/molecule?format=json&limit=0&offset={offset}"
        tasks.append(async_client.async_get_json(url, {}))
    results = await asyncio.gather(*tasks)
    return results


def annotate_from_chembl(rosetta):
    """
    Gets and caches chembl annotations.
    """
    j = 100  # assume first that we can finish the whole thing with 100 rounds of 100 request for each round
    all_results = []
    logger.debug('annotating chembl data')
    annotator = ChemicalAnnotator(rosetta)
    for i in range(0, j):
        # open the loop
        loop = asyncio.new_event_loop()
        num_requests = 100
        start = (num_requests * 1000) * i
        results = loop.run_until_complete(make_multiple_chembl_requests(num_requests=num_requests, start=start))
        loop.close()
        if i == 0:
            # determine the actual number of records to not just guess when we should stop
            total_count = results[0]['page_meta']['total_count']
            j = round(total_count / (1000 * num_requests))
        for result in results:
            extract_chebml_data_add_to_cache(result, annotator, rosetta)
        logger.debug(f'done annotating {(i / j) * 100} % of chembl')

    logger.debug('caching chebml stuff done...')


def extract_chebml_data_add_to_cache(result, annotator, rosetta):
    """
    Helper function to parse out and extract useful info form a single request result from chebml api.
    """
    molecules = result['molecules']
    for molecule in molecules:
        extract = annotator.extract_chembl_data(molecule, annotator.get_prefix_config('CHEMBL.COMPOUND')['keys'])
        logger.debug(extract)
        chembl_id = molecule['molecule_chembl_id']
        rosetta.cache.set(f"annotation({Text.upper_curie(chembl_id)})", extract)


def load_annotations_chemicals(rosetta):
    annotate_from_chebi(rosetta)
    annotate_from_chembl(rosetta)

#######
# Main - Stand alone entry point for testing
#######
if __name__ == '__main__':
    load_chemicals(refresh_mesh=False,refresh_uniprot=False,refresh_pubchem=True,refresh_chembl=False)
    #load_unichem(working_dir='.',xref_file='UC_XREF.txt.gz',struct_file='UC_STRUCTURE.txt')

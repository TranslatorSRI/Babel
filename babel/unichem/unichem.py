import ftplib
import gzip
import pickle
import logging

import pandas

from src.util import LoggingUtil
from babel.babel_utils import make_local_name,pull_via_urllib
from babel.big_gz_sort import batch_sort

logger = LoggingUtil.init_logging("chemicals", logging.DEBUG, format='medium')

def load_unichem(working_dir: str = '', xref_file: str = None, struct_file: str = None, refresh=False) -> dict:
    if not refresh:
        upname = make_local_name('unichem.pickle')
        with open(upname,'rb') as up:
            synonyms=pickle.load(up)
        return synonyms
    else:
        return refresh_unichem(working_dir,xref_file,struct_file)


def refresh_unichem(working_dir: str = '', xref_file: str = None, struct_file: str = None) -> dict:
    logger.info(f'Start of Unichem loading. Working directory: {working_dir}')

    # declare the unichem ids for the target data
    data_sources: dict = {1: 'CHEMBL.COMPOUND', 2: 'DRUGBANK', 4: 'GTOPDB', 6: 'KEGG.COMPOUND', 7: 'CHEBI', 14: 'UNII', 18: 'HMDB', 22: 'PUBCHEM.COMPOUND'}
    #data_sources: dict = {1: 'CHEMBL.COMPOUND', 2: 'DRUGBANK', 4: 'GTOPDB', 6: 'KEGG.COMPOUND', 7: 'CHEBI', 14: 'UNII', 18: 'HMDB', 22: 'PUBCHEM'}

    # get the newest UniChem data directory name
    struct_file, xref_file = get_unichem_files(struct_file, xref_file)

    logger.info(f'Using decompressed UniChem XREF file: {xref_file} and STRUCTURE file: {struct_file}')
    logger.info(f'Start of data pre-processing.')

    logger.debug('filter xrefs by srcid')
    srcfiltered_xref_file = filter_xrefs_by_srcid(data_sources, xref_file)

    sorted_xref_file = sort_xref_file(srcfiltered_xref_file, xref_file)
    sorted_struct_file = sort_struct_file(struct_file)

    logger.debug('filter unii')
    #we used to remove singletons.  Now we don't but we do need to handle the unii
    #problem, so we still do this light filtering.
    filtered_xref_file = filter_bad_unii(data_sources, sorted_xref_file)

    synonyms = merge_xref_with_structure(filtered_xref_file,sorted_struct_file)
    print('Synonyms done, now pickle')

    upname = make_local_name('unichem.pickle')
    with open(upname,'wb') as up:
        pickle.dump(synonyms,up)

    # return the resultant list set to the caller
    return synonyms

def advance_xrefs(line,xrefs):
    """Given the last line read from the xrefs file, plus the xrefs file pointer, having just read that line,
    read the rest of the lines with the same uci, and return them, along with the first line after them."""
    #The columns are: [0'uci_old', 1'src_id', 2'src_compound_id', 3'assignment', 4'last_release_u_when_current', 5 'created ',
    # 6'lastupdated', 7'userstamp', 8'aux_src', 9'uci'])
    # we want: ['uci', 'src_id', 'src_compound_id'],, i.e. 9, 1, 2
    group = []
    x = line.split('\t')
    t = (int(x[9].strip()), int(x[1]), x[2])
    uci = t[0]
    original_uci = uci
    #loop over new lines, until we get a new uci or EOF
    while uci == original_uci and not line == "":
        if uci == original_uci:
            group.append(t)
        line = xrefs.readline()
        if not line == "":
            #This is too simple.  There's at least one line with no x[0], so stripping changes all the positions. 
            #x = line.strip().split('\t')
            x = line.split('\t')
            t = (int(x[9].strip()), int(x[1]), x[2])
            uci = t[0]
    return group,original_uci,line

def get_inchi(uci, struct_file):
    """Given a (int) uci and an open structure file, go until we get to that uci and give back the structure.
    Assumes that the struct_file is sorted on uci.  We'll check for that and blow up if it fails"""
    #struct header [0'uci_old', 1'standardinchi', 2'standardinchikey', 3'created', 4'username', 5'fikhb', 6'uci', 'parent_smiles'],
    found_uci = -1
    while found_uci != uci:
        line = struct_file.readline().strip().split('\t')
        found_uci = int(line[6])
        if found_uci > uci:
            print(f'Found a uci too big. Looking for {uci} but got to {found_uci}')
            print('Are you sure that the structure file is sorted by uci?')
            exit()
        if len(line) == 0:
            print(f'got to the end of the structfile without finding uci {uci}')
            exit()
        return line[2]



#replaces merge_xref_with_structure_pandas
def merge_xref_with_structure(filtered_xref_file,struct_file):
    """Given an xref file which is already filtered to structures of interest, and is sorted by uci_key and a
    structure file from which we can pull inchikeys, and which is also sorted by uci_key, create a list of
    synonymous chemicals by walking through the two files in parallel."""
    #Used to construct curies.  It's repeated from load_unichem which is a bad smell.
    data_sources: dict = {1: 'CHEMBL.COMPOUND', 2: 'DRUGBANK', 4: 'GTOPDB', 6: 'KEGG.COMPOUND', 7: 'CHEBI', 14: 'UNII', 18: 'HMDB', 22: 'PUBCHEM.COMPOUND'}
    #initialize
    synonyms: dict = {}
    chem_counter = 0
    with open(filtered_xref_file,'r') as xrefs, open(struct_file,'r') as structs:
        xrefline = xrefs.readline().strip()
        while xrefline != '':
            nextgroup,uci,xrefline = advance_xrefs(xrefline,xrefs)
            inchi = get_inchi(uci,structs)
            syn_list = [f'{data_sources[t[1]]}:{t[2]}' for t in nextgroup]
            syn_list.append(f'INCHIKEY:{inchi}')
            # create a dict of all the curies. each element gets equated with the whole list
            syn_dict: dict = dict.fromkeys(syn_list, set(syn_list))
            # add it to the returned list
            synonyms.update(syn_dict)
            # increment the counter
            chem_counter += 1
            # output some feedback for the user
            if (chem_counter % 250000) == 0:
                logger.info(f'Processed {chem_counter} unichem chemicals...')
                print(f'Processed {chem_counter} unichem chemicals...')
    return synonyms


#Here's the original, very slow, implementation.  I can't see any reason to think another approach
# will be faster, but it will at least be more memory efficient not to load everything at once.
def merge_xref_with_structure_pandas(filtered_xref_file,struct_file):
    # init the returned list
    synonyms: dict = {}
    chem_counter = 0

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

    logger.info(f'Load complete. Processed a total of {chem_counter} unichem chemicals.')
    return synonyms

def get_unichem_files(struct_file, xref_file):
    if xref_file is None or struct_file is None:
        #pull
        # get the latest UC directory name
        #target_uc_url: str = get_latest_unichem_url()
        #logger.info(f'Target unichem FTP URL: {target_uc_url}')
        # get the files
        #xref_file = pull_via_urllib(target_uc_url, 'UC_XREF.txt.gz', decompress=False)
        #struct_file = pull_via_urllib(target_uc_url, 'UC_STRUCTURE.txt.gz' )

        # shortcut to local files.
        xref_file = make_local_name('UC_XREF.txt.gz')
        struct_file = make_local_name('UC_STRUCTURE.txt')
    return struct_file, xref_file


def filter_bad_unii(data_sources, sorted_xref_file):
    filtered_xref_file = make_local_name('UC_XREF.filtered.txt')
    srclist = [str(k) for k in data_sources.keys()]
    # There's a particular problem with UNII (src id 14). Because they can't
    # seem to generate inchikeys nicely, there are sometimes 2 UNIIs per key
    # if we leave them, it's bad.  And we don't know which we should use, so
    # take them out.
    with open(sorted_xref_file, 'r') as inf, open(filtered_xref_file, 'w') as outf:
        lines = []
        uniilines = []
        lastuci = ''
        for line in inf:
            x = line.split('\t')
            if x[0] != lastuci:
                if len(uniilines) == 1:
                    lines.append(uniilines[0])
                # we had been filtering out singletons, which made sense if we only wanted synonyms
                # but in a nodenormalization setting, we want to recognize them all, even if we don't know
                # any other names for them
                # if len(lines) > 1:
                for wline in lines:
                    outf.write(wline)
                lines = []
                uniilines = []
                lastuci = x[0]
            if x[1] == '14':
                uniilines.append(line)
            else:
                lines.append(line)
    logger.debug('.. done ..')
    return filtered_xref_file


def sort_xref_file(srcfiltered_xref_file, xref_file):
    sorted_xref_file = make_local_name('UC_XREF.sorted.txt')
    logger.debug(f'sort xrefs {xref_file}=>{sorted_xref_file}')
    with open(srcfiltered_xref_file, 'rb', 64 * 1024) as inf, open(sorted_xref_file, 'wb') as outf:
        batch_sort(inf, outf, key=uci_key, tempdirs='.')
    logger.debug('.. done ..')
    return sorted_xref_file

def sort_struct_file(struct_file):
    sorted_struct_file = make_local_name('UC_STRUCT.sorted.txt')
    with open(struct_file, 'rb', 64 * 1024) as inf, open(sorted_struct_file, 'wb') as outf:
        batch_sort(inf, outf, key=uci_key_2, tempdirs='.')
    logger.debug('.. done ..')
    return sorted_struct_file



def filter_xrefs_by_srcid(data_sources, xref_file):
    srcfiltered_xref_file = make_local_name('UC_XREF.srcfiltered.txt')
    srclist = [str(k) for k in data_sources.keys()]
    with gzip.open(xref_file, 'rt') as inf, open(srcfiltered_xref_file, 'w') as outf:
        for line in inf:
            x = line.split('\t')
            if x[1] in srclist and x[3] == '1':
                outf.write(line)
    return srcfiltered_xref_file


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


#Note that sometime between September and December 2019, the UCI moved in UNICHEM's files
#Which by the way, don't have a header in the file itself, but which are given an a readme :(
#So there's no computer only way to figure this out :( :( :(
def uci_key(row):
    try:
        return int(row.split(b'\t')[9])
    except Exception as e:
        print(row)
        exit()

def uci_key_2(row):
    try:
        return int(row.split(b'\t')[6])
    except Exception as e:
        print(row)
        exit()

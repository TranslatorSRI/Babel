import subprocess
import traceback
from enum import Enum
from ftplib import FTP
from io import BytesIO
import gzip
from datetime import datetime as dt, datetime
from datetime import timedelta
import time
import requests
import os
import urllib
import jsonlines
from humanfriendly import format_timespan

from src.metadata.provenance import write_combined_metadata
from src.node import NodeFactory, SynonymFactory, DescriptionFactory, InformationContentFactory, TaxonFactory
from src.properties import PropertyList, HAS_ALTERNATIVE_ID
from src.util import Text, get_config, get_memory_usage_summary, get_logger
from src.LabeledID import LabeledID
from collections import defaultdict
import sqlite3
from typing import List, Tuple

# Configuration items
WRITE_COMPENDIUM_LOG_EVERY_X_CLIQUES = 1_000_000

# Set up a logger.
logger = get_logger(__name__)

def make_local_name(fname,subpath=None):
    config = get_config()
    if subpath is None:
        return os.path.join(config['download_directory'],fname)
    odir = os.path.join(config['download_directory'],subpath)
    os.makedirs(odir, exist_ok=True)
    return os.path.join(odir,fname)


class StateDB():
    def __init__(self,fname):
        self.dbname = make_local_name(fname)
        new = True
        if os.path.exists(self.dbname):
            new = False
        self.connection = sqlite3.connect(self.dbname)
        if new:
            self.initialize_db()
    def initialize_db(self):
        curr = self.connection.cursor()
        curr.execute('CREATE TABLE cache (key text, value text)')
        self.connection.commit()
    def get(self,key):
        curr = self.connection.cursor()
        curr.execute('SELECT value FROM cache WHERE key=?', (key,))
        result = curr.fetchone()
        if result is not None:
            return result[0]
        return None
    def put(self,key,value):
        curr=self.connection.cursor()
        curr.execute(f"INSERT INTO cache VALUES (?,?)",(key,value))
        self.connection.commit()

#The signature here should be modified to be like pull via urlllib
def pull_via_ftp(ftpsite, ftpdir, ftpfile, decompress_data=False, outfilename=None):
    """Retrieve data via ftp.
    Setting decompress=True will ungzip the data
    If outfilename is None (default) then the data will be returned.
    Otherwise it will be written to the downloads directory."""
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    print('   getting data')
    config = get_config()
    if outfilename is None:
        with BytesIO() as data:
            ftp.retrbinary(f'RETR {ftpfile}', data.write)
            ftp.quit()
            binary = data.getvalue()
            if decompress_data:
                return gzip.decompress(binary).decode()
            else:
                return binary.decode()
    ofilename = os.path.join(config['download_directory'],outfilename)
    odir = os.path.abspath(os.path.dirname(ofilename))
    if not os.path.exists(odir):
        os.makedirs(odir)
    print(f'  writing data to {ofilename}')
    print(f'{ftpsite}/{ftpdir}/{ftpfile}')
    if not decompress_data:
        with open(ofilename,'wb') as ofile:
            ftp.retrbinary(f'RETR {ftpfile}', ofile.write)
            ftp.quit()
    else:
        with BytesIO() as data:
            ftp.retrbinary(f'RETR {ftpfile}', data.write)
            ftp.quit()
            value = gzip.decompress(data.getvalue()).decode()
        with open(ofilename,'w') as ofile:
            ofile.write(value)
    return ofilename

def dump_dict(outdict,outfname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__),config['download_directory'], outfname)
    with open(oname,'w') as outf:
        for k,v in outdict.items():
            outf.write(f'{k}\t{v}\n')

def dump_dicts(dicts,fname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__),config['download_directory'], fname)
    with open(oname,'w') as outf:
        for k in dicts:
            outf.write(f'{k}\t{dicts[k]}\n')

def dump_sets(sets,fname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__),config['download_directory'], fname)
    print('dumping: ',oname)
    with open(oname,'w') as outf:
        for s in sets:
            outf.write(f'{s}\n')

class ThrottledRequester:
    """Make sure that the time from the last call to the current call is greater than or equal to
    a configurable delta.   Wait before making request to ensure this. Used to make sure eutils
    doesn't get angry.  Returns the json, as well as a flag whether this call waited or not."""
    def __init__(self,delta_ms):
        self.last_time = None
        self.delta = timedelta(milliseconds = delta_ms)
    def get(self,url):
        now = dt.now()
        throttled=False
        if self.last_time is not None:
            cdelta = now - self.last_time
            if cdelta < self.delta:
                waittime = self.delta - cdelta
                time.sleep(waittime.microseconds / 1e6)
                throttled = True
        self.last_time = dt.now()
        response = requests.get(url)
        return response, throttled
    def get_json(self,url):
        """Add retries to the throttling, return json"""
        ntries = 0
        maxtries = 100
        while ntries < maxtries:
            try:
                response,_ = self.get(url)
                result = response.json()
                return result
            except Exception as e:
                ntries += 1



def pull_via_urllib(url: str, in_file_name: str, decompress = True, subpath=None):
    """
    Retrieve files via urllib, optionally decompresses it, and writes it locally into downloads
    url: str - the url with the correct version attached
    in_file_name: str - the name of the target file to work
    returns: str - the output file name
    """
    #Everything goes in downloads
    download_dir = get_config()['download_directory']
    working_dir = download_dir

    # get the (local) download file name, derived from the input file name
    if subpath is None:
        dl_file_name = os.path.join(download_dir,in_file_name)
    else:
        dl_file_name = os.path.join(download_dir,subpath,in_file_name)

    # Add support for redirects
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())

    # get a handle to the ftp file
    print(url+in_file_name)
    handle = opener.open(url + in_file_name)

    # create the compressed file
    with open(dl_file_name, 'wb') as compressed_file:
        # while there is data
        while True:
            # read a block of data
            data = handle.read(1024)

            # fif nothing read about
            if len(data) == 0:
                break

            # write out the data to the output file
            compressed_file.write(data)

    if decompress:
        out_file_name = dl_file_name[:-3]

        # create the output text file
        with open(out_file_name, 'w') as output_file:
            # open the compressed file
            with gzip.open(dl_file_name, 'rt') as compressed_file:
                for line in compressed_file:
                    # write the data to the output file
                    output_file.write(line)

        #remove the compressed file
        os.remove(dl_file_name)
    else:
        out_file_name = dl_file_name

    # return the filename to the caller
    return out_file_name


# Recursion options for pull_via_wget().
# See https://www.gnu.org/software/wget/manual/html_node/Recursive-Download.html for wget's recursion options.
class WgetRecursionOptions(Enum):
    NO_RECURSION = 0                 # Don't do any recursion
    RECURSE_SUBFOLDERS = 1           # Recurse into subfolders -- equivalent to `-np`
    RECURSE_DIRECTORY_ONLY = 2       # Recurse through a single directory only -- equivalent to `-np -l1`


def pull_via_wget(
        url_prefix: str,
        in_file_name: str,
        decompress=True,
        subpath:str=None,
        outpath:str=None,
        continue_incomplete:bool=True,
        timestamping=True,
        recurse:WgetRecursionOptions = WgetRecursionOptions.NO_RECURSION,
        retries:int=10):
    """
    Download a file using wget. We call wget from the command line, and use command line options to
    request continuing incomplete downloads.

    :param url_prefix: The URL prefix to download.
    :param in_file_name: The filename to download -- this will be concatenated to the URL prefix. This should include
        the compression extension (e.g. `.gz`); we will remove that extension during decompression. If recursion is
        turned on, in_file_name refers to the directory where the recursive content will be downloaded.
    :param decompress: Whether this is a Gzip file that should be decompressed after download.
    :param subpath: The subdirectory of `babel_download` where this file should be stored.
    :param outpath: The full output directory to write this file to. Both subpath and outpath cannot be set at the same time.
    :param continue_incomplete: Should wget continue an incomplete download?
    :param recurse: Do we want to download recursively? Should be from Wget_Recursion_Options, such as Wget_Recursion_Options.NO_RECURSION.
    :param retries: The number of retries to attempt.
    """

    # Prepare download URL and location
    download_dir = get_config()['download_directory']
    url = url_prefix + in_file_name
    if subpath and outpath:
        raise RuntimeError("pull_via_wget() cannot be called with both subpath and outpath set.")
    elif outpath:
        dl_file_name = outpath
    elif subpath:
        dl_file_name = os.path.join(download_dir, subpath, in_file_name)
    else:
        dl_file_name = os.path.join(download_dir, in_file_name)

    # Prepare wget options.
    wget_command_line = [
        'wget',
        '--progress=bar:force:noscroll',
    ]
    if continue_incomplete:
        wget_command_line.append('--continue')
    if timestamping:
        wget_command_line.append('--timestamping')
    if retries > 0:
        wget_command_line.append(f'--tries={retries}')

    # Add URL and output file.
    wget_command_line.append(url)

    # Handle recursion options
    match recurse:
        case WgetRecursionOptions.NO_RECURSION:
            # Write to a single file, dl_file_name
            wget_command_line.extend(['-O', dl_file_name])
        case WgetRecursionOptions.RECURSE_SUBFOLDERS:
            # dl_file_name should be a directory name.
            wget_command_line.extend(['--recursive', '--no-parent', '--no-directories', '--directory-prefix=' + dl_file_name])
        case WgetRecursionOptions.RECURSE_DIRECTORY_ONLY:
            # dl_file_name should be a directory name.
            wget_command_line.extend(['--recursive', '--no-parent', '--no-directories', '--level=1', '--directory-prefix=' + dl_file_name])

    # Execute wget.
    logger.info(f"Downloading {dl_file_name} using wget: {wget_command_line}")
    process = subprocess.run(wget_command_line)
    if process.returncode != 0:
        raise RuntimeError(f"Could not execute wget {wget_command_line}: {process.stderr}")

    # Decompress the downloaded file if needed.
    uncompressed_filename = None
    if decompress:
        if dl_file_name.endswith('.gz'):
            uncompressed_filename = dl_file_name[:-3]
            process = subprocess.run(['gunzip', dl_file_name])
            if process.returncode != 0:
                raise RuntimeError(f"Could not execute gunzip ['gunzip', {dl_file_name}]: {process.stderr}")
        else:
            raise RuntimeError(f"Don't know how to decompress {in_file_name}")

        if os.path.isfile(uncompressed_filename):
            file_size = os.path.getsize(uncompressed_filename)
            logger.info(f"Downloaded {uncompressed_filename} from {url}, file size {file_size} bytes.")
        else:
            raise RuntimeError(f'Expected uncompressed file {uncompressed_filename} does not exist.')
    else:
        if os.path.isfile(dl_file_name):
            file_size = os.path.getsize(dl_file_name)
            logger.info(f"Downloaded {dl_file_name} from {url}, file size {file_size} bytes.")
        elif os.path.isdir(dl_file_name):
            # Count the number of files in directory dl_file_name
            dir_size = sum(os.path.getsize(os.path.join(dl_file_name, f)) for f in os.listdir(dl_file_name) if os.path.isfile(os.path.join(dl_file_name, f)))
            logger.info(f"Downloaded {dir_size} files from {url} to {dl_file_name}.")
        else:
            raise RuntimeError(f'Unknown file type {dl_file_name}')


def sort_identifiers_with_boosted_prefixes(identifiers, prefixes):
    """
    Given a list of identifiers (with `identifier` and `label` keys), sort them using
    the following rules:
    - Any identifier that has a prefix in prefixes is sorted based on its order in prefixes.
    - Any identifier that does not have a prefix in prefixes is left in place.

    :param identifiers: A list of identifiers to sort. This is a list of dictionaries
        containing `identifier` and `label` keys, and possible others that we ignore.
    :param prefixes: A list of prefixes, in the order in which they should be boosted.
        We assume that CURIEs match these prefixes if they are in the form `{prefix}:...`.
    :return: The list of identifiers sorted as described above.
    """

    # Thanks to JetBrains AI.
    return sorted(
        identifiers,
        key=lambda identifier: prefixes.index(identifier['identifier'].split(':', 1)[0]) if identifier['identifier'].split(':', 1)[0] in prefixes else len(prefixes)
    )


def get_numerical_curie_suffix(curie):
    """
    If a CURIE has a numerical suffix, return it as an integer. Otherwise return None.
    :param curie: A CURIE.
    :return: An integer if the CURIE suffix is castable to int, otherwise None.
    """
    curie_parts = curie.split(':', 1)
    if len(curie_parts) > 0:
        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
        try:
            return int(curie_parts[1])
        except ValueError:
            pass
    return None


def write_compendium(metadata_yamls, synonym_list, ofname, node_type, labels=None, extra_prefixes=None, icrdf_filename=None, properties_jsonl_gz_files=None):
    """
    :param metadata_yaml: The YAML files containing the metadata for this compendium.
    :param synonym_list:
    :param ofname: Output filename. A file with this filename will be created in both the `compendia` and `synonyms` output directories.
    :param node_type:
    :param labels: A map of identifiers
        Not needed if each identifier will have a label in the correct directory (i.e. downloads/PMID/labels for PMID:xxx).
    :param extra_prefixes: We default to only allowing the prefixes allowed for a particular type in Biolink.
        If you want to allow additional prefixes, list them here.
    :param icrdf_filename: (REQUIRED) The file to read the information content from (icRDF.tsv). Although this is a
        named parameter to make it easier to specify this when calling write_compendium(), it is REQUIRED, and
        write_compendium() will throw a RuntimeError if it is not specified. This is to ensure that it has been
        properly specified as a prerequisite in a Snakemake file, so that write_compendium() is not run until after
        icRDF.tsv has been generated.
    :param properties_files: (OPTIONAL) A list of SQLite3 files containing properties to be added to the output.
    :return:
    """
    logger.info(f"Starting write_compendium({metadata_yamls}, {len(synonym_list)} slists, {ofname}, {node_type}, {len(labels)} labels, {extra_prefixes}, {icrdf_filename}, {properties_jsonl_gz_files}): {get_memory_usage_summary()}")

    if extra_prefixes is None:
        extra_prefixes = []
    if labels is None:
        labels = {}
    config = get_config()
    cdir = config['output_directory']
    biolink_version = config['biolink_version']

    node_factory = NodeFactory(make_local_name(''),biolink_version)
    logger.info(f"NodeFactory ready: {node_factory} with {get_memory_usage_summary()}")
    synonym_factory = SynonymFactory(make_local_name(''))
    logger.info(f"SynonymFactory ready: {synonym_factory} with {get_memory_usage_summary()}")

    # Load the preferred_name_boost_prefixes -- this tells us which prefixes to boost when
    # coming up with a preferred label for a particular Biolink class.
    preferred_name_boost_prefixes = config['preferred_name_boost_prefixes']

    # Create an InformationContentFactory based on the specified icRDF.tsv file. Default to the one in the download
    # directory.
    if not icrdf_filename:
        raise RuntimeError("No icrdf_filename parameter provided to write_compendium() -- this is required!")
    ic_factory = InformationContentFactory(icrdf_filename)
    logger.info(f"InformationContentFactory ready: {ic_factory} with {get_memory_usage_summary()}")

    description_factory = DescriptionFactory(make_local_name(''))
    logger.info(f"DescriptionFactory ready: {description_factory} with {get_memory_usage_summary()}")

    taxon_factory = TaxonFactory(make_local_name(''))
    logger.info(f"TaxonFactory ready: {taxon_factory} with {get_memory_usage_summary()}")

    node_test = node_factory.create_node(input_identifiers=[],node_type=node_type,labels={},extra_prefixes = extra_prefixes)
    logger.info(f"NodeFactory test complete: {node_test} with {get_memory_usage_summary()}")

    # Create compendia and synonyms directories, just in case they haven't been created yet.
    os.makedirs(os.path.join(cdir, 'compendia'), exist_ok=True)
    os.makedirs(os.path.join(cdir, 'synonyms'), exist_ok=True)

    # Load all the properties.
    property_list = PropertyList()
    if properties_jsonl_gz_files:
        for properties_jsonl_gz_file in properties_jsonl_gz_files:
            logger.info(f"Loading properties from {properties_jsonl_gz_file}...")
            count_loaded = property_list.add_properties_jsonl_gz(properties_jsonl_gz_file)
            logger.info(f"Loaded {count_loaded} unique properties from {properties_jsonl_gz_file}")
        logger.info(f"All {len(properties_jsonl_gz_files)} property files loaded ({property_list.count_unique()} total unique properties): {get_memory_usage_summary()}")
    else:
        logger.info("No property files provided or loaded.")

    property_source_count = defaultdict(int)

    # Counts.
    count_cliques = 0
    count_eq_ids = 0
    count_synonyms = 0

    # Write compendium and synonym files.
    with jsonlines.open(os.path.join(cdir,'compendia',ofname),'w') as outf, jsonlines.open(os.path.join(cdir,'synonyms',ofname),'w') as sfile:
        # Calculate an estimated time to completion.
        start_time = time.time_ns()
        count_slist = 0
        total_slist = len(synonym_list)

        for slist in synonym_list:
            # Before we get started, let's estimate where we're at.
            count_slist += 1
            if (count_slist == 1) or (count_slist % WRITE_COMPENDIUM_LOG_EVERY_X_CLIQUES == 0):
                time_elapsed_seconds = (time.time_ns() - start_time) / 1E9
                if time_elapsed_seconds < 0.001:
                    # We don't want to divide by zero.
                    time_elapsed_seconds = 0.001
                remaining_slist = total_slist - count_slist
                # count_slist --> time_elapsed_seconds
                # remaining_slist --> remaining_slist/count_slit*time_elapsed_seconds
                logger.info(f"Generating compendia and synonyms for {ofname} currently at {count_slist:,} out of {total_slist:,} ({count_slist/total_slist*100:.2f}%) in {format_timespan(time_elapsed_seconds)}: {get_memory_usage_summary()}")
                logger.info(f" - Current rate: {count_slist/time_elapsed_seconds:.2f} cliques/second or {time_elapsed_seconds/count_slist:.6f} seconds/clique.")

                time_remaining_seconds = (time_elapsed_seconds / count_slist * remaining_slist)
                logger.info(f" - Estimated time remaining: {format_timespan(time_remaining_seconds)}")

            node = node_factory.create_node(input_identifiers=slist, node_type=node_type,labels = labels, extra_prefixes = extra_prefixes)
            if node is None:
                # This usually happens because every CURIE in the node is not in the id_prefixes list for that node_type.
                # Something to fix at some point, but we don't want to break the pipeline for this, so
                # we emit a warning and skip this clique.
                logger.warning(f"Could not create node for ({slist}, {node_type}, {labels}, {extra_prefixes}): returned None.")
                continue
            else:
                count_cliques += 1
                count_eq_ids += len(slist)

                nw = {"type": node['type']}
                ic = ic_factory.get_ic(node)
                nw['ic'] = ic

                # Determine types.
                types = node_factory.get_ancestors(node["type"])

                # Generate a preferred label for this clique.
                #
                # To pick a preferred label for this clique, we need to do three things:
                # 1. We sort all labels in the preferred-name order. By default, this should be
                #    the preferred CURIE order, but if this clique is in one of the Biolink classes in
                #    preferred_name_boost_prefixes, we boost those prefixes in that order to the top of the list.
                # 2. We filter out any suspicious labels.
                #    (If this simple filter doesn't work, and if prefixes are inconsistent, we can build upon the
                #    algorithm proposed by Jeff at
                #    https://github.com/NCATSTranslator/Feedback/issues/259#issuecomment-1605140850)
                # 3. We filter out any labels longer than config['demote_labels_longer_than'], but only if there is
                #    at least one label shorter than this limit.
                # 4. We choose the first label that isn't blank (that allows us to use our rule of smallest-prefix-first to find the broadest name for this concept). If no labels remain, we generate a warning.

                # Step 1.1. Sort labels in boosted prefix order if possible.
                possible_labels = []
                for typ in types:
                    if typ in preferred_name_boost_prefixes:
                        # This is the most specific matching type, so we use this and then break.
                        possible_labels = list(map(lambda identifier: identifier.get('label', ''),
                                              sort_identifiers_with_boosted_prefixes(
                                                  node["identifiers"],
                                                  preferred_name_boost_prefixes[typ]
                                              )))

                        # Add in all the other labels -- we'd still like to consider them, but at a lower priority.
                        for id in node["identifiers"]:
                            label = id.get('label', '')
                            if label not in possible_labels:
                                possible_labels.append(label)

                        # Since this is the most specific matching type, we shouldn't do other (presumably higher-level)
                        # categories: so let's break here.
                        break

                # Step 1.2. If we didn't have a preferred_name_boost_prefixes, just use the identifiers in their
                # Biolink prefix order.
                if not possible_labels:
                    possible_labels = map(lambda identifier: identifier.get('label', ''), node["identifiers"])

                # Step 2. Filter out any suspicious labels.
                filtered_possible_labels = [l for l in possible_labels if l ]   # Ignore blank or empty names.

                # Step 3. Filter out labels longer than config['demote_labels_longer_than'], but only if there is at
                # least one label shorter than this limit.
                labels_shorter_than_limit = [l for l in filtered_possible_labels if l and len(l) <= config['demote_labels_longer_than']]
                if labels_shorter_than_limit:
                    filtered_possible_labels = labels_shorter_than_limit

                # Step 4. Pick the first label if it isn't blank.
                if filtered_possible_labels:
                    preferred_name = filtered_possible_labels[0]
                else:
                    preferred_name = ''

                # At this point, we insert any HAS_ADDITIONAL_ID IDs we have.
                # The logic we use is: we insert all additional IDs for a CURIE *AFTER* that CURIE, in a random order, as long
                # as the additional CURIE is not already in the list of CURIEs.
                #
                # We will attempt to retrieve a label or description for this ID as well.
                current_curies = set()
                identifier_list = []
                curie_labels = dict()
                for nid in node['identifiers']:
                    iid = nid['identifier']

                    # Prevent duplicates (might happen if e.g. we have an additional CURIE that duplicates an existing one later in the list).
                    if iid in current_curies:
                        continue

                    identifier_list.append(iid)
                    current_curies.add(iid)

                    if 'label' in nid:
                        curie_labels[iid] = nid['label']

                    # Are there any additional CURIEs for this CURIE?
                    props = property_list.get_all(iid, HAS_ALTERNATIVE_ID)
                    if props:
                        # Get just the additional CURIEs.
                        additional_curies = [prop.value for prop in props]

                        # ac_labelled will be a list that consists of either LabeledID (if the CURIE could be labeled)
                        # or str objects (consisting of an unlabeled CURIE).
                        ac_labelled = node_factory.apply_labels(input_identifiers=additional_curies, labels=labels)

                        for prop, label in zip(props, ac_labelled):
                            additional_curie = Text.get_curie(label)
                            if ':' not in additional_curie:
                                raise ValueError(f"Additional ID '{additional_curie}' for '{iid}' is not a valid CURIE: {prop}, {label} (from {ac_labelled})")
                            if additional_curie not in current_curies:
                                identifier_list.append(additional_curie)
                                current_curies.add(additional_curie)

                                # Track the property sources we used.
                                property_source_count[prop.source] += 1

                                if isinstance(label, LabeledID) and label.label:
                                    curie_labels[additional_curie] = label.label

                # Add description and taxon information and construct the final nw object.
                logger.debug(f"Getting descriptions and taxa for {len(identifier_list)} identifiers: {identifier_list}")
                descs = description_factory.get_descriptions(identifier_list)
                taxa = taxon_factory.get_taxa(identifier_list)

                # Construct the written-out identifier objects.
                nw['identifiers'] = []
                for iid in identifier_list:
                    id_info = {'i': iid}

                    if iid in curie_labels:
                        id_info['l'] = curie_labels[iid]
                    else:
                        id_info['l'] = ''

                    if id_info['i'] in descs:
                        # Sort descriptions from the shortest to the longest.
                        id_info['d'] = list(sorted(descs[id_info['i']], key=lambda x: len(x)))
                    else:
                        id_info['d'] = []

                    if id_info['i'] in taxa:
                        # Sort taxa by CURIE suffix.
                        id_info['t'] = list(sorted(taxa[id_info['i']], key=get_numerical_curie_suffix))
                    else:
                        id_info['t'] = []

                    nw['identifiers'].append(id_info)

                # Write out the preferred name, if we have one.
                nw['preferred_name'] = preferred_name

                # Collect taxon names for this node.
                nw['taxa'] = list(sorted(set().union(*taxa.values()), key=get_numerical_curie_suffix))

                outf.write( nw )

                # get_synonyms() returns tuples in the form ('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym', 'Caudal articular process of eighteenth thoracic vertebra')
                # But we're only interested in the synonyms themselves, so we can skip the relationship for now.
                curie = node["identifiers"][0]["identifier"]

                # get_synonyms() returns a list of tuples, where each tuple is a relation and a synonym.
                # So we extract just the synonyms here, ditching the relations (result[0]), then unique-ify the
                # synonyms.
                synonyms = [result[1] for result in synonym_factory.get_synonyms(identifier_list)]
                synonyms_list = sorted(set(synonyms), key=lambda x: len(x))

                try:
                    document = {"curie": curie,
                                "names": synonyms_list,
                                "types": [t[8:] for t in types]} # remove biolink:

                    count_synonyms += len(synonyms_list)

                    # Write out the preferred name.
                    if preferred_name:
                        document["preferred_name"] = preferred_name
                    else:
                        logger.debug(
                            f"No preferred name for {nw}, probably because all names were filtered out, skipping."
                        )
                        continue

                    # We previously used the shortest length of a name as a proxy for how good a match it is, i.e. given
                    # two concepts that both have the word "acetaminophen" in them, we assume that the shorter one is the
                    # more interesting one for users. I'm not sure if there's a better way to do that -- for instance,
                    # could we consider the information content values? -- but in the interests of getting something
                    # working quickly, this code restores that previous method.

                    # Since synonyms_list is sorted, we can use the length of the first term as the synonym.
                    if len(synonyms_list) == 0:
                        logger.debug(f"Synonym list for {nw} is empty: no valid name. Skipping.")
                        continue
                    else:
                        document["shortest_name_length"] = len(synonyms_list[0])

                    # Cliques with more identifiers might be better than cliques with smaller identifiers.
                    # So let's try to incorporate that here.
                    # Note that this includes all the alternative IDs.
                    document["clique_identifier_count"] = len(nw["identifiers"])

                    # We want to see if we can use the CURIE suffix to sort concepts with similar identifiers.
                    # We want to sort this numerically, so we only do this if the CURIE suffix is numerical.
                    curie_parts = curie.split(':', 1)
                    if len(curie_parts) > 0:
                        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
                        try:
                            document["curie_suffix"] = int(curie_parts[1])
                        except ValueError:
                            pass

                    # Collect taxon names for this node.
                    document['taxa'] = list(sorted(set().union(*taxa.values()), key=get_numerical_curie_suffix))

                    sfile.write( document )
                except Exception as ex:
                    print(f"Exception thrown while write_compendium() was generating {ofname}: {ex}")
                    print(nw["type"])
                    print(node_factory.get_ancestors(nw["type"]))
                    traceback.print_exc()
                    raise ex

    # Write out the metadata.yaml file combining information from all the metadata.yaml files.
    write_combined_metadata(
        os.path.join(cdir, 'metadata', ofname + '.yaml'),
        typ='compendium',
        name=ofname,
        counts={
            'cliques': count_cliques,
            'eq_ids': count_eq_ids,
            'synonyms': count_synonyms,
            'property_sources': property_source_count,
        },
        combined_from_filenames=metadata_yamls,
    )

    # Close all the factories.
    taxon_factory.close()


def glom(conc_set, newgroups, unique_prefixes=['INCHIKEY'],pref='HP',close={}):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivalent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    n = 0
    bad = 0
    shit_prefixes=set(['KEGG','PUBCHEM'])
    test_id = 'xUBERON:0002262'
    debugit = False
    excised = set()
    for xgroup in newgroups:
        if isinstance(xgroup,frozenset):
            group = set(xgroup)
        else:
            group = xgroup
        #As of now, xgroup should never be more than two things
        if len(xgroup) > 2:
            print(xgroup)
            print('nope nope nope')
            raise ValueError
        n+=1
        if debugit:
            print("new group",group)
        if test_id in group:
            print('higroup',group)
        #Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        existing_sets_w_x = [ (conc_set[x],x) for x in group if x in conc_set ]
        #All of these sets are now going to be combined through the equivalence of our new set.
        existing_sets = [ es[0] for es in existing_sets_w_x ]
        x =  [ es[1] for es in existing_sets_w_x ]
        newset=set().union(*existing_sets)
        if debugit:
            print("merges:",existing_sets)
        #put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        if test_id in newset:
            print('hiset',newset)
            print('input_set',group)
            print('esets')
            for eset in existing_sets:
                print(' ',eset,group.intersection(eset))
        for check_element in newset:
            prefix = check_element.split(':')[0]
            if prefix in shit_prefixes:
                print(prefix)
                print(check_element)
                raise Exception('garbage')
        if debugit:
            print("final set",newset)
        #make sure we didn't combine anything we want to keep separate
        setok = True
        if test_id in group:
            print('setok?',setok)
        for up in unique_prefixes:
            if test_id in group:
                print('up?',up)
            idents = [e if type(e)==str else e.identifier for e in newset]
            if len(set([e for e in idents if (e.split(':')[0] ==up)])) > 1:
                bad += 1
                setok = False
                wrote = set()
                for s in existing_sets:
                    fs = frozenset(s)
                    wrote.add(fs)
                for gel in group:
                    if Text.get_prefix_or_none(gel) == pref:
                        killer = gel
                #for preset in wrote:
                #    print(f'{killer}\t{set(group).intersection(preset)}\t{preset}\n')
                #print('------------')
        NPC = sum(1 for s in newset if s.startswith("PUBCHEM.COMPOUND:"))
        if (("PUBCHEM.COMPOUND:3100" in newset) and (NPC > 3)):
            if debugit:
                l = sorted(list(newset))
                print("bad")
                for li in l:
                    print(li)
                exit()
        if (not setok):
            #Our new group created a new set that merged stuff we didn't want to merge.
            #Previously we did a lot of fooling around at this point.  But now we're just going to say, I have a
            # pairwise concordance.  That can at most link two groups.  just don't link them. In other words,
            # we are simply ignoring this concordance.
            continue
            #Let's figure out the culprit(s) and excise them
            #counts = defaultdict(int)
            #for x in group:
            #    counts[x] += 1
            ##THe way existing sets was created, means that the same set can be in there twice, and we don't want to
            # count things that way
            #unique_existing_sets = []
            #for ex in existing_sets:
            #    u = True
            #    for q in unique_existing_sets:
            #        if ex == q:
            #            u = False
            #    if u:
            #        unique_existing_sets.append(ex)
            #for es in unique_existing_sets:
            #    for y in es:
            #        counts[y] += 1
            #bads = [ x for x,y in counts.items() if y > 1 ]
            #now we know which identifiers are causing trouble.
            #We don't want to completely throw them out, but we can't allow them to gum things up.
            #So, we need to first remove them from all the sets, then we need to put them in their own set
            #It might be good to track this somehow?
            #excised.update(bads)
            #for b in bads:
            #    if b in group:
            #        group.remove(b)
            #    for exset in existing_sets:
            #        if b in exset:
            #            exset.remove(b)
            #    conc_set[b] = set([b])
            #for x in group:
            #    conc_set[x] = group
            #continue
        #Now check the 'close' dictionary to see if we've accidentally gotten to a close match becoming an exact match
        setok = True
        for cpref, closedict in close.items():
            idents = set([e if type(e) == str else e.identifier for e in newset])
            prefidents = [e for e in idents if e.startswith(cpref)]
            for pident in prefidents:
                for cd in closedict[pident]:
                    if cd in newset:
                        setok = False
            if len(prefidents) == 0:
                continue
        if not setok:
            continue
        #Now make all the elements point to this new set:
        for element in newset:
            conc_set[element] = newset

def get_prefixes(idlist):
    """ Return a dictionary of identifiers from idlist with their prefix as the key.

    :param idlist: A list of identifiers. Should NOT contain any LabeledIDs.
    """
    prefs = defaultdict(list)
    for ident in idlist:
        if isinstance(ident,LabeledID):
            print('nonono')
            exit()
            prefs.add(Text.get_prefix_or_none(ident.identifier))
        else:
            prefs[Text.get_prefix_or_none(ident)].append(ident)
    return prefs


def clean_sets(result_dict):
    """The keys for this are unique and unmergable: Don't merge GO!
    But there are values that are showing up in multiple GOs (could be
    MetaCycs or RHEAs or Reactomes).  It's just how GO is mapping.  Now,
    the right answer here is probably to kboom this whole mess.  But
    for prototype, we're just going to filter out garbage merge values).
    Note that this isn't limited to GO. Even MONDO include some #exactMatch
    to the same MESH from two different MONDO ids"""
    cmap = defaultdict(int)
    for v in result_dict.values():
        for x in v:
            cmap[x] += 1
    bad_values = [ k for k,v in cmap.items() if v > 1 ]
    for bv in bad_values:
        if bv.startswith('Meta'):
            print(bv)
    for k,v in result_dict.items():
        newv = [ vi for vi in v if vi not in bad_values ]
        result_dict[k] = newv
    return result_dict

def filter_out_non_unique_ids(old_list):
    """
    filters out elements that exist accross rows
    eg input [{'z', 'x', 'y'}, {'z', 'n', 'm'}]
    output [{'x', 'y'}, {'m', 'n'}]
    """
    idcounts = defaultdict(int)
#    mondomap = defaultdict(list)
    for terms in old_list:
        for term in terms:
            idcounts[term] += 1
#            mondomap[term].append(terms)
    bad_ids = set( [k for k,v in idcounts.items() if v > 1])
#    for b in bad_ids:
#        mm = mondomap[b]
#        mondos = []
#        for ms in mm:
#            for x in ms:
#                if Text.get_curie(x) == 'MONDO':
#                    mondos.append(x)
#        print(b, mondos)
    new_list = list(map(
        lambda term_list : \
        set(
            filter(
                lambda term: term not in bad_ids,
                term_list
            )), old_list))
    return new_list


def read_identifier_file(infile):
    """Identifier files are mostly just lists of identifiers that constitutes all the id's from a given
    source that should be included in a normalization run.   There is an optional second column that contains
    a hint to the normalizer about the proper biolink type for this entity."""
    types = {}
    identifiers = list()
    with open(infile,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            identifiers.append((x[0],))
            if len(x) > 1:
                types[x[0]] = x[1]
    return identifiers,types


def remove_overused_xrefs(pairlist: List[Tuple], bothways:bool = False):
    """Given a list of tuples (id1, id2) meaning id1-[xref]->id2, remove any id2 that are associated with more
    than one id1.  The idea is that if e.g. id1 is made up of UBERONS and 2 of those have an xref to say a UMLS
    then it doesn't mean that all of those should be identified.  We don't really know what it means, so remove it."""
    xref_counts_v = defaultdict(int)
    xref_counts_k = defaultdict(int)
    for k, v in pairlist:
        xref_counts_v[v] += 1
        xref_counts_k[k] += 1
    improved_pairs = []
    for k,v in pairlist:
        if xref_counts_v[v] < 2:
            if bothways:
                if xref_counts_k[k] < 2:
                    improved_pairs.append( (k,v) )
            else:
                improved_pairs.append((k, v))
    return improved_pairs

def norm(x,op):
    #Get curie returns the uppercase
    pref = Text.get_prefix_or_none(x)
    if pref in op:
        return Text.recurie(x,op[pref])
    return x

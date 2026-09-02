import gzip
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import traceback
import urllib
from collections import defaultdict
from datetime import datetime, timedelta
from enum import Enum
from ftplib import FTP
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import jsonlines
import requests
from humanfriendly import format_timespan

from src.LabeledID import LabeledID
from src.metadata.provenance import write_combined_metadata
from src.node import DescriptionFactory, InformationContentFactory, NodeFactory, SynonymFactory, TaxonFactory
from src.properties import HAS_ALTERNATIVE_ID, PropertyList
from src.synonyms.filter import get_synonym_filter
from src.util import Text, ensure_parent_dir, get_config, get_logger, get_memory_usage_summary

# Configuration items
WRITE_COMPENDIUM_LOG_EVERY_X_CLIQUES = 1_000_000
MAX_DOWNLOAD_ERROR = 1


def get_user_agent() -> str:
    """Return the User-Agent string for outbound HTTP requests, including the build branch."""
    config = get_config()
    branch = config["build"]["branch"]
    github_url = config["babel"]["github_url"]
    return f"TranslatorBabel/{branch} ({github_url})"


# Set up a logger.
logger = get_logger(__name__)

# Matches an RDF language-tagged literal as returned by pyoxigraph as a raw string, e.g. "value"@en
_RDF_LANG_LITERAL_RE = re.compile(r'^"(.*)"@\w+$')


class TypedClique(NamedTuple):
    """A clique that carries its own Biolink node type.

    Used as an element of the ``synonym_list`` passed to :func:`write_compendium` when the
    cliques in a single compendium run do not all share the same Biolink type.  Passing a
    heterogeneous list of ``TypedClique`` objects (with ``node_type=None`` in
    ``write_compendium``) lets each clique declare its own type independently, which is how
    the leftover-UMLS compendium handles entities that span many Biolink classes.

    :param node_type: The ``biolink:``-prefixed class URI for this clique
        (e.g. ``"biolink:Disease"``).  Use the named constants in ``src/categories.py``
        rather than raw strings.
    :param identifiers: The list of CURIEs that belong to this clique.
    :param labels: Optional ``{curie: label}`` for this clique only, consulted before the ``labels``
        mapping passed to :func:`write_compendium`. Lets a caller stream tens of millions of cliques
        (Publication: one title per PMID) without first building one global labels dict.
    """

    node_type: str
    identifiers: list[str]
    labels: dict[str, str] | None = None


def parse_rdf_literal(literal: str) -> str:
    """Strip quoting from a pyoxigraph SPARQL literal string.

    pyoxigraph returns plain literals as '"value"' and language-tagged literals as '"value"@en'.
    Both forms are reduced to just the inner value string.  Typed literals of the form
    '"value"^^<xsd:type>' are not yet handled and will be returned incorrectly; this is
    acceptable because none of the current RDF sources use typed literals in label/synonym
    positions.  See https://github.com/NCATSTranslator/Babel/issues/760
    """
    if not literal.startswith('"'):
        return literal
    m = _RDF_LANG_LITERAL_RE.match(literal)
    if m:
        return m.group(1)
    return literal[1:-1]


def reduce_to_most_specific_tree_codes(codes, code_to_tree):
    """Reduce a set of hierarchy codes to only the most specific ones.

    Given an iterable of ``codes`` and a ``code_to_tree`` map from each code to its
    dot-delimited tree number (e.g. a UMLS TUI ``"T116"`` -> ``"A1.4.1.2.1.7"``, or a MeSH
    tree number like ``"C04.557"``), return the subset of codes whose tree number is NOT a
    proper ancestor of another code's tree number in the set.

    A tree number is a proper ancestor of another when it is a strict dot-*component* prefix
    of it: ``"A1.2"`` is an ancestor of ``"A1.2.3"`` but NOT of ``"A1.20"`` (comparison is on
    ``tree.split(".")`` component lists, not raw string prefixes). Unrelated codes (siblings or
    codes in different subtrees) all survive. A code with no entry in ``code_to_tree`` -- or an
    empty tree number -- has no ancestor relationship to anything and is always kept.

    This is vocabulary-agnostic: it only needs a code -> tree-number mapping, so it works for
    UMLS semantic-type tree numbers (MRSTY STN) and MeSH tree numbers alike.

    :param codes: An iterable of codes to reduce.
    :param code_to_tree: A mapping from code to its dot-delimited tree number string.
    :return: A set of the most-specific codes.
    """
    codes = set(codes)
    tree_components = {code: code_to_tree.get(code, "").split(".") if code_to_tree.get(code) else [] for code in codes}
    survivors = set()
    for code in codes:
        components = tree_components[code]
        # Keep this code unless its tree number is a proper ancestor of some other code's.
        is_ancestor_of_other = any(
            other != code
            and components
            and len(tree_components[other]) > len(components)
            and tree_components[other][: len(components)] == components
            for other in codes
        )
        if not is_ancestor_of_other:
            survivors.add(code)
    return survivors


def make_local_name(fname, subpath=None):
    config = get_config()
    if subpath is None:
        return os.path.join(config["download_directory"], fname)
    odir = os.path.join(config["download_directory"], subpath)
    os.makedirs(odir, exist_ok=True)
    return os.path.join(odir, fname)


class StateDB:
    def __init__(self, fname):
        self.dbname = make_local_name(fname)
        new = True
        if os.path.exists(self.dbname):
            new = False
        self.connection = sqlite3.connect(self.dbname)
        if new:
            self.initialize_db()

    def initialize_db(self):
        curr = self.connection.cursor()
        curr.execute("CREATE TABLE cache (key text, value text)")
        self.connection.commit()

    def get(self, key):
        curr = self.connection.cursor()
        curr.execute("SELECT value FROM cache WHERE key=?", (key,))
        result = curr.fetchone()
        if result is not None:
            return result[0]
        return None

    def put(self, key, value):
        curr = self.connection.cursor()
        curr.execute("INSERT INTO cache VALUES (?,?)", (key, value))
        self.connection.commit()


# The signature here should be modified to be like pull via urlllib
def pull_via_ftp(ftpsite, ftpdir, ftpfile, decompress_data=False, outfilename=None):
    """Retrieve data via ftp.
    Setting decompress=True will ungzip the data
    If outfilename is None (default) then the data will be returned.
    Otherwise it will be written to the downloads directory."""
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    print("   getting data")
    config = get_config()
    if outfilename is None:
        with BytesIO() as data:
            ftp.retrbinary(f"RETR {ftpfile}", data.write)
            ftp.quit()
            binary = data.getvalue()
            if decompress_data:
                return gzip.decompress(binary).decode()
            else:
                return binary.decode()
    ofilename = os.path.join(config["download_directory"], outfilename)
    odir = os.path.abspath(os.path.dirname(ofilename))
    if not os.path.exists(odir):
        os.makedirs(odir)
    print(f"  writing data to {ofilename}")
    print(f"{ftpsite}/{ftpdir}/{ftpfile}")
    if not decompress_data:
        with open(ofilename, "wb") as ofile:
            ftp.retrbinary(f"RETR {ftpfile}", ofile.write)
            ftp.quit()
    else:
        # Stream the compressed file to a temp file rather than buffering it all in
        # memory with BytesIO+gzip.decompress — the old approach needed ~2× the
        # uncompressed size in RAM (e.g. ~35+ GB for ChEMBL's 17 GB TTL).
        tmp_path = None  # set before try so finally can always check it
        try:
            with tempfile.NamedTemporaryFile(delete=False, dir=odir, suffix=".gz") as tmp:
                tmp_path = tmp.name
                ftp.retrbinary(f"RETR {ftpfile}", tmp.write)
                ftp.quit()
            with gzip.open(tmp_path, "rt") as gz_in, open(ofilename, "w") as ofile:
                shutil.copyfileobj(gz_in, ofile)
        finally:
            if tmp_path is not None and os.path.exists(tmp_path):
                os.unlink(tmp_path)
    return ofilename


def dump_dict(outdict, outfname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__), config["download_directory"], outfname)
    with open(oname, "w") as outf:
        for k, v in outdict.items():
            outf.write(f"{k}\t{v}\n")


def dump_dicts(dicts, fname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__), config["download_directory"], fname)
    with open(oname, "w") as outf:
        for k in dicts:
            outf.write(f"{k}\t{dicts[k]}\n")


def dump_sets(sets, fname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__), config["download_directory"], fname)
    print("dumping: ", oname)
    with open(oname, "w") as outf:
        for s in sets:
            outf.write(f"{s}\n")


class ThrottledRequester:
    """Make sure that the time from the last call to the current call is greater than or equal to
    a configurable delta.   Wait before making request to ensure this. Used to make sure eutils
    doesn't get angry.  Returns the json, as well as a flag whether this call waited or not."""

    def __init__(self, delta_ms):
        self.last_time = None
        self.delta = timedelta(milliseconds=delta_ms)

    def get(self, url):
        now = datetime.now()
        throttled = False
        if self.last_time is not None:
            cdelta = now - self.last_time
            if cdelta < self.delta:
                waittime = self.delta - cdelta
                time.sleep(waittime.total_seconds())
                throttled = True
        self.last_time = datetime.now()
        response = requests.get(url)
        return response, throttled

    def get_json(self, url):
        """Add retries to the throttling, return json"""
        ntries = 0
        maxtries = 100
        while ntries < maxtries:
            try:
                response, _ = self.get(url)
                result = response.json()
                return result
            except Exception:
                ntries += 1


def raise_if_cloudflare_challenge(download_url: str, local_file_name: str, error: urllib.error.URLError):
    """Fail fast (no retry) if a URLError is actually a Cloudflare bot challenge.

    Cloudflare marks a challenge-page response (a 403 with an interactive JS/Turnstile
    challenge instead of the requested content) with a `cf-mitigated: challenge` header --
    see https://developers.cloudflare.com/cloudflare-challenges/challenge-types/challenge-pages/detect-response/.
    Retrying or changing the User-Agent won't help: this is served identically to a real
    browser. Raise immediately with instructions for a human to download the file manually.

    Only an HTTPError carries response headers; a plain URLError (DNS failure, connection
    refused) has no `.headers` at all, so we getattr() rather than narrowing the caller's
    except clause to HTTPError and duplicating its retry body.
    """
    headers = getattr(error, "headers", None)
    if headers is not None and headers.get("cf-mitigated") == "challenge":
        raise RuntimeError(
            f"{download_url} is behind a Cloudflare bot challenge (cf-mitigated: challenge) and cannot be "
            "downloaded automatically. Please download the file manually in a browser and place it at "
            f"{local_file_name}, then re-run this rule."
        )


def pull_via_urllib(url: str, in_file_name: str, decompress=True, subpath=None, verify_gzip=False):
    """
    Download a file via the given URL, optionally decompress it, and save it
    to the specified local path. Handles HTTP redirects gracefully.

    :param url: The base URL of the remote server (e.g., "http://example.com/").
        It is combined with the provided filename to determine the full file path.
    :type url: str
    :param in_file_name: The name of the file to download, specified as the filename
        on the remote server.
    :type in_file_name: str
    :param decompress: Whether to decompress the downloaded file if it is gzipped.
        Defaults to True.
    :type decompress: bool, optional
    :param subpath: An optional subpath under the main download directory to save the file.
        If None, the file is saved directly in the download directory.
    :type subpath: str, optional
    :param verify_gzip: If downloading a Gzip file that isn't being decompressed, verify that the
        file is valid (by reading it). Has no effect if decompress=True.
    :type verify_gzip: bool, optional
    :return: The path to the downloaded (and optionally decompressed) file.
    :rtype: str
    """
    # Everything goes in downloads
    download_dir = get_config()["download_directory"]

    # get the (local) download file name, derived from the input file name
    if subpath is None:
        dl_file_name = os.path.join(download_dir, in_file_name)
    else:
        dl_file_name = os.path.join(download_dir, subpath, in_file_name)

    ensure_parent_dir(dl_file_name)

    # Add support for redirects
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())

    download_url = url + in_file_name
    logger.info(f"Downloading {download_url}")
    user_agent = get_user_agent()

    # create the compressed file
    download_verified = False
    download_attempt = 0
    while not download_verified:
        Path(dl_file_name).unlink(missing_ok=True)
        download_attempt += 1
        if download_attempt > MAX_DOWNLOAD_ERROR:
            raise RuntimeError(
                f"Could not download and verify {download_url}: more than {MAX_DOWNLOAD_ERROR} attempts."
            )
        logger.info(f"Downloading {dl_file_name} using urllib, attempt {download_attempt}...")

        # Open a fresh connection on each attempt so a truncated response doesn't
        # leave us reading from an exhausted handle on the next retry.
        try:
            req = urllib.request.Request(download_url, headers={"User-Agent": user_agent})
            with opener.open(req) as handle, open(dl_file_name, "wb") as compressed_file:
                # while there is data
                while True:
                    # read a block of data
                    data = handle.read(1024)

                    # if nothing read, abort
                    if len(data) == 0:
                        break

                    # write out the data to the output file
                    compressed_file.write(data)
        except urllib.error.URLError as e:
            raise_if_cloudflare_challenge(download_url, dl_file_name, e)
            logger.warning(f"Download attempt {download_attempt} of {download_url} failed with network/HTTP error: {e}")
            time.sleep(5 * download_attempt)
            continue

        if decompress:
            out_file_name = dl_file_name[:-3]

            # create the output text file
            with open(out_file_name, "w") as output_file:
                # open the compressed file
                with gzip.open(dl_file_name, "rt") as compressed_file:
                    for line in compressed_file:
                        # write the data to the output file
                        output_file.write(line)

            # remove the compressed file
            os.remove(dl_file_name)

            download_verified = True
        else:
            out_file_name = dl_file_name

            # Do we need to verify this gzip file?
            download_verified = True
            if verify_gzip:
                # Is it blank/very small? If so, we immediately fail verification.
                file_size = os.path.getsize(out_file_name)
                if file_size < 1024:
                    logger.warning(
                        f"Downloaded Gzip file {out_file_name} is too small ({file_size} bytes), skipping verification."
                    )
                    download_verified = False
                    continue

                # To verify a Gzip file, we need to read it entirely.
                try:
                    with gzip.open(out_file_name, "rb") as f:
                        for _ in iter(lambda: f.read(1024 * 1024), b""):
                            pass
                    download_verified = True
                except Exception as e:
                    logger.warning(f"Error while verifying downloaded Gzip file {out_file_name}: {e}")
                    download_verified = False

    # return the filename to the caller
    return out_file_name


# Recursion options for pull_via_wget().
# See https://www.gnu.org/software/wget/manual/html_node/Recursive-Download.html for wget's recursion options.
class WgetRecursionOptions(Enum):
    NO_RECURSION = 0  # Don't do any recursion
    RECURSE_SUBFOLDERS = 1  # Recurse into subfolders -- equivalent to `-np`
    RECURSE_DIRECTORY_ONLY = 2  # Recurse through a single directory only -- equivalent to `-np -l1`


def pull_via_wget(
    url_prefix: str,
    in_file_name: str,
    decompress=True,
    subpath: str = None,
    outpath: str = None,
    continue_incomplete: bool = True,
    timestamping=True,
    recurse: WgetRecursionOptions = WgetRecursionOptions.NO_RECURSION,
    retries: int = 1,
    connect_timeout: int = 60,
    read_timeout: int = 300,
    verify_gzip: bool = False,
):
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
    :param continue_incomplete: Should wget continue an incomplete download? Must be False in a recursive
        download, where resuming can corrupt a file whose content changed upstream; we raise if it isn't.
    :param timestamping: Should wget re-fetch a file only when the server's copy is newer, or differs in
        size, from ours? Must be True in a recursive download; we raise if it isn't.
    :param recurse: Do we want to download recursively? Should be from WgetRecursionOptions, such as WgetRecursionOptions.NO_RECURSION.
    :param retries: The number of retries to attempt.
    :param verify_gzip: If downloading a Gzip file that isn't being decompressed, verify that the
        file is valid (by reading it entirely). Has no effect if decompress=True.
    """

    # Prepare download URL and location
    download_dir = get_config()["download_directory"]
    url = url_prefix + in_file_name
    if subpath and outpath:
        raise RuntimeError("pull_via_wget() cannot be called with both subpath and outpath set.")
    elif outpath:
        dl_file_name = outpath
    elif subpath:
        dl_file_name = os.path.join(download_dir, subpath, in_file_name)
    else:
        dl_file_name = os.path.join(download_dir, in_file_name)

    ensure_parent_dir(dl_file_name)

    # A recursive download is always timestamped and never continued. --continue resumes by
    # appending to whatever local file it finds, which is only correct if that file is a truncated
    # prefix of the server's copy; recursing, we may instead meet a file whose *content* changed
    # upstream (or one carried over from a previous run), and appending the tail of the new file to
    # the old one silently produces a corrupt result. --timestamping is what re-fetches such a file,
    # in full — and it is also what stops wget saving a second copy as `file.1` when the file is
    # already there, which is the job --continue would otherwise be doing.
    #
    # (Non-recursive downloads pass -O, which makes wget ignore --timestamping entirely, so there
    # --continue is the only resume mechanism and is kept.)
    if recurse != WgetRecursionOptions.NO_RECURSION:
        if continue_incomplete:
            raise ValueError(
                f"pull_via_wget({url}) cannot combine continue_incomplete=True with recursion: resuming a "
                f"recursive download can corrupt a file whose content changed upstream. Pass "
                f"continue_incomplete=False."
            )
        if not timestamping:
            raise ValueError(
                f"pull_via_wget({url}) cannot disable timestamping in a recursive download: without "
                f"--timestamping, and with --continue unavailable, wget saves a second copy of every "
                f"file we already have as `file.1`."
            )

    # Prepare wget options.
    wget_command_line = [
        "wget",
        "--progress=bar:force:noscroll",
    ]
    if continue_incomplete:
        wget_command_line.append("--continue")
    # --timestamping is a no-op combined with -O (wget disables -N and warns); only pass it when
    # we're not writing to a fixed output file via -O.
    if timestamping and recurse != WgetRecursionOptions.NO_RECURSION:
        wget_command_line.append("--timestamping")
    if retries > 0:
        wget_command_line.append(f"--tries={retries}")
    if connect_timeout > 0:
        wget_command_line.append(f"--connect-timeout={connect_timeout}")
    if read_timeout > 0:
        wget_command_line.append(f"--read-timeout={read_timeout}")

    # Add URL and output file.
    wget_command_line.append(url)

    # Handle recursion options
    match recurse:
        case WgetRecursionOptions.NO_RECURSION:
            # Write to a single file, dl_file_name
            wget_command_line.extend(["-O", dl_file_name])
        case WgetRecursionOptions.RECURSE_SUBFOLDERS:
            # dl_file_name should be a directory name.
            wget_command_line.extend(
                ["--recursive", "--no-parent", "--no-directories", "--directory-prefix=" + dl_file_name]
            )
        case WgetRecursionOptions.RECURSE_DIRECTORY_ONLY:
            # dl_file_name should be a directory name.
            wget_command_line.extend(
                ["--recursive", "--no-parent", "--no-directories", "--level=1", "--directory-prefix=" + dl_file_name]
            )

    # Execute wget.
    logger.info(f"Downloading {dl_file_name} using wget: {wget_command_line}")
    process = subprocess.run(wget_command_line)
    if process.returncode != 0:
        raise RuntimeError(f"Could not execute wget {wget_command_line}: {process.stderr}")

    # Decompress the downloaded file if needed.
    uncompressed_filename = None
    if decompress:
        if dl_file_name.lower().endswith(".gz"):
            uncompressed_filename = dl_file_name[:-3]
            process = subprocess.run(["gunzip", dl_file_name])
            if process.returncode != 0:
                raise RuntimeError(f"Could not execute gunzip ['gunzip', {dl_file_name}]: {process.stderr}")
        else:
            raise RuntimeError(
                f"Don't know how to decompress {in_file_name}, which was downloaded as '{dl_file_name}'."
            )

        if os.path.isfile(uncompressed_filename):
            file_size = os.path.getsize(uncompressed_filename)
            logger.info(f"Downloaded {uncompressed_filename} from {url}, file size {file_size} bytes.")
        else:
            raise RuntimeError(f"Expected uncompressed file {uncompressed_filename} does not exist.")
    else:
        if os.path.isfile(dl_file_name):
            file_size = os.path.getsize(dl_file_name)
            logger.info(f"Downloaded {dl_file_name} from {url}, file size {file_size} bytes.")
            if verify_gzip:
                if file_size < 1024:
                    raise RuntimeError(
                        f"Downloaded Gzip file {dl_file_name} is too small ({file_size} bytes) to be valid."
                    )
                result = subprocess.run(["gzip", "-t", dl_file_name], capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(
                        f"Downloaded Gzip file {dl_file_name} failed verification: {result.stderr.strip()}"
                    )
                logger.info(f"Verified {dl_file_name} as a valid Gzip file.")
        elif os.path.isdir(dl_file_name):
            # Count the number of files in directory dl_file_name
            dir_size = sum(
                os.path.getsize(os.path.join(dl_file_name, f))
                for f in os.listdir(dl_file_name)
                if os.path.isfile(os.path.join(dl_file_name, f))
            )
            logger.info(f"Downloaded {dir_size} files from {url} to {dl_file_name}.")
        else:
            raise RuntimeError(f"Unknown file type {dl_file_name}")


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
        key=lambda identifier: (
            prefixes.index(identifier["identifier"].split(":", 1)[0])
            if identifier["identifier"].split(":", 1)[0] in prefixes
            else len(prefixes)
        ),
    )


def choose_preferred_name(node, types, preferred_name_boost_prefixes, demote_labels_longer_than):
    """Return the preferred name for a node, or "" if none is available."""
    # Walk the ancestor chain (most-specific type first) to find the first matching entry
    # for each config dict. Using the most specific type ensures a SmallMolecule, for example,
    # picks up boost/demotion rules defined on ChemicalEntity without overriding a more
    # specific rule that might exist on SmallMolecule itself.
    boost_prefixes = None
    length_limit = None
    for typ in types:
        if boost_prefixes is None and typ in preferred_name_boost_prefixes:
            boost_prefixes = preferred_name_boost_prefixes[typ]
        if length_limit is None and typ in demote_labels_longer_than:
            length_limit = demote_labels_longer_than[typ]
        if boost_prefixes is not None and length_limit is not None:
            break  # Both resolved — no need to scan further up the hierarchy.

    # Build the candidate label list in priority order.
    # If boost prefixes apply, promoted prefixes move to the front; all other identifiers
    # follow in their original Biolink prefix order.
    if boost_prefixes is not None:
        ordered_identifiers = sort_identifiers_with_boosted_prefixes(node["identifiers"], boost_prefixes)
    else:
        ordered_identifiers = node["identifiers"]

    # Drop blank/missing labels and apply the label filter as a safety net.
    # (Labels should already have been cleared by apply_labels(), but this catches
    # anything supplied via the explicit labels dict or through an unforeseen path.)
    synonym_filter = get_synonym_filter()
    filtered = []
    for id_entry in ordered_identifiers:
        label = id_entry.get("label", "")
        if not label:
            continue
        prefix = id_entry["identifier"].split(":", 1)[0]
        if synonym_filter.should_suppress(label, source=f"{prefix} (preferred name)", node_types=types):
            continue
        filtered.append(label)

    # Demote long labels: if any label fits within the limit, discard those that don't.
    # If *all* labels exceed the limit we keep them rather than returning empty.
    if length_limit is not None:
        shorter = [label for label in filtered if len(label) <= length_limit]
        if shorter:
            filtered = shorter

    return filtered[0] if filtered else ""


def get_numerical_curie_suffix(curie):
    """
    If a CURIE has a numerical suffix, return it as an integer. Otherwise return None.
    :param curie: A CURIE.
    :return: An integer if the CURIE suffix is castable to int, otherwise None.
    """
    curie_parts = curie.split(":", 1)
    if len(curie_parts) > 0:
        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
        try:
            return int(curie_parts[1])
        except ValueError:
            pass
    return None


def write_compendium(
    metadata_yamls,
    synonym_list,
    ofname,
    node_type,
    labels=None,
    extra_prefixes=None,
    icrdf_filename=None,
    properties_jsonl_gz_files=None,
):
    """
    :param metadata_yaml: The YAML files containing the metadata for this compendium.
    :param synonym_list:
    :param ofname: Output filename. A file with this filename will be created in both the `compendia` and `synonyms` output directories.
    :param node_type: The Biolink type of this compendium (including `biolink:` prefix). Set this to None
        only if every item in synonym_list is a TypedClique with its own node_type.
    :param labels: A map of identifiers
        Not needed if each identifier will have a label in the correct directory (i.e. downloads/PMID/labels for PMID:xxx).
    :param extra_prefixes: We default to only allowing the prefixes allowed for a particular type in Biolink.
        If you want to allow additional prefixes, list them here. They are appended *after* the
        Biolink-registered ones, so an extra prefix keeps its identifiers alive in the clique but can
        never win the preferred-CURIE contest -- which also means such an identifier will not
        normalize on its own, and is visible only in the clique's equivalent identifiers. That is the
        intended shape for shipping a prefix ahead of the Biolink Model (see
        `config.yaml: disease_extra_prefixes`), and the reason it is safe to do so.
    :param icrdf_filename: (REQUIRED) The file to read the information content from (icRDF.tsv). Although this is a
        named parameter to make it easier to specify this when calling write_compendium(), it is REQUIRED, and
        write_compendium() will throw a RuntimeError if it is not specified. This is to ensure that it has been
        properly specified as a prerequisite in a Snakemake file, so that write_compendium() is not run until after
        icRDF.tsv has been generated.
    :param properties_files: (OPTIONAL) A list of SQLite3 files containing properties to be added to the output.
    :return:
    """
    if extra_prefixes is None:
        extra_prefixes = []
    if labels is None:
        labels = {}
    logger.info(
        f"Starting write_compendium({metadata_yamls}, {len(synonym_list)} slists, {ofname}, {node_type}, {len(labels)} labels, {extra_prefixes}, {icrdf_filename}, {properties_jsonl_gz_files}): {get_memory_usage_summary()}"
    )
    config = get_config()
    cdir = config["output_directory"]
    biolink_version = config["biolink_version"]

    node_factory = NodeFactory(make_local_name(""), biolink_version)
    logger.info(f"NodeFactory ready: {node_factory} with {get_memory_usage_summary()}")
    synonym_factory = SynonymFactory(make_local_name(""))
    logger.info(f"SynonymFactory ready: {synonym_factory} with {get_memory_usage_summary()}")

    # Load the preferred_name_boost_prefixes -- this tells us which prefixes to boost when
    # coming up with a preferred label for a particular Biolink class.
    preferred_name_boost_prefixes = config["preferred_name_boost_prefixes"]

    # Load the per-type label length demotion config. Types not listed here are never demoted.
    demote_labels_longer_than = config.get("demote_labels_longer_than", {})

    # Create an InformationContentFactory based on the specified icRDF.tsv file. Default to the one in the download
    # directory.
    if not icrdf_filename:
        raise RuntimeError("No icrdf_filename parameter provided to write_compendium() -- this is required!")
    ic_factory = InformationContentFactory(icrdf_filename)
    logger.info(f"InformationContentFactory ready: {ic_factory} with {get_memory_usage_summary()}")

    description_factory = DescriptionFactory(make_local_name(""))
    logger.info(f"DescriptionFactory ready: {description_factory} with {get_memory_usage_summary()}")

    taxon_factory = TaxonFactory(make_local_name(""))
    logger.info(f"TaxonFactory ready: {taxon_factory} with {get_memory_usage_summary()}")

    if node_type is not None:
        node_test = node_factory.create_node(
            input_identifiers=[], node_type=node_type, labels={}, extra_prefixes=extra_prefixes
        )
        logger.info(f"NodeFactory test complete: {node_test} with {get_memory_usage_summary()}")
    else:
        logger.info("Skipping NodeFactory type test for heterogeneous typed cliques.")

    # Create compendia and synonyms directories, just in case they haven't been created yet.
    os.makedirs(os.path.join(cdir, "compendia"), exist_ok=True)
    os.makedirs(os.path.join(cdir, "synonyms"), exist_ok=True)

    # Load all the properties.
    property_list = PropertyList()
    if properties_jsonl_gz_files:
        for properties_jsonl_gz_file in properties_jsonl_gz_files:
            logger.info(f"Loading properties from {properties_jsonl_gz_file}...")
            count_loaded = property_list.add_properties_jsonl_gz(properties_jsonl_gz_file)
            logger.info(f"Loaded {count_loaded} unique properties from {properties_jsonl_gz_file}")
        logger.info(
            f"All {len(properties_jsonl_gz_files)} property files loaded ({property_list.count_unique()} total unique properties): {get_memory_usage_summary()}"
        )
    else:
        logger.info("No property files provided or loaded.")

    property_source_count = defaultdict(int)

    synonym_filter = get_synonym_filter()
    filter_count_snapshot = synonym_filter.filtered_count

    # Counts.
    count_cliques = 0
    count_eq_ids = 0
    count_synonyms = 0

    # Write compendium and synonym files.
    with (
        jsonlines.open(os.path.join(cdir, "compendia", ofname), "w") as outf,
        jsonlines.open(os.path.join(cdir, "synonyms", ofname), "w") as sfile,
    ):
        # Calculate an estimated time to completion.
        start_time = time.time_ns()
        count_slist = 0
        total_slist = len(synonym_list)

        for slist in synonym_list:
            clique_labels = labels
            if isinstance(slist, TypedClique):
                current_node_type = slist.node_type
                input_identifiers = slist.identifiers
                if slist.labels:
                    clique_labels = {**labels, **slist.labels}
            else:
                if node_type is None:
                    raise RuntimeError("write_compendium() requires node_type unless every clique is a TypedClique.")
                current_node_type = node_type
                input_identifiers = slist

            # Before we get started, let's estimate where we're at.
            count_slist += 1
            if (count_slist == 1) or (count_slist % WRITE_COMPENDIUM_LOG_EVERY_X_CLIQUES == 0):
                # TODO: replace with tqdm.
                time_elapsed_seconds = (time.time_ns() - start_time) / 1e9
                if time_elapsed_seconds < 0.001:
                    # We don't want to divide by zero.
                    time_elapsed_seconds = 0.001
                remaining_slist = total_slist - count_slist
                # count_slist --> time_elapsed_seconds
                # remaining_slist --> remaining_slist/count_slit*time_elapsed_seconds
                logger.info(
                    f"Generating compendia and synonyms for {ofname} currently at {count_slist:,} out of {total_slist:,} ({count_slist / total_slist * 100:.2f}%) in {format_timespan(time_elapsed_seconds)}: {get_memory_usage_summary()}"
                )
                logger.info(
                    f" - Current rate: {count_slist / time_elapsed_seconds:.2f} cliques/second or {time_elapsed_seconds / count_slist:.6f} seconds/clique."
                )

                time_remaining_seconds = time_elapsed_seconds / count_slist * remaining_slist
                logger.info(f" - Estimated time remaining: {format_timespan(time_remaining_seconds)}")

            node = node_factory.create_node(
                input_identifiers=input_identifiers,
                node_type=current_node_type,
                labels=clique_labels,
                extra_prefixes=extra_prefixes,
            )
            if node is None:
                # This usually happens because every CURIE in the node is not in the id_prefixes list for that node_type.
                # Something to fix at some point, but we don't want to break the pipeline for this, so
                # we emit a warning and skip this clique.
                logger.warning(
                    f"Could not create node for ({input_identifiers}, {current_node_type}, {labels}, {extra_prefixes}): returned None."
                )
                continue
            else:
                count_cliques += 1
                count_eq_ids += len(input_identifiers)

                nw = {"type": node["type"]}
                ic = ic_factory.get_ic(node)
                nw["ic"] = ic

                # Determine types.
                types = node_factory.get_ancestors(node["type"])

                # Generate a preferred label for this clique using choose_preferred_name().
                preferred_name = choose_preferred_name(
                    node, types, preferred_name_boost_prefixes, demote_labels_longer_than
                )

                # At this point, we insert any HAS_ADDITIONAL_ID IDs we have.
                # The logic we use is: we insert all additional IDs for a CURIE *AFTER* that CURIE, in a random order, as long
                # as the additional CURIE is not already in the list of CURIEs.
                #
                # We will attempt to retrieve a label or description for this ID as well.
                current_curies = set()
                identifier_list = []
                curie_labels = dict()
                for nid in node["identifiers"]:
                    iid = nid["identifier"]

                    # Prevent duplicates (might happen if e.g. we have an additional CURIE that duplicates an existing one later in the list).
                    if iid in current_curies:
                        continue

                    identifier_list.append(iid)
                    current_curies.add(iid)

                    if "label" in nid:
                        curie_labels[iid] = nid["label"]

                    # Are there any additional CURIEs for this CURIE?
                    props = property_list.get_all(iid, HAS_ALTERNATIVE_ID)
                    if props:
                        # Get just the additional CURIEs.
                        additional_curies = [prop.value for prop in props]

                        # ac_labelled will be a list that consists of either LabeledID (if the CURIE could be labeled)
                        # or str objects (consisting of an unlabeled CURIE).
                        ac_labelled = node_factory.apply_labels(
                            input_identifiers=additional_curies, labels=clique_labels, node_types=types
                        )

                        for prop, label in zip(props, ac_labelled):
                            additional_curie = Text.get_curie(label)
                            if ":" not in additional_curie:
                                raise ValueError(
                                    f"Additional ID '{additional_curie}' for '{iid}' is not a valid CURIE: {prop}, {label} (from {ac_labelled})"
                                )
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
                nw["identifiers"] = []
                for iid in identifier_list:
                    id_info = {"i": iid}

                    if iid in curie_labels:
                        id_info["l"] = curie_labels[iid]
                    else:
                        id_info["l"] = ""

                    if id_info["i"] in descs:
                        # Sort descriptions from the shortest to the longest.
                        id_info["d"] = list(sorted(descs[id_info["i"]], key=lambda x: len(x)))
                    else:
                        id_info["d"] = []

                    if id_info["i"] in taxa:
                        # Sort taxa by CURIE suffix.
                        id_info["t"] = list(sorted(taxa[id_info["i"]], key=get_numerical_curie_suffix))
                    else:
                        id_info["t"] = []

                    nw["identifiers"].append(id_info)

                # Write out the preferred name, if we have one.
                nw["preferred_name"] = preferred_name

                # Collect taxon IDs for this node.
                nw["taxa"] = list(sorted(set().union(*taxa.values()), key=get_numerical_curie_suffix))

                outf.write(nw)

                # get_synonyms() returns tuples in the form ('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym', 'Caudal articular process of eighteenth thoracic vertebra')
                # But we're only interested in the synonyms themselves, so we can skip the relationship for now.
                curie = node["identifiers"][0]["identifier"]

                # get_synonyms() returns a list of tuples, where each tuple is a relation and a synonym.
                # So we extract just the synonyms here, ditching the relations (result[0]), then unique-ify the
                # synonyms.
                synonyms = [
                    result[1] for result in synonym_factory.get_synonyms(identifier_list, node_types=types) if result[1]
                ]
                synonyms_list = sorted(set(synonyms), key=lambda x: len(x))

                try:
                    document = {
                        "curie": curie,
                        "names": synonyms_list,
                        "types": [t[8:] for t in types],
                    }  # remove biolink:

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
                    # Note that this includes all the alternative IDs.kl
                    document["clique_identifier_count"] = len(nw["identifiers"])

                    # We want to see if we can use the CURIE suffix to sort concepts with similar identifiers.
                    # We want to sort this numerically, so we only do this if the CURIE suffix is numerical.
                    curie_parts = curie.split(":", 1)
                    if len(curie_parts) > 0:
                        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
                        try:
                            document["curie_suffix"] = int(curie_parts[1])
                        except ValueError:
                            pass

                    # Collect taxon names for this node.
                    document["taxa"] = list(sorted(set().union(*taxa.values()), key=get_numerical_curie_suffix))
                    if len(document["taxa"]) > 0:
                        # This concept is specific to one or more particular taxa.
                        document["taxon_specific"] = True
                    else:
                        # This concept is not specific to any taxa (that we know about).
                        document["taxon_specific"] = False

                    sfile.write(document)
                except Exception as ex:
                    print(f"Exception thrown while write_compendium() was generating {ofname}: {ex}")
                    print(nw["type"])
                    print(node_factory.get_ancestors(nw["type"]))
                    traceback.print_exc()
                    raise ex

    # Log a per-compendium summary of any obsolete labels that were filtered.
    filtered_this_run = synonym_filter.filtered_count - filter_count_snapshot
    if filtered_this_run > 0:
        logger.warning(f"SynonymFilter: matched {filtered_this_run} obsolete label(s)/synonym(s) in {ofname}")
    else:
        logger.info(f"SynonymFilter: no obsolete labels found in {ofname}")

    # Write out the metadata.yaml file combining information from all the metadata.yaml files.
    write_combined_metadata(
        os.path.join(cdir, "metadata", ofname + ".yaml"),
        typ="compendium",
        name=ofname,
        counts={
            "cliques": count_cliques,
            "eq_ids": count_eq_ids,
            "synonyms": count_synonyms,
            "property_sources": dict(property_source_count),
        },
        combined_from_filenames=metadata_yamls,
    )

    # Close all the factories.
    taxon_factory.close()


def glom(conc_set, newgroups, unique_prefixes=["INCHIKEY"], pref="HP", close={}):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivalent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    n = 0
    bad = 0
    shit_prefixes = set(["KEGG", "PUBCHEM"])
    test_id = "xUBERON:0002262"
    debugit = False
    # excised = set()
    for xgroup in newgroups:
        if isinstance(xgroup, frozenset):
            group = set(xgroup)
        else:
            group = xgroup
        # As of now, xgroup should never be more than two things
        if len(xgroup) > 2:
            print(xgroup)
            print("nope nope nope")
            raise ValueError
        n += 1
        if debugit:
            print("new group", group)
        if test_id in group:
            print("higroup", group)
        # Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        existing_sets_w_x = [(conc_set[x], x) for x in group if x in conc_set]
        # All of these sets are now going to be combined through the equivalence of our new set.
        existing_sets = [es[0] for es in existing_sets_w_x]
        # x = [es[1] for es in existing_sets_w_x]
        newset = set().union(*existing_sets)
        if debugit:
            print("merges:", existing_sets)
        # put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        if test_id in newset:
            print("hiset", newset)
            print("input_set", group)
            print("esets")
            for eset in existing_sets:
                print(" ", eset, group.intersection(eset))
        for check_element in newset:
            prefix = check_element.split(":")[0]
            if prefix in shit_prefixes:
                print(prefix)
                print(check_element)
                raise Exception("garbage")
        if debugit:
            print("final set", newset)
        # make sure we didn't combine anything we want to keep separate
        setok = True
        if test_id in group:
            print("setok?", setok)
        for up in unique_prefixes:
            if test_id in group:
                print("up?", up)
            idents = [e if isinstance(e, str) else e.identifier for e in newset]
            if len(set([e for e in idents if (e.split(":")[0] == up)])) > 1:
                bad += 1
                setok = False
                wrote = set()
                for s in existing_sets:
                    fs = frozenset(s)
                    wrote.add(fs)
                for gel in group:
                    if Text.get_prefix_or_none(gel) == pref:
                        # killer = gel
                        pass
                # for preset in wrote:
                #    print(f'{killer}\t{set(group).intersection(preset)}\t{preset}\n')
                # print('------------')
        NPC = sum(1 for s in newset if s.startswith("PUBCHEM.COMPOUND:"))
        if ("PUBCHEM.COMPOUND:3100" in newset) and (NPC > 3):
            if debugit:
                raise ValueError(f"Debugging information: {sorted(list(newset))}")
        if not setok:
            # Our new group created a new set that merged stuff we didn't want to merge.
            # Previously we did a lot of fooling around at this point.  But now we're just going to say, I have a
            # pairwise concordance.  That can at most link two groups.  just don't link them. In other words,
            # we are simply ignoring this concordance.
            continue
            # Let's figure out the culprit(s) and excise them
            # counts = defaultdict(int)
            # for x in group:
            #    counts[x] += 1
            ##THe way existing sets was created, means that the same set can be in there twice, and we don't want to
            # count things that way
            # unique_existing_sets = []
            # for ex in existing_sets:
            #    u = True
            #    for q in unique_existing_sets:
            #        if ex == q:
            #            u = False
            #    if u:
            #        unique_existing_sets.append(ex)
            # for es in unique_existing_sets:
            #    for y in es:
            #        counts[y] += 1
            # bads = [ x for x,y in counts.items() if y > 1 ]
            # now we know which identifiers are causing trouble.
            # We don't want to completely throw them out, but we can't allow them to gum things up.
            # So, we need to first remove them from all the sets, then we need to put them in their own set
            # It might be good to track this somehow?
            # excised.update(bads)
            # for b in bads:
            #    if b in group:
            #        group.remove(b)
            #    for exset in existing_sets:
            #        if b in exset:
            #            exset.remove(b)
            #    conc_set[b] = set([b])
            # for x in group:
            #    conc_set[x] = group
            # continue
        # Now check the 'close' dictionary to see if we've accidentally gotten to a close match becoming an exact match
        setok = True
        for cpref, closedict in close.items():
            idents = set([e if isinstance(e, str) else e.identifier for e in newset])
            prefidents = [e for e in idents if e.startswith(cpref)]
            for pident in prefidents:
                for cd in closedict[pident]:
                    if cd in newset:
                        setok = False
            if len(prefidents) == 0:
                continue
        if not setok:
            continue
        # Now make all the elements point to this new set:
        for element in newset:
            conc_set[element] = newset


def get_prefixes(idlist):
    """Return a dictionary of identifiers from idlist with their prefix as the key.

    :param idlist: A list of identifiers. Should NOT contain any LabeledIDs.
    """
    prefs = defaultdict(list)
    for ident in idlist:
        if isinstance(ident, LabeledID):
            print("nonono")
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
    bad_values = [k for k, v in cmap.items() if v > 1]
    for bv in bad_values:
        if bv.startswith("Meta"):
            print(bv)
    for k, v in result_dict.items():
        newv = [vi for vi in v if vi not in bad_values]
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
    bad_ids = set([k for k, v in idcounts.items() if v > 1])
    #    for b in bad_ids:
    #        mm = mondomap[b]
    #        mondos = []
    #        for ms in mm:
    #            for x in ms:
    #                if Text.get_curie(x) == 'MONDO':
    #                    mondos.append(x)
    #        print(b, mondos)
    new_list = list(map(lambda term_list: set(filter(lambda term: term not in bad_ids, term_list)), old_list))
    return new_list


def read_identifier_file(infile):
    """Identifier files are mostly just lists of identifiers that constitutes all the id's from a given
    source that should be included in a normalization run.   There is an optional second column that contains
    a hint to the normalizer about the proper biolink type for this entity."""
    types = {}
    identifiers = list()
    with open(infile) as inf:
        for line in inf:
            x = line.strip().split("\t")
            identifiers.append((x[0],))
            if len(x) > 1:
                types[x[0]] = x[1]
    return identifiers, types


def read_badxrefs(fn):
    """Read an ``input_data/*_badxrefs.txt`` file into a set of ``(subject, object)`` tuples.

    Format is one space-separated pair per line; ``#`` comment lines and blank lines are
    skipped. These files drop individually wrong cross-reference pairs that survive
    prefix-level filtering, for cases where the target prefix is legitimate in general but
    this particular pair is not.

    Callers decide whether to match directionally (diseasephenotype) or in either direction
    (anatomy, which builds frozensets from these); the returned set is unordered either way.

    A line that is neither blank, a comment, nor exactly two space-separated tokens raises
    ``ValueError``. Skipping it instead would mean an entry a maintainer believed was
    suppressing a bad xref silently does nothing — the pair reappears in the compendia and
    nothing anywhere says why.

    Tabs are rejected explicitly, because a tab-separated pair is the easy way to write a line
    that looks right and parses wrong. Runs of spaces are not: ``split()`` collapses them, so a
    stray double space is unambiguous and is accepted rather than failing a build over it.
    """
    morebad = set()
    with open(fn) as inf:
        for lineno, line in enumerate(inf, 1):
            if line.startswith("#"):
                continue
            stripped = line.strip()
            if not stripped:
                continue
            if "\t" in stripped:
                raise ValueError(f"{fn}:{lineno}: CURIEs must be separated by a space, not a tab: {line.rstrip()!r}")
            x = stripped.split()
            if len(x) != 2:
                raise ValueError(f"{fn}:{lineno}: expected two space-separated CURIEs, got {len(x)}: {line.rstrip()!r}")
            morebad.add((x[0], x[1]))
    return morebad


def remove_overused_xrefs(pairlist: list[tuple], bothways: bool = False, target_prefixes=None):
    """Given a list of tuples (id1, id2) meaning id1-[xref]->id2, remove any id2 that are associated with more
    than one id1.  The idea is that if e.g. id1 is made up of UBERONS and 2 of those have an xref to say a UMLS
    then it doesn't mean that all of those should be identified.  We don't really know what it means, so remove it.

    :param target_prefixes: if given, only targets in these namespaces are eligible to be dropped;
        a target in any other namespace is kept however many subjects claim it. This scopes the
        filter to the vocabulary that is actually causing merges, instead of trading one source's
        real problem against the collateral damage to its other namespaces. DOID is the worked
        case: its ICD codes name disease *families* and fuse every subtype citing one, while its
        MeSH/SNOMED/UMLS targets are mostly fine -- so an unscoped filter under-cleans ICD (most
        ICD rows are 1:1) and over-cleans everything else. Matched against
        ``Text.get_prefix_or_none()``, which upper-cases, so the comparison is case-insensitive.
        See ``diseasephenotype.OVERUSE_FILTERED_CONCORDS`` and docs/sources/DOID/mappings.md.
    """
    eligible = {p.upper() for p in target_prefixes} if target_prefixes is not None else None
    xref_counts_v = defaultdict(int)
    xref_counts_k = defaultdict(int)
    for k, v in pairlist:
        xref_counts_v[v] += 1
        xref_counts_k[k] += 1
    improved_pairs = []
    for k, v in pairlist:
        if eligible is not None and (Text.get_prefix_or_none(v) or "") not in eligible:
            improved_pairs.append((k, v))
            continue
        if xref_counts_v[v] < 2:
            if bothways:
                if xref_counts_k[k] < 2:
                    improved_pairs.append((k, v))
            else:
                improved_pairs.append((k, v))
    return improved_pairs


# A prefix carrying a release stamp, e.g. DOID's "SNOMEDCT_US_2025_09_01:267692008". Sources that
# do this mint a new prefix on every upstream release, so an `op` map naming the stamped spellings
# silently goes stale -- and the un-renamed CURIE still reaches glom(), fusing subjects through a
# namespace no compendium can ever join. Match on the stem instead of pinning the dates.
VERSION_STAMPED_PREFIX = re.compile(r"^(.*)_\d{4}_\d{2}_\d{2}$")


def norm(x, op):
    """Rename a CURIE's prefix per the `op` map, keying on the upper-cased prefix.

    A prefix that misses is retried without a trailing `_YYYY_MM_DD` release stamp, so `op` names
    the stem (`SNOMEDCT_US`) once rather than every dated spelling a source has ever emitted.

    An `op` value is normally the replacement prefix. It may instead be a callable taking the whole
    CURIE and returning the rewritten one, for the cases where the target prefix depends on the
    local id and not just the source prefix -- OMIM is the one that needs this, since `MIM:PS303350`
    is a phenotypic series (`OMIM.PS:303350`) while `MIM:115210` is a plain entry (`OMIM:115210`).
    """
    # Get curie returns the uppercase
    pref = Text.get_prefix_or_none(x)
    if pref is None:
        return x
    stamped = VERSION_STAMPED_PREFIX.match(pref)
    for candidate in (pref, stamped.group(1) if stamped else None):
        if candidate in op:
            rename = op[candidate]
            return rename(x) if callable(rename) else Text.recurie(x, rename)
    return x

from os import path
from zipfile import ZipFile

import xmltodict

from src.babel_utils import pull_via_urllib
from src.prefixes import HMDB
from src.util import get_config, get_logger

logger = get_logger(__name__)

HMDB_DOWNLOAD_URL = "https://hmdb.ca/system/downloads/current/"
HMDB_ZIP_FILENAME = "hmdb_metabolites.zip"


def pull_hmdb():
    """Download and unpack the HMDB metabolites dump.

    HMDB sits behind a Cloudflare bot challenge that no unattended client can pass, so
    raise_if_cloudflare_challenge() tells the operator to fetch the zip in a browser and drop it
    in place. An already-present zip is therefore used as-is rather than re-downloaded --
    pull_via_urllib() deletes its target before each attempt, so without this the manual copy
    would be wiped and the rule would fail again the same way.
    """
    dname = path.join(get_config()["download_directory"], "HMDB", HMDB_ZIP_FILENAME)
    if path.exists(dname):
        logger.info("Using previously downloaded %s instead of downloading it again.", dname)
    else:
        dname = pull_via_urllib(HMDB_DOWNLOAD_URL, HMDB_ZIP_FILENAME, decompress=False, subpath="HMDB")
    ddir = path.dirname(dname)
    with ZipFile(dname, "r") as zipObj:
        zipObj.extractall(ddir)


def handle_metabolite(metabolite, lfile, synfile, smifile):
    hmdbident = f"{HMDB}:{metabolite['accession']}"
    label = metabolite["name"]
    lfile.write(f"{hmdbident}\t{label}\n")
    syns = metabolite["synonyms"]
    if (syns is not None) and ("synonym" in syns):
        # In some cases, syns['synonym'] may be a single string.
        # If so, we turn it into a single-element list.
        synonyms_list = syns["synonym"]
        if not isinstance(synonyms_list, list):
            synonyms_list = [synonyms_list]

        for sname in synonyms_list:
            synfile.write(f"{hmdbident}\toio:exact\t{sname}\n")
    if "smiles" in metabolite:
        smifile.write(f"{hmdbident}\t{metabolite['smiles']}\n")


def make_labels_and_synonyms_and_smiles(inputfile, labelfile, synfile, smifile):
    with open(inputfile) as inf:
        xml = inf.read()
    parsed = xmltodict.parse(xml)
    metabolites = parsed["hmdb"]["metabolite"]
    with open(labelfile, "w") as lfile, open(synfile, "w") as sfile, open(smifile, "w") as smiles:
        for metabolite in metabolites:
            handle_metabolite(metabolite, lfile, sfile, smiles)

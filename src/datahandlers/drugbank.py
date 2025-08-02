# Download CC-0 licensed data from DrugBank (https://go.drugbank.com/releases/latest)
import csv
import gzip
import os.path
import shutil
from zipfile import ZipFile

from src.util import get_config
import requests

from src.prefixes import DRUGBANK


def download_drugbank_vocabulary(drugbank_version, outfile):
    """ Download a particular version of the DrugBank vocabulary."""

    # Download from URL using Requests.
    response = requests.get(
        f"https://go.drugbank.com/releases/{drugbank_version}/downloads/all-drugbank-vocabulary",
        stream=True)

    with open(outfile + '.zip', 'wb') as fout:
        shutil.copyfileobj(response.raw, fout)

    # Decompress file.
    with ZipFile(outfile + '.zip', 'r') as zipObj:
        zipObj.extractall(os.path.dirname(outfile))


def extract_drugbank_labels_and_synonyms(drugbank_vocab_csv, labels, synonyms):
    """
    Extract labels and synonyms for DRUGBANK IDs from the DrugBank vocabulary file (see download_drugbank_vocabulary()).

    :param drugbank_vocab_csv: The DrugBank vocabulary file downloaded with download_drugbank_vocabulary().
    :param labels: The file to write labels into.
    :param synonyms: The file to write synonyms into.
    """

    with open(drugbank_vocab_csv, 'r') as fin, open(labels, 'w') as labelsf, open(synonyms, 'w') as synonymsf:
        reader = csv.DictReader(fin)
        assert 'DrugBank ID' in reader.fieldnames
        assert 'Common name' in reader.fieldnames
        assert 'Synonyms' in reader.fieldnames
        for line in reader:
            drugbank_id = f"{DRUGBANK}:{line['DrugBank ID']}"
            if 'Common name' in line and line['Common name'].strip() != '':
                labelsf.write(f"{drugbank_id}\t{line['Common name']}\n")
            if 'Synonyms' in line and line['Synonyms'].strip() != '':
                synonyms = line['Synonyms'].split(' | ')
                for syn in synonyms:
                    synonymsf.write(f"{drugbank_id}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{syn}\n")

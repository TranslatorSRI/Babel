from os import path
from zipfile import ZipFile
import requests
from pathlib import Path


def download_and_unzip(url, filepath):
    """
    Given a `.zip` file, download it to a location, then uncompress it in the same folder.
    :param url_prefix: The URL prefix, e.g. https://api.pharmgkb.org/v1/download/file/data/
    :param filename: The filename to download and save, e.g. genes.zip
    :param filepath: The location to write the file to.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    bname = Path(filepath).stem
    ddir = path.dirname(filepath)
    with ZipFile(filepath, 'r') as zipObj:
        zipObj.extractall(path.join(ddir, bname))


def pull_pharmgkb(genes_zip_filepath, chemicals_zip_filepath, drugs_zip_filepath):
    """
    Download the PharmaGKB files we need.
    """
    download_and_unzip('https://api.pharmgkb.org/v1/download/file/data/genes.zip', genes_zip_filepath)
    download_and_unzip('https://api.pharmgkb.org/v1/download/file/data/chemicals.zip', chemicals_zip_filepath)
    download_and_unzip('https://api.pharmgkb.org/v1/download/file/data/drugs.zip', drugs_zip_filepath)

from src.babel_utils import pull_via_urllib
from src.prefixes import HGNC
import json

from src.properties import PrefixPropertyStore


def pull_hgnc():
    # As per the "quick links" from https://www.genenames.org/download/archive/
    pull_via_urllib(
        'https://storage.googleapis.com/public-download-files/hgnc/json/json/',
        'hgnc_complete_set.json',
        decompress=False,
        subpath=HGNC)

def pull_hgnc_labels_and_synonyms(infile, labelfile, synonymfile):
    source = "datahandlers.hgnc:pull_hgnc_labels_and_synonyms()"

    with open(infile,'r') as data:
        hgnc_json = json.load(data)

    with PrefixPropertyStore(prefix=HGNC, autocommit=False) as pps:
        for gene in hgnc_json['response']['docs']:
            hgnc_id =gene['hgnc_id']
            symbol = gene['symbol']
            pps.add_label(hgnc_id, symbol, source)

            name = gene['name']
            pps.add_exact_synonym(hgnc_id, name, source)

            if 'alias_symbol' in gene:
                alias_symbols = gene['alias_symbol']
                for asym in alias_symbols:
                    pps.add_related_synonym(hgnc_id, asym, source)
            if 'alias_name' in gene:
                alias_names = gene['alias_name']
                for asym in alias_names:
                    pps.add_related_synonym(hgnc_id, asym, source)

        with open(labelfile,'w') as lfile:
            pps.to_tsv(lfile, include_properties=False)
        with open(synonymfile,'w') as sfile:
            pps.to_tsv(sfile, include_properties=True)
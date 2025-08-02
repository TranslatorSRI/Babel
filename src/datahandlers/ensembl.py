import json

from src.util import get_config
from apybiomart import find_datasets, query, find_attributes
import logging
import os

# As per https://support.bioconductor.org/p/39744/#39751, more attributes than this result in an
# error from BioMart: Too many attributes selected for External References
# This is the real MAX minus one: for every batch, we'll query the ensembl_gene_id so that we can
# put the batches back together again afterward.
BIOMART_MAX_ATTRIBUTE_COUNT = 8


# Note that Ensembl doesn't seem to assign its own labels or synonyms to its gene identifiers.  It appears that
# they are all imported from other sources.   Therefore, we will not generate labels or synonym files.  We
# do pull down data so that we can get the full list of ensembl identifiers though.

# In principle, we want to pull some file from somewhere, but ensembl, in all of its glory, lacks a list of
# genes that can be gathered without downloading hundreds of gigs of other stuff.  So, we'll use biomart to pull
# just what we need.
def pull_ensembl(ensembl_dir, complete_file, only_download_datasets=None, max_attribute_count=BIOMART_MAX_ATTRIBUTE_COUNT,) -> dict:
    """
    Pulls gene information from Ensembl datasets, filters the datasets based on provided
    criteria, and saves the queried data locally.

    This function handles downloading and processing gene dataset information from Ensembl
    using the BioMart API. It allows filtering specific datasets and skips those marked to
    be excluded in configuration settings. Data is saved in a tab-separated value format for
    each dataset, and a log file is created indicating the number of processed datasets.

    :param ensembl_dir: The directory to download ENSEMBL files to.
    :param complete_file: A file that will be created to indicate that the pull is complete.
    :param only_download_datasets: A list of dataset IDs to download. If None, all
        datasets will be evaluated except those marked in skip config. Can be used to
        override config['ensembl_datasets_to_skip']
    :param max_attribute_count: The maximum number of attributes to request in a single query.
        Should be the (actual max - 1), because we'll need to use the ensembl_gene_id to merge.
        Defaults to BIOMART_MAX_ATTRIBUTE_COUNT.
    :return: A dictionary with dataset names as keys as the number of downloads with dictionaries describing
        each dataset as values.
    """
    # Find ENSEMBL datasets. Then either:
    # - Only download the datasets listed in only_download_datasets, or
    # - Process all the datasets EXCEPT those listed in get_config()['ensembl_datasets_to_skip']
    f = find_datasets()
    skip_dataset_ids = set(get_config()['ensembl_datasets_to_skip'])
    dataset_ids = f['Dataset_ID']
    if only_download_datasets is not None:
        dataset_ids = only_download_datasets
        skip_dataset_ids = set()

    # Prepare a report to return.
    report = {}

    # Columns to choose.
    cols_to_find = {"ensembl_gene_id", "ensembl_peptide_id", "description", "external_gene_name", "external_gene_source",
            "external_synonym", "chromosome_name", "source", "gene_biotype", "entrezgene_id", "zfin_id_id", 'mgi_id',
            'rgd_id', 'flybase_gene_id', 'sgd_gene', 'wormbase_gene'}
    for ds in dataset_ids:
        logging.info(f"Downloading ENSEMBL dataset {ds}")
        if ds in skip_dataset_ids:
            print(f'Skipping {ds} as it is included in skip_dataset_ids: {skip_dataset_ids}')
            continue
        outfile = os.path.join(ensembl_dir, ds, 'BioMart.tsv')
        # Really, we should let snakemake handle this, but then we would need to put a list of all the 200+ sets in our
        # config, and keep it up to date.  Maybe you could have a job that gets the datasets and writes a dataset file,
        # but then updates the config? That sounds bogus.
        if os.path.exists(outfile):
            report[ds] = {
                'status': 'skipped',
                'output_file': outfile,
                'batches': [],
                'message': f"Output file already exists for dataset {ds}, skipping."
            }
            logging.info(f'Skipping {ds} as it already exists')
            continue
        try:
            atts = find_attributes(ds)
            existingatts = set(atts['Attribute_ID'].to_list())
            attsIcanGet = cols_to_find.intersection(existingatts)

            if len(attsIcanGet) <= max_attribute_count:
                # Excellent: we can do this in one query.
                logging.info(f'Found {len(attsIcanGet)} attributes for {ds} for single query: {attsIcanGet}')
                df = query(attributes=list(attsIcanGet), filters={}, dataset=ds)
                report[ds] = {
                    'status': 'downloaded',
                    'message': f"Downloaded dataset {ds} in a single query",
                    'batches': [],
                    'attributes': list(attsIcanGet),
                    'num_rows': len(df),
                    'output_file': outfile,
                }
            else:
                # We need to retrieve all the attributes in batches.
                # We'll remove ensembl_gene_id from the list so we can use that to stitch the individual
                # results back together again.
                attributes_to_retrieve = list(attsIcanGet)
                attributes_to_retrieve.remove('ensembl_gene_id')
                df = None
                report[ds] = {
                    'status': 'downloading',
                    'message': f"Dataset {ds} has more than {max_attribute_count} attributes for single query, so they will be downloaded in batches.",
                    'batches': [],
                    'output_file': outfile,
                }
                # Go through the list of attributes stepping at max_attribute_count.
                for i in range(0, len(attributes_to_retrieve), max_attribute_count):
                    # Create a batch of attributes to query.
                    attr_batch = attributes_to_retrieve[i:i + max_attribute_count]
                    logging.info(f"Querying batch of {len(attr_batch)} attributes for {ds} (+ 'ensembl_gene_id'): {attr_batch}")

                    # Download the list of Biomart records for this set of attributes for this dataset.
                    batch_df = query(attributes=['ensembl_gene_id'] + list(attr_batch), filters={}, dataset=ds)
                    report[ds]['batches'].append({
                        'attributes': attr_batch,
                        'num_rows': len(batch_df),
                    })
                    if df is None:
                        # If we're the first df, we don't need to merge anything.
                        df = batch_df
                    else:
                        # Merge these new results with the earlier results.
                        df = df.merge(batch_df, on='Gene stable ID', how='outer', sort=True)

                # Note that we downloaded all the batches.
                report[ds]['status'] = 'downloaded'
                report[ds]['message'] =  f"Dataset {ds} has more than {max_attribute_count} attributes for single query, so they were downloaded in batches."
                report[ds]['attributes'] = list(attsIcanGet)
                report[ds]['num_rows'] = len(df)

            # Write out the data frame after sorting it by Gene stable ID.
            df.sort_values(by=['Gene stable ID'], inplace=True)
            os.makedirs(os.path.dirname(outfile))
            df.to_csv(outfile, index=False, sep='\t')
        except Exception as exc:
            biomart_dir = os.path.dirname(outfile)
            print(f'Deleting BioMart directory {biomart_dir} so its clear it needs to be downloaded again.')
            if os.path.exists(biomart_dir):
                os.rmdir(biomart_dir)
            raise exc

    # Write out a complete file with the report as a JSON object.
    with open(complete_file, 'w') as outf:
        json.dump(report, outf, indent=2, sort_keys=True)

    return report


if __name__ == '__main__':
    ensembl_dir = os.path.join(get_config()['babel_downloads'], 'ENSEMBL')
    pull_ensembl(ensembl_dir, os.path.join(ensembl_dir, 'BioMartDownloadComplete'))

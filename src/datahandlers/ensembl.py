from src.babel_utils import get_config
from apybiomart import find_datasets, query, find_attributes
import logging
import os

# As per https://support.bioconductor.org/p/39744/#39751, more attributes than this result in an
# error from BioMart: Too many attributes selected for External References
# This is the real MAX minus one: for every batch, we'll query the ensembl_gene_id so that we can
# put the batches back together again afterwards.
BIOMART_MAX_ATTRIBUTE_COUNT = 9


# Note that Ensembl doesn't seem to assign its own labels or synonyms to its gene identifiers.  It appears that
# they are all imported from other sources.   Therefore, we will not generate labels or synonym files.  We
# do pull down data so that we can get the full list of ensembl identifiers though.

# In principle, we want to pull some file from somewhere, but ensembl, in all of its glory, lacks a list of
# genes that can be gathered without downloading hundreds of gigs of other stuff.  So, we'll use biomart to pull
# just what we need.
def pull_ensembl(ensembl_dir, complete_file, only_download_datasets=None):
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
    :return: None
    """
    if only_download_datasets is None:
        only_download_datasets = set()
    f = find_datasets()

    skip_dataset_ids = set(get_config()['ensembl_datasets_to_skip'])
    dataset_ids = f['Dataset_ID']
    if only_download_datasets:
        dataset_ids = only_download_datasets
        skip_dataset_ids = set()

    cols = {"ensembl_gene_id", "ensembl_peptide_id", "description", "external_gene_name", "external_gene_source",
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
            logging.info(f'Skipping {ds} as it already exists')
            continue
        try:
            atts = find_attributes(ds)
            existingatts = set(atts['Attribute_ID'].to_list())
            attsIcanGet = cols.intersection(existingatts)

            if len(attsIcanGet) <= BIOMART_MAX_ATTRIBUTE_COUNT:
                # Excellent: we can do this in one query.
                logging.info(f'Found {len(attsIcanGet)} attributes for {ds} for single query: {attsIcanGet}')
                df = query(attributes=list(attsIcanGet), filters={}, dataset=ds)
            else:
                # We need to retrieve all the attributes in batches.
                # We'll remove ensembl_gene_id from the list to
                attributes_to_retrieve = list(attsIcanGet)
                attributes_to_retrieve.remove('ensembl_gene_id')
                df = None
                for i in range(0, len(attributes_to_retrieve), BIOMART_MAX_ATTRIBUTE_COUNT):
                    attr_batch = attributes_to_retrieve[i:i + BIOMART_MAX_ATTRIBUTE_COUNT]
                    logging.info(f"Querying batch of {len(attr_batch)} attributes for {ds} (+ 'ensembl_gene_id'): {attr_batch}")
                    batch_df = query(attributes=['ensembl_gene_id'] + list(attr_batch), filters={}, dataset=ds)
                    if df is None:
                        df = batch_df
                    else:
                        df = df.merge(batch_df, on='Gene stable ID', how='outer')

            # Write out the data frame.
            os.makedirs(os.path.dirname(outfile))
            df.to_csv(outfile, index=False, sep='\t')
        except Exception as exc:
            biomart_dir = os.path.dirname(outfile)
            print(f'Deleting BioMart directory {biomart_dir} so its clear it needs to be downloaded again.')
            if os.path.exists(biomart_dir):
                os.rmdir(biomart_dir)
            raise exc
    with open(complete_file, 'w') as outf:
        outf.write(f'Downloaded gene sets for {len(f)} data sets.')


if __name__ == '__main__':
    ensembl_dir = os.path.join(get_config()['babel_downloads'], 'ENSEMBL')
    pull_ensembl(ensembl_dir, os.path.join(ensembl_dir, 'BioMartDownloadComplete'))

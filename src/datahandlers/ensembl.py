import json
import os
import shutil
import time

import apybiomart.classes as _apy_classes
from apybiomart import find_attributes, find_datasets, query

from src.util import ensure_parent_dir, get_config, get_logger

logger = get_logger(__name__)

# apybiomart's pre-flight connectivity check uses HTTPS and fails with SSL
# certificate errors in some environments (see
# https://github.com/robertopreste/apybiomart/issues/131). The check is
# redundant — if BioMart is actually unreachable the query will fail anyway.
# Bypass it here until apybiomart is replaced (see
# https://github.com/NCATSTranslator/Babel/pull/588).
_apy_classes._Server._check_connection = staticmethod(lambda: True)

# A conservative global stand-in for a limit BioMart actually applies *per attribute page*: it
# refuses a query selecting more than about three External References attributes (entrezgene_id,
# zfin_id_id, mgi_id, rgd_id, flybase_gene_id, sgd_gene, wormbase_gene) with "Too many attributes
# selected for External References" -- see https://support.bioconductor.org/p/39744/#39751 and
# https://github.com/NCATSTranslator/Babel/issues/193.
#
# It is NOT a cap on the total number of attributes, and reading it as one will mislead you: a
# dataset with few cross-references serves 8 attributes in one query quite happily, while
# hgfemale_gene_ensembl fails at 13 because five of them are External References. Batching by a
# flat count is simply the cheapest way to stay under the real limit without modelling which page
# each attribute belongs to.
#
# This is the real MAX minus one: for every batch, we'll query the ensembl_gene_id so that we can
# put the batches back together again afterward.
BIOMART_MAX_ATTRIBUTE_COUNT = 6
BIOMART_MAX_RETRIES = 5
BIOMART_RETRY_DELAY_SECS = 30

# The BioMart attributes Babel wants from every Ensembl gene dataset: the identifier itself, the
# label/description fields, and the cross-references to the model-organism databases. A dataset
# exposes only some of these (a plant dataset has no MGI id), so pull_ensembl() intersects this
# with the dataset's own attribute list and asks only for what is there.
#
# Module-level rather than local to pull_ensembl() because it is the input that decides whether a
# dataset needs batching, so a test cannot tell a genuinely-unbatched download from a batched one
# without it.
BIOMART_ATTRIBUTES = {
    "ensembl_gene_id",
    "ensembl_peptide_id",
    "description",
    "external_gene_name",
    "external_gene_source",
    "external_synonym",
    "chromosome_name",
    "source",
    "gene_biotype",
    "entrezgene_id",
    "zfin_id_id",
    "mgi_id",
    "rgd_id",
    "flybase_gene_id",
    "sgd_gene",
    "wormbase_gene",
}


# Note that Ensembl doesn't seem to assign its own labels or synonyms to its gene identifiers.  It appears that
# they are all imported from other sources.   Therefore, we will not generate labels or synonym files.  We
# do pull down data so that we can get the full list of ensembl identifiers though.


# In principle, we want to pull some file from somewhere, but ensembl, in all of its glory, lacks a list of
# genes that can be gathered without downloading hundreds of gigs of other stuff.  So, we'll use biomart to pull
# just what we need.
def pull_ensembl(
    ensembl_dir,
    complete_file,
    only_download_datasets=None,
    max_attribute_count=BIOMART_MAX_ATTRIBUTE_COUNT,
) -> dict:
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
    skip_dataset_ids = set(get_config()["ensembl_datasets_to_skip"])
    dataset_ids = f["Dataset_ID"]
    if only_download_datasets is not None:
        dataset_ids = only_download_datasets
        skip_dataset_ids = set()

    # Prepare a report to return.
    report = {}
    failed_datasets = {}

    # Columns to choose.
    cols_to_find = BIOMART_ATTRIBUTES
    for ds in dataset_ids:
        logger.info(f"Downloading ENSEMBL dataset {ds}")
        if ds in skip_dataset_ids:
            print(f"Skipping {ds} as it is included in skip_dataset_ids: {skip_dataset_ids}")
            continue
        outfile = os.path.join(ensembl_dir, ds, "BioMart.tsv")
        # Really, we should let snakemake handle this, but then we would need to put a list of all the 200+ sets in our
        # config, and keep it up to date.  Maybe you could have a job that gets the datasets and writes a dataset file,
        # but then updates the config? That sounds bogus.
        if os.path.exists(outfile):
            report[ds] = {
                "status": "skipped",
                "output_file": outfile,
                "batches": [],
                "message": f"Output file already exists for dataset {ds}, skipping.",
            }
            logger.info(f"Skipping {ds} as it already exists")
            continue
        last_exc = None
        for attempt in range(1, BIOMART_MAX_RETRIES + 1):
            try:
                atts = find_attributes(ds)
                existingatts = set(atts["Attribute_ID"].to_list())
                attsIcanGet = cols_to_find.intersection(existingatts)

                if len(attsIcanGet) <= max_attribute_count:
                    # Excellent: we can do this in one query.
                    logger.info(f"Found {len(attsIcanGet)} attributes for {ds} for single query: {attsIcanGet}")
                    df = query(attributes=list(attsIcanGet), filters={}, dataset=ds)
                    report[ds] = {
                        "status": "downloaded",
                        "message": f"Downloaded dataset {ds} in a single query",
                        "batches": [],
                        "attributes": list(attsIcanGet),
                        "num_rows": len(df),
                        "output_file": outfile,
                    }
                else:
                    # We need to retrieve all the attributes in batches.
                    # We'll remove ensembl_gene_id from the list so we can use that to stitch the individual
                    # results back together again.
                    attributes_to_retrieve = list(attsIcanGet)
                    attributes_to_retrieve.remove("ensembl_gene_id")
                    df = None
                    report[ds] = {
                        "status": "downloading",
                        "message": f"Dataset {ds} has more than {max_attribute_count} attributes for single query, so they will be downloaded in batches.",
                        "batches": [],
                        "output_file": outfile,
                    }
                    # Go through the list of attributes stepping at max_attribute_count.
                    for i in range(0, len(attributes_to_retrieve), max_attribute_count):
                        # Create a batch of attributes to query.
                        attr_batch = attributes_to_retrieve[i : i + max_attribute_count]
                        logger.info(
                            f"Querying batch of {len(attr_batch)} attributes for {ds} (+ 'ensembl_gene_id'): {attr_batch}"
                        )

                        # Download the list of Biomart records for this set of attributes for this dataset.
                        batch_df = query(attributes=["ensembl_gene_id"] + list(attr_batch), filters={}, dataset=ds)
                        report[ds]["batches"].append(
                            {
                                "attributes": attr_batch,
                                "num_rows": len(batch_df),
                            }
                        )
                        if df is None:
                            # If we're the first df, we don't need to merge anything.
                            df = batch_df
                        else:
                            # Merge these new results with the earlier results.
                            df = df.merge(batch_df, on="Gene stable ID", how="outer", sort=True)

                    # Note that we downloaded all the batches.
                    report[ds]["status"] = "downloaded"
                    report[ds]["message"] = (
                        f"Dataset {ds} has more than {max_attribute_count} attributes for single query, so they were downloaded in batches."
                    )
                    report[ds]["attributes"] = list(attsIcanGet)
                    report[ds]["num_rows"] = len(df)

                # Write out the data frame after sorting it by Gene stable ID.
                df.sort_values(by=["Gene stable ID"], inplace=True)
                ensure_parent_dir(outfile)
                df.to_csv(outfile, index=False, sep="\t")
                break
            except Exception as exc:
                last_exc = exc
                biomart_dir = os.path.dirname(outfile)
                logger.warning(f"Failed to download dataset {ds} (attempt {attempt}/{BIOMART_MAX_RETRIES}): {exc}")
                if os.path.exists(biomart_dir):
                    logger.warning(f"Deleting BioMart directory {biomart_dir}")
                    shutil.rmtree(biomart_dir)
                if attempt < BIOMART_MAX_RETRIES:
                    logger.info(f"Retrying dataset {ds} in {BIOMART_RETRY_DELAY_SECS}s...")
                    time.sleep(BIOMART_RETRY_DELAY_SECS)
        else:
            report[ds] = {"status": "failed", "message": str(last_exc), "output_file": outfile}
            failed_datasets[ds] = last_exc
            logger.error(f"Dataset {ds} failed all {BIOMART_MAX_RETRIES} attempts; continuing with remaining datasets.")

    # Write out a complete file with the report as a JSON object.
    with open(complete_file, "w") as outf:
        json.dump(report, outf, indent=2, sort_keys=True)

    if failed_datasets:
        failed_list = ", ".join(failed_datasets)
        raise RuntimeError(
            f"{len(failed_datasets)} dataset(s) failed after {BIOMART_MAX_RETRIES} attempts each: {failed_list}"
        )

    return report


if __name__ == "__main__":
    ensembl_dir = os.path.join(get_config()["babel_downloads"], "ENSEMBL")
    pull_ensembl(ensembl_dir, os.path.join(ensembl_dir, "BioMartDownloadComplete"))

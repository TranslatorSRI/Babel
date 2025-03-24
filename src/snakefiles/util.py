# Shared code used by Snakemake files
import shutil
import gzip

import src.util

logger = src.util.LoggingUtil.init_logging(__name__, level="INFO")

def write_done(filename):
    """ Write a file to indicate that we are done. """
    with open(filename, 'w') as f:
        print("done", f)


def gzip_files(input_filenames):
    """ Compress files using Gzip. Like with `gzip`, we compress the file by adding `.gz` to the end of the filename, but
    we do NOT delete the original file -- we'll leave that to the user.

    :param input_filenames: A list of Gzip files to compress.
    """
    logger.info(f"Compressing: {input_filenames}")
    for filename in input_filenames:
        output_filename = filename + '.gz'
        with open(filename, 'rb') as f_in, gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        logger.info(f"Compressed {filename} to {output_filename} using the gzip module.")
    logger.info(f"Done compressing: {input_filenames}")


# List of all the compendia files that need to be converted.
def get_all_compendia(config):
    return (config['anatomy_outputs'] +
            config['chemical_outputs'] +
            config['disease_outputs'] +
            config['gene_outputs'] +
            config['genefamily_outputs'] +
            config['process_outputs'] +
            config['protein_outputs'] +
            config['taxon_outputs'] +
            config['cell_line_outputs'] +
            config['umls_outputs'] +
            config['macromolecularcomplex_outputs'] +
            config['publication_outputs'])


def get_all_synonyms(config):
    """
    List of all the synonym files, including DrugChemicalConflated. Note that this duplicates synonyms: chemical output
    synonyms will be in both the individual chemical outputs and the DrugChemicalConflated file.

    :param config: The Babel config to use.
    :return: A list of filenames expected in the `synonyms/` directory.
    """
    return (
        config['anatomy_outputs'] +
        config['gene_outputs'] +
        config['protein_outputs'] +
        config['disease_outputs'] +
        config['process_outputs'] +
        config['chemical_outputs'] +
        config['taxon_outputs'] +
        config['cell_line_outputs'] +
        config['genefamily_outputs'] +
        config['drugchemicalconflated_synonym_outputs'] +
        config['umls_outputs'] +
        config['macromolecularcomplex_outputs'] +
        # Publication.txt is empty, but it's still created, so it needs to be here.
        config['publication_outputs']
    )


def get_all_synonyms_except_drugchemicalconflated(config):
    """
    List of all the synonym files, except DrugChemicalConflated.

    :param config: The Babel config to use.
    :return: A list of filenames expected in the `synonyms/` directory.
    """
    return (
            config['anatomy_outputs'] +
            config['gene_outputs'] +
            config['protein_outputs'] +
            config['disease_outputs'] +
            config['process_outputs'] +
            config['chemical_outputs'] +
            config['taxon_outputs'] +
            config['cell_line_outputs'] +
            config['genefamily_outputs'] +
            # config['drugchemicalconflated_synonym_outputs'] +
            config['umls_outputs'] +
            config['macromolecularcomplex_outputs']
    )


def get_all_synonyms_with_drugchemicalconflated(config):
    """
    List of all the synonym files including DrugChemicalConflated instead of the files it duplicates.

    :param config: The Babel config to use.
    :return: A list of filenames expected in the `synonyms/` directory.
    """
    return (
            config['anatomy_outputs'] +
            config['gene_outputs'] +
            config['protein_outputs'] +
            config['disease_outputs'] +
            config['process_outputs'] +
            # config['chemical_outputs'] +
            config['taxon_outputs'] +
            config['cell_line_outputs'] +
            config['genefamily_outputs'] +
            config['drugchemicalconflated_synonym_outputs'] +
            config['umls_outputs'] +
            config['macromolecularcomplex_outputs']
    )


def get_all_gzipped(config, list):
    """
    Helper method to add '.gz' to all the files in a list (presumably from get_all_synonyms_*()).

    :param config: The Babel config to use.
    :param list: List of filenames.
    :return: List of filenames with '.gz' appended.
    """
    return list(map(lambda x: x + '.gz', list))

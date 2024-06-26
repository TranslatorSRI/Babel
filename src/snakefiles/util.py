# Shared code used by Snakemake files

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
            config['genefamily_outputs'] +
            config['drugchemicalconflated_synonym_outputs'] +
            config['umls_outputs'] +
            config['macromolecularcomplex_outputs']
    )
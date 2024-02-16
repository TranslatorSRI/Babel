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
            config['macromolecularcomplex_outputs'])


# List of all the synonym files, except DrugChemicalConflated.
def get_synonyms(config):
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


# List of all the synonym files including DrugChemicalConflated instead of the files it
# duplicates.
def get_all_synonyms_with_drugchemicalconflated(config):
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
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
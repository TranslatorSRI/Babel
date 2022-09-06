from src.createcompendia import umls

##
## This Snakefile implements the algorithm proposed in
## https://github.com/TranslatorSRI/NodeNormalization/issues/119#issuecomment-1154751451
##
## 1. Once all the other targets have been generated, we make a list of every UMLS term
##    that has been mapped in all the output compendia files.
## 2. We then go through MRCONSO.RRF and note down all UMLS concepts that have NOT been mapped,
##    and write them into its own compendia, consisting only of:
##      - UMLS identifiers
##      - Label
##      - A Biolink category mapped from the UMLS category
## 3. To help with debugging this, we also create a report that summarizes the categories of
##    these "leftover" UMLS concepts -- the idea is that they are here to be used for labels,
##    but eventually any significant gaps in UMLS should be filled in.
##

configfile: "config.json"

rule leftover_umls:
    input:
        input_compendia = expand("{output}/compendia/{compendium}", output=config['output_directory'],
            compendium=config['anatomy_outputs'] +
                config['gene_outputs'] +
                config['protein_outputs'] +
                config['disease_outputs'] +
                config['process_outputs'] +
                config['chemical_outputs'] +
                config['taxon_outputs'] +
                config['genefamily_outputs']),
        mrconso = config['input_directory'] + '/private/MRCONSO.RRF',
        mrsty = config['input_directory'] + '/private/MRSTY.RRF'
    output:
        umls_compendium = config['output_directory'] + "/compendia/umls.txt",
        report = config['output_directory'] + "/reports/umls.txt",
        done = config['output_directory'] + "/reports/umls_done"
    run:
        umls.write_leftover_umls(input.input_compendia, input.mrconso, input.mrsty, output.umls_compendium, output.report, output.done)

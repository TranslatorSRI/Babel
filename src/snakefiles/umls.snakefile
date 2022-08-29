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

# Process all the compendia, except for umls.txt itself.
all_compendia = filter(lambda x: x != 'umls.txt',
    glob_wildcards(config['output_directory'] + "/compendia/{compendia}.txt").compendia)

rule leftover_umls:
    input:
        input_compendia = expand("{output}/compendia/{compendium}.txt", output=config['output_directory'], compendium=all_compendia),
        mrconso = config['input_directory'] + '/private/MRCONSO.RRF',
        mrsty = config['input_directory'] + '/private/MRSTY.RRF'
    output:
        umls_compendium = config['output_directory'] + "/compendia/umls.txt",
        report = config['output_directory'] + "/reports/umls.txt",
        done = config['output_directory'] + "/reports/umls_done"
    run:
        umls.write_leftover_umls(input.input_compendia, input.mrconso, input.mrsty, output.umls_compendium, output.report, output.done)

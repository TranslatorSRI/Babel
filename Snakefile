import src.createcompendia.anatomy as anatomy
import src.createcompendia.geneprotein as geneprotein
import src.assess_compendia as assessments

configfile: "config.json"

include: "src/snakefiles/datacollect.snakefile"
include: "src/snakefiles/anatomy.snakefile"
include: "src/snakefiles/gene.snakefile"

rule all:
    input:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])

rule clean_compendia:
    params:
        dir=config['output_directory']
    shell:
        "rm {params.dir}/compendia/*; rm {params.dir}/synonyms/*"

rule clean_data:
    params:
        dir=config['download_directory']
    shell:
        "rm -rf {params.dir}/*"

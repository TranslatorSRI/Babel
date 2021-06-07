configfile: "config.json"

include: "src/snakefiles/datacollect.snakefile"
include: "src/snakefiles/anatomy.snakefile"
include: "src/snakefiles/gene.snakefile"
include: "src/snakefiles/protein.snakefile"
include: "src/snakefiles/geneprotein.snakefile"
include: "src/snakefiles/diseasephenotype.snakefile"
include: "src/snakefiles/process.snakefile"
include: "src/snakefiles/chemical.snakefile"
include: "src/snakefiles/taxon.snakefile"

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


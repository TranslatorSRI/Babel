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
include: "src/snakefiles/genefamily.snakefile"

rule all:
    input:
        config['output_directory'] + '/reports/anatomy_done',
        config['output_directory'] + '/reports/chemicals_done',
        config['output_directory'] + '/reports/disease_done',
        config['output_directory'] + '/reports/gene_done',
        config['output_directory'] + '/reports/genefamily_done',
        config['output_directory'] + '/reports/process_done',
        config['output_directory'] + '/reports/protein_done',
        config['output_directory'] + '/reports/taxon_done'
    output:
        x = config['output_directory'] + '/reports/all_done'
    shell:
        "echo 'done' >> hi"


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


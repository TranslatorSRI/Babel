from src import datacollect
from src import anatomy

configfile: "config.json"

rule all:
    input:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])

#####
#
# Data sets: pull data sets, and parse them to get labels and synonyms
#
####

### MESH

rule get_mesh:
    output:
        config['download_directory']+'/MESH/mesh.nt'
    run:
        datacollect.pull_mesh()

rule get_mesh_labels:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        config['download_directory']+'/MESH/labels'
    run:
        datacollect.pull_mesh_labels()

rule get_mesh_synonyms:
    #We don't actually get any.  Maybe we could from the nt?
    output:
        ofn=config['download_directory']+'/MESH/synonyms'
    shell:
        "touch {output.ofn}"

### UMLS / SNOMEDCT

rule get_umls_labels_and_synonyms:
    output:
        config['download_directory']+'/UMLS/labels',
        config['download_directory']+'/UMLS/synonyms',
        config['download_directory']+'/SNOMEDCT/labels',
        config['download_directory']+'/SNOMEDCT/synonyms'
    run:
        datacollect.pull_umls()

### OBO Ontologies

rule get_ontology_labels_and_synonyms:
    output:
        expand("{download_directory}/{onto}/labels", download_directory = config['download_directory'], onto = config['ubergraph_ontologies']),
        expand("{download_directory}/{onto}/synonyms", download_directory = config['download_directory'], onto = config['ubergraph_ontologies'])
    run:
        datacollect.pull_uber(config['ubergraph_ontologies'])

####
#
# Categories: For a given biolink class (or set of related classes), build files showing possible cross-vocabulary
#   relationships. Then combine those to create the compendia and related synonym files.
#
####

### AnatomicalEntity / Cell / CellularComponent

rule get_anatomy_relationships:
    output:
        outf = config['download_directory']+'/anatomy/concords'
    run:
        anatomy.build_anatomy_relationships(output.outf)

rule anatomy:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['anatomy_prefixes']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['anatomy_prefixes']),
        inflist=[config['download_directory']+'/anatomy/concords']
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
    run:
        anatomy.build_concordance(input.inflist)

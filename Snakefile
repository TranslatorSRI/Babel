import src.datahandlers.mesh as mesh
import src.datahandlers.obo as obo
import src.datahandlers.umls as umls
import src.createcompendia.anatomy as anatomy
import src.assess_compendia as assessments

configfile: "config.json"

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
        mesh.pull_mesh()

rule get_mesh_labels:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        config['download_directory']+'/MESH/labels'
    run:
        mesh.pull_mesh_labels()

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
        umls.pull_umls()

### OBO Ontologies

rule get_ontology_labels_and_synonyms:
    output:
        expand("{download_directory}/{onto}/labels", download_directory = config['download_directory'], onto = config['ubergraph_ontologies']),
        expand("{download_directory}/{onto}/synonyms", download_directory = config['download_directory'], onto = config['ubergraph_ontologies'])
    run:
        obo.pull_uber(config['ubergraph_ontologies'])

####
#
# Categories: For a given biolink class (or set of related classes), build files showing possible cross-vocabulary
#   relationships. Then combine those to create the compendia and related synonym files.
#
####

### AnatomicalEntity / Cell / CellularComponent

rule anatomy_uberon_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/UBERON"
    run:
        anatomy.write_uberon_ids(output.outfile)

rule anatomy_cl_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/CL"
    run:
        anatomy.write_cl_ids(output.outfile)

rule anatomy_go_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/GO"
    run:
        anatomy.write_go_ids(output.outfile)

rule anatomy_ncit_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/NCIT"
    run:
        anatomy.write_ncit_ids(output.outfile)

rule anatomy_mesh_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/MESH"
    run:
        anatomy.write_mesh_ids(output.outfile)

rule anatomy_umls_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/UMLS"
    run:
        anatomy.write_umls_ids(output.outfile)

rule get_anatomy_obo_relationships:
    output:
        config['download_directory']+'/anatomy/concords/UBERON',
        config['download_directory']+'/anatomy/concords/CL',
        config['download_directory']+'/anatomy/concords/GO',
    run:
        anatomy.build_anatomy_obo_relationships(config['download_directory']+'/anatomy/concords')

rule get_anatomy_umls_relationships:
    input:
        infile=config['download_directory']+"/anatomy/ids/UMLS"
    output:
        outfile=config['download_directory']+'/anatomy/concords/UMLS',
    run:
        anatomy.build_anatomy_umls_relationships(input.infile,output.outfile)

rule anatomy_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['anatomy_prefixes']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['anatomy_prefixes']),
        concords=expand("{dd}/anatomy/concords/{ap}",dd=config['download_directory'],ap=config['anatomy_concords']),
        idlists=expand("{dd}/anatomy/ids/{ap}",dd=config['download_directory'],ap=config['anatomy_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
    run:
        anatomy.build_compendia(input.concords,input.idlists)

rule check_anatomy_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
    output:
        report_file = config['output_directory']+'/reports/anatomy_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/anatomy/ids',input.input_compendia,output.report_file)

rule check_anatomical_entity:
    input:
        infile=config['output_directory']+'/compendia/AnatomicalEntity.txt'
    output:
        outfile=config['output_directory']+'/reports/AnatomicalEntity.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_gross_anatomical_structure:
    input:
        infile=config['output_directory']+'/compendia/GrossAnatomicalStructure.txt'
    output:
        outfile=config['output_directory']+'/reports/GrossAnatomicalStructure.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_cell:
    input:
        infile=config['output_directory']+'/compendia/Cell.txt'
    output:
        outfile=config['output_directory']+'/reports/Cell.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_cellular_component:
    input:
        infile=config['output_directory']+'/compendia/CellularComponent.txt'
    output:
        outfile=config['output_directory']+'/reports/CellularComponent.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule anatomy:
    input:
        config['output_directory']+'/reports/anatomy_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['anatomy_outputs'])
    output:
        x=config['output_directory']+'/reports/anatomy_done'
    shell:
        "echo 'done' >> {output.x}"

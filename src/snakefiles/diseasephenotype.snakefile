import src.createcompendia.diseasephenotype as diseasephenotype
import src.assess_compendia as assessments

### Disease / Phenotypic Feature


# SNOMEDCT will not have an independent list
# MEDDRA will not have an independent list
# They will only have identifiers that enter via links in UMLS

rule disease_mondo_ids:
    output:
        outfile=config['download_directory']+"/disease/ids/MONDO"
    run:
        diseasephenotype.write_mondo_ids(output.outfile)

rule disease_doid_ids:
    input:
        infile=config['download_directory']+'/DOID/labels'
    output:
        outfile=config['download_directory']+"/disease/ids/DOID"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Disease\"}}' {input.infile} > {output.outfile}"

rule disease_orphanet_ids:
    input:
        infile=config['download_directory']+'/Orphanet/labels'
    output:
        outfile=config['download_directory']+"/disease/ids/Orphanet"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Disease\"}}' {input.infile} > {output.outfile}"

rule disease_efo_ids:
    output:
        outfile=config['download_directory']+"/disease/ids/EFO"
    run:
        diseasephenotype.write_efo_ids(output.outfile)

rule disease_ncit_ids:
    output:
        outfile=config['download_directory']+"/disease/ids/NCIT"
    run:
        diseasephenotype.write_ncit_ids(output.outfile)

rule disease_mesh_ids:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['download_directory']+"/disease/ids/MESH"
    run:
        diseasephenotype.write_mesh_ids(output.outfile)

rule disease_umls_ids:
    #The location of the RRFs is known to the guts, but should probably come out here.
    output:
        outfile=config['download_directory']+"/disease/ids/UMLS"
    run:
        diseasephenotype.write_umls_ids(output.outfile)

rule disease_hp_ids:
    #The location of the RRFs is known to the guts, but should probably come out here.
    output:
        outfile=config['download_directory']+"/disease/ids/HP"
    run:
        diseasephenotype.write_hp_ids(output.outfile)

#rule get_anatomy_obo_relationships:
#    output:
#        config['download_directory']+'/anatomy/concords/UBERON',
#        config['download_directory']+'/anatomy/concords/CL',
#        config['download_directory']+'/anatomy/concords/GO',
#    run:
#        anatomy.build_anatomy_obo_relationships(config['download_directory']+'/anatomy/concords')
#
#rule get_anatomy_umls_relationships:
#    input:
#        infile=config['download_directory']+"/anatomy/ids/UMLS"
#    output:
#        outfile=config['download_directory']+'/anatomy/concords/UMLS',
#    run:
#        anatomy.build_anatomy_umls_relationships(input.infile,output.outfile)
#
#rule anatomy_compendia:
#    input:
#        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['anatomy_prefixes']),
#        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['anatomy_prefixes']),
#        concords=expand("{dd}/anatomy/concords/{ap}",dd=config['download_directory'],ap=config['anatomy_concords']),
#        idlists=expand("{dd}/anatomy/ids/{ap}",dd=config['download_directory'],ap=config['anatomy_ids']),
#    output:
#        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
#        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
#    run:
#        anatomy.build_compendia(input.concords,input.idlists)
#
#rule check_anatomy_completeness:
#    input:
#        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
#    output:
#        report_file = config['output_directory']+'/reports/anatomy_completeness.txt'
#    run:
#        assessments.assess_completeness(config['download_directory']+'/anatomy/ids',input.input_compendia,output.report_file)
#
#rule check_anatomical_entity:
#    input:
#        infile=config['output_directory']+'/compendia/AnatomicalEntity.txt'
#    output:
#        outfile=config['output_directory']+'/reports/AnatomicalEntity.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule check_gross_anatomical_structure:
#    input:
#        infile=config['output_directory']+'/compendia/GrossAnatomicalStructure.txt'
#    output:
#        outfile=config['output_directory']+'/reports/GrossAnatomicalStructure.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule check_cell:
#    input:
#        infile=config['output_directory']+'/compendia/Cell.txt'
#    output:
#        outfile=config['output_directory']+'/reports/Cell.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule check_cellular_component:
#    input:
#        infile=config['output_directory']+'/compendia/CellularComponent.txt'
#    output:
#        outfile=config['output_directory']+'/reports/CellularComponent.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule anatomy:
#    input:
#        config['output_directory']+'/reports/anatomy_completeness.txt',
#        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['anatomy_outputs'])
#    output:
#        x=config['output_directory']+'/reports/anatomy_done'
#    shell:
#        "echo 'done' >> {output.x}"
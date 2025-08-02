import src.createcompendia.anatomy as anatomy
import src.assess_compendia as assessments
import src.snakefiles.util as util

### AnatomicalEntity / Cell / CellularComponent

rule anatomy_uberon_ids:
    output:
        outfile=config['intermediate_directory']+"/anatomy/ids/UBERON"
    run:
        anatomy.write_uberon_ids(output.outfile)

rule anatomy_cl_ids:
    output:
        outfile=config['intermediate_directory']+"/anatomy/ids/CL"
    run:
        anatomy.write_cl_ids(output.outfile)

rule anatomy_go_ids:
    output:
        outfile=config['intermediate_directory']+"/anatomy/ids/GO"
    run:
        anatomy.write_go_ids(output.outfile)

rule anatomy_ncit_ids:
    output:
        outfile=config['intermediate_directory']+"/anatomy/ids/NCIT"
    run:
        anatomy.write_ncit_ids(output.outfile)

rule anatomy_mesh_ids:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['intermediate_directory']+"/anatomy/ids/MESH"
    run:
        anatomy.write_mesh_ids(output.outfile)

rule anatomy_umls_ids:
    input:
        mrsty=config['download_directory'] + "/UMLS/MRSTY.RRF"
    output:
        outfile=config['intermediate_directory']+"/anatomy/ids/UMLS"
    run:
        anatomy.write_umls_ids(input.mrsty, output.outfile)

rule get_anatomy_obo_relationships:
    output:
        config['intermediate_directory']+'/anatomy/concords/UBERON',
        config['intermediate_directory']+'/anatomy/concords/CL',
        config['intermediate_directory']+'/anatomy/concords/GO',
    run:
        anatomy.build_anatomy_obo_relationships(config['intermediate_directory']+'/anatomy/concords')

rule get_wikidata_cell_relationships:
    output:
        config['intermediate_directory']+'/anatomy/concords/WIKIDATA',
    run:
        anatomy.build_wikidata_cell_relationships(config['intermediate_directory']+'/anatomy/concords')

rule get_anatomy_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/anatomy/ids/UMLS"
    output:
        outfile=config['intermediate_directory']+'/anatomy/concords/UMLS',
    run:
        anatomy.build_anatomy_umls_relationships(input.mrconso, input.infile, output.outfile)

rule anatomy_compendia:
    input:
        labels=os.path.join(config["download_directory"], 'common', config["common"]["labels"][0]),
        synonyms=os.path.join(config["download_directory"], 'common', config["common"]["synonyms"][0]),
        concords=expand("{dd}/anatomy/concords/{ap}",dd=config['intermediate_directory'],ap=config['anatomy_concords']),
        idlists=expand("{dd}/anatomy/ids/{ap}",dd=config['intermediate_directory'],ap=config['anatomy_ids']),
        icrdf_filename=config['download_directory']+'/icRDF.tsv',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        temp(expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']))
    run:
        anatomy.build_compendia(input.concords, input.idlists, input.icrdf_filename)

rule check_anatomy_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
    output:
        report_file = config['output_directory']+'/reports/anatomy_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/anatomy/ids',input.input_compendia,output.report_file)

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
        synonyms=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['anatomy_outputs'])
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od = config['output_directory'], ap = config['anatomy_outputs']),
        x=config['output_directory']+'/reports/anatomy_done'
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)

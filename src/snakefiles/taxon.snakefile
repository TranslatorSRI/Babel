import src.createcompendia.taxon as taxon
import src.assess_compendia as assessments

rule taxon_ncbi_ids:
    input:
        infile=config['download_directory']+'/NCBITaxon/labels'
    output:
        outfile=config['intermediate_directory']+'/taxon/ids/NCBITaxon'
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:OrganismTaxon\"}}' {input.infile} > {output.outfile}"

rule taxon_mesh_ids:
    input:
        infile=config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['intermediate_directory']+"/taxon/ids/MESH"
    run:
        taxon.write_mesh_ids(output.outfile)

rule taxon_umls_ids:
    input:
        mrsty=config['download_directory'] + "/UMLS/MRSTY.RRF"
    output:
        outfile=config['intermediate_directory']+"/taxon/ids/UMLS"
    run:
        taxon.write_umls_ids(input.mrsty, output.outfile)

rule get_taxon_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/taxon/ids/UMLS"
    output:
        outfile=config['intermediate_directory']+'/taxon/concords/UMLS',
    run:
        taxon.build_taxon_umls_relationships(input.mrconso, input.infile, output.outfile)

rule get_taxon_relationships:
    input:
        meshfile=config['download_directory']+"/MESH/mesh.nt",
        meshids=config['intermediate_directory']+"/taxon/ids/MESH",
    output:
        outfile=config['intermediate_directory']+'/taxon/concords/NCBI_MESH'
    run:
        taxon.build_relationships(output.outfile,input.meshids)

rule taxon_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['taxon_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['taxon_synonyms']),
        concords=expand("{dd}/taxon/concords/{ap}",dd=config['intermediate_directory'],ap=config['taxon_concords']),
        idlists=expand("{dd}/taxon/ids/{ap}",dd=config['intermediate_directory'],ap=config['taxon_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['taxon_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['taxon_outputs'])
    run:
        taxon.build_compendia(input.concords,input.idlists)

rule check_taxon_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['taxon_outputs'])
    output:
        report_file = config['output_directory']+'/reports/taxon_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/taxon/ids',input.input_compendia,output.report_file)

rule check_taxon:
    input:
        infile=config['output_directory']+'/compendia/OrganismTaxon.txt'
    output:
        outfile=config['output_directory']+'/reports/OrganismTaxon.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule taxon:
    input:
        config['output_directory']+'/reports/taxon_completeness.txt',
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['taxon_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['taxon_outputs'])
    output:
        x=config['output_directory']+'/reports/taxon_done'
    shell:
        "echo 'done' >> {output.x}"

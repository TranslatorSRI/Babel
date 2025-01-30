import src.createcompendia.processactivitypathway as pap
import src.assess_compendia as assessments
import src.snakefiles.util as util

### Process / Activity / Pathway

rule process_go_ids:
    output:
        outfile=config['intermediate_directory']+"/process/ids/GO"
    run:
        pap.write_go_ids(output.outfile)

rule process_reactome_ids:
    input:
        infile=config['download_directory']+'/REACT/Events.json'
    output:
        outfile=config['intermediate_directory']+"/process/ids/REACT"
    run:
        pap.write_react_ids(input.infile,output.outfile)

rule process_rhea_ids:
    input:
        infile=config['download_directory']+'/RHEA/labels'
    output:
        outfile=config['intermediate_directory']+"/process/ids/RHEA"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:MolecularActivity\"}}' {input.infile} > {output.outfile}"

rule process_ec_ids:
    output:
        outfile=config['intermediate_directory']+"/process/ids/EC"
    run:
        pap.write_ec_ids(output.outfile)

rule process_smpdb_ids:
    input:
        infile=config['download_directory']+'/SMPDB/labels'
    output:
        outfile=config['intermediate_directory']+"/process/ids/SMPDB"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Pathway\"}}' {input.infile} > {output.outfile}"

rule process_panther_ids:
    input:
        infile=config['download_directory']+'/PANTHER.PATHWAY/labels'
    output:
        outfile=config['intermediate_directory']+"/process/ids/PANTHER.PATHWAY"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Pathway\"}}' {input.infile} > {output.outfile}"

rule process_umls_ids:
    input:
        mrsty=config['download_directory'] + "/UMLS/MRSTY.RRF"
    output:
        outfile=config['intermediate_directory']+"/process/ids/UMLS"
    run:
        pap.write_umls_ids(input.mrsty, output.outfile)

### Concords

rule get_process_go_relationships:
    output:
        config['intermediate_directory']+'/process/concords/GO',
    run:
        pap.build_process_obo_relationships(config['intermediate_directory']+'/process/concords')

rule get_process_rhea_relationships:
    input:
        infile=config['download_directory']+"/RHEA/rhea.rdf",
    output:
        outfile=config['intermediate_directory']+'/process/concords/RHEA',
    run:
        pap.build_process_rhea_relationships(output.outfile)


rule get_process_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/process/ids/UMLS",
    output:
        outfile=config['intermediate_directory']+'/process/concords/UMLS',
    run:
        pap.build_process_umls_relationships(input.mrconso, input.infile, output.outfile)

rule process_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['process_labels']),
        #synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['process_labelsandsynonyms']),
        concords=expand("{dd}/process/concords/{ap}",dd=config['intermediate_directory'],ap=config['process_concords']),
        idlists=expand("{dd}/process/ids/{ap}",dd=config['intermediate_directory'],ap=config['process_ids']),
        icrdf_filename=config['download_directory']+'/icRDF.tsv',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['process_outputs']),
        temp(expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['process_outputs']))
    run:
        pap.build_compendia(input.concords,input.idlists,input.icrdf_filename)

rule check_process_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['process_outputs'])
    output:
        report_file = config['output_directory']+'/reports/process_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/process/ids',input.input_compendia,output.report_file)

rule check_process:
    input:
        infile=config['output_directory']+'/compendia/BiologicalProcess.txt'
    output:
        outfile=config['output_directory']+'/reports/BiologicalProcess.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_activity:
    input:
        infile=config['output_directory']+'/compendia/MolecularActivity.txt'
    output:
        outfile=config['output_directory']+'/reports/MolecularActivity.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_pathway:
    input:
        infile=config['output_directory']+'/compendia/Pathway.txt'
    output:
        outfile=config['output_directory']+'/reports/Pathway.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule process:
    input:
        config['output_directory']+'/reports/process_completeness.txt',
        synonyms=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['process_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['process_outputs'])
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['process_outputs']),
        x=config['output_directory']+'/reports/process_done'
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)
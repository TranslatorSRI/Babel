import src.createcompendia.genefamily as genefamily
import src.assess_compendia as assessments
import src.snakefiles.util as util

rule genefamily_pantherfamily_ids:
    input:
        infile=config['download_directory']+'/PANTHER.FAMILY/labels'
    output:
        outfile=config['intermediate_directory']+'/genefamily/ids/PANTHER.FAMILY'
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:GeneFamily\"}}' {input.infile} > {output.outfile}"

rule genefamily_hgncfamily_ids:
    input:
        infile=config['download_directory']+'/HGNC.FAMILY/labels'
    output:
        outfile=config['intermediate_directory']+"/genefamily/ids/HGNC.FAMILY"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:GeneFamily\"}}' {input.infile} > {output.outfile}"

rule genefamily_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['genefamily_labels']),
        idlists=expand("{dd}/genefamily/ids/{ap}",dd=config['intermediate_directory'],ap=config['genefamily_ids']),
        metadata_yamls=expand("{dd}/{ap}/metadata.yaml",dd=config['download_directory'],ap=config['genefamily_labels']),
        icrdf_filename=config['download_directory'] + '/icRDF.tsv',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['genefamily_outputs']),
        temp(expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['genefamily_outputs']))
    run:
        genefamily.build_compendia(input.idlists, input.metadata_yamls, input.icrdf_filename)

rule check_genefamily_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['genefamily_outputs'])
    output:
        report_file = config['output_directory']+'/reports/genefamily_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/genefamily/ids',input.input_compendia,output.report_file)

rule check_genefamily:
    input:
        infile=config['output_directory']+'/compendia/GeneFamily.txt'
    output:
        outfile=config['output_directory']+'/reports/GeneFamily.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule genefamily:
    input:
        config['output_directory']+'/reports/genefamily_completeness.txt',
        synonyms=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['genefamily_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['genefamily_outputs'])
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od = config['output_directory'], ap = config['genefamily_outputs']),
        x=config['output_directory']+'/reports/genefamily_done'
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)

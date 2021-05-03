import src.createcompendia.protein as protein
import src.assess_compendia as assessments

### Gene / Protein

rule protein_pr_ids:
    output:
        outfile=config['download_directory']+"/protein/ids/PR"
    run:
        protein.write_pr_ids(output.outfile)

rule protein_uniprotkb_ids:
    input:
        infile=config['download_directory']+'/UniProtKB/labels'
    output:
        outfile=config['download_directory']+"/protein/ids/UniProtKB"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1}}' {input.infile} > {output.outfile}"

rule protein_umls_ids:
    output:
        outfile=config['download_directory']+"/protein/ids/UMLS"
    run:
        protein.write_umls_ids(output.outfile)

rule protein_ensembl_ids:
    input:
        infile=config['download_directory']+'/ENSEMBL/BioMartDownloadComplete'
    output:
        outfile=config['download_directory']+"/protein/ids/ENSEMBL"
    run:
        protein.write_ensembl_ids(config['download_directory'] + '/ENSEMBL',output.outfile)

rule get_protein_uniprotkb_ensembl_relationships:
    input:
        infile = config['download_directory'] + '/UniProtKB/idmapping.dat'
    output:
        outfile = config['download_directory'] + '/protein/concords/UniProtKB'
    run:
        protein.build_protein_uniprotkb_ensemble_relationships(input.infile,output.outfile)

rule get_protein_pr_uniprotkb_relationships:
    output:
        outfile  = config['download_directory'] + '/protein/concords/PR'
    run:
        protein.build_pr_uniprot_relationships(output.outfile)

rule protein_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['gene_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['gene_labels']),
        concords=expand("{dd}/gene/concords/{ap}",dd=config['download_directory'],ap=config['gene_concords']),
        idlists=expand("{dd}/gene/ids/{ap}",dd=config['download_directory'],ap=config['gene_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['gene_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['gene_outputs'])
    run:
        protein.build_protein_compendia(input.concords,input.idlists)

rule check_protein_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['gene_outputs'])
    output:
        report_file = config['output_directory']+'/reports/protein_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/protein/ids',input.input_compendia,output.report_file)

rule check_protein:
    input:
        infile=config['output_directory']+'/compendia/Protein.txt'
    output:
        outfile=config['output_directory']+'/reports/Protein.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule protein:
    input:
        config['output_directory']+'/reports/protein_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['protein_outputs'])
    output:
        x=config['output_directory']+'/reports/protein_done'
    shell:
        "echo 'done' >> {output.x}"
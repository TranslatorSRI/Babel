import src.createcompendia.gene as gene
import src.assess_compendia as assessments

rule gene_ncbi_ids:
    input:
        infile=config['download_directory']+'/NCBIGene/labels'
    output:
        outfile=config['download_directory']+"/gene/ids/NCBIGene"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1}}' {input.infile} > {output.outfile}"

rule gene_omim_ids:
    input:
        infile=config['download_directory']+'/OMIM/mim2gene.txt'
    output:
        outfile=config['download_directory']+"/gene/ids/OMIM"
    run:
        gene.write_omim_ids(input.infile,output.outfile)

rule gene_ensembl_ids:
    input:
        infile=config['download_directory']+'/ENSEMBL/BioMartDownloadComplete'
    output:
        outfile=config['download_directory']+"/gene/ids/ENSEMBL"
    run:
        gene.write_ensembl_ids(config['download_directory'] + '/ENSEMBL',output.outfile)

rule gene_hgnc_ids:
    input:
        infile=config['download_directory']+"/HGNC/hgnc_complete_set.json"
    output:
        outfile=config['download_directory']+"/gene/ids/HGNC"
    run:
        gene.write_hgnc_ids(input.infile,output.outfile)

rule gene_umls_ids:
    output:
        outfile=config['download_directory']+"/gene/ids/UMLS"
    run:
        gene.write_umls_ids(output.outfile)

rule get_gene_ncbigene_ensembl_relationships:
    input:
        infile=config['download_directory']+"/NCBIGene/gene2ensembl.gz",
        idfile=config['download_directory'] + "/gene/ids/NCBIGene"
    output:
        outfile=config['download_directory']+'/gene/concords/NCBIGeneENSEMBL'
    run:
        gene.build_gene_ncbi_ensembl_relationships(input.infile,input.idfile,output.outfile)

rule get_gene_ncbigene_relationships:
    input:
        infile=config['download_directory']+"/NCBIGene/gene_info.gz",
        idfile=config['download_directory']+"/gene/ids/NCBIGene"
    output:
        outfile=config['download_directory']+'/gene/concords/NCBIGene'
    run:
        gene.build_gene_ncbigene_xrefs(input.infile,input.idfile,output.outfile)

rule get_gene_ensembl_relationships:
    input:
        infile =config['download_directory'] + '/ENSEMBL/BioMartDownloadComplete'
    output:
        outfile=config['download_directory']+'/gene/concords/ENSEMBL'
    run:
        gene.build_gene_ensembl_relationships(config['download_directory']+'/ENSEMBL',output.outfile)


rule get_gene_medgen_relationships:
    input:
        infile=config['download_directory']+'/NCBIGene/mim2gene_medgen'
    output:
        outfile=config['download_directory']+'/gene/concords/medgen'
    run:
        gene.build_gene_medgen_relationships(input.infile, output.outfile)

rule get_gene_umls_relationships:
    input:
        infile=config['download_directory']+'/gene/ids/UMLS'
    output:
        outfile=config['download_directory']+'/gene/concords/UMLS'
    run:
        gene.build_gene_umls_hgnc_relationships(input.infile, output.outfile)

rule gene_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['gene_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['gene_labels']),
        concords=expand("{dd}/gene/concords/{ap}",dd=config['download_directory'],ap=config['gene_concords']),
        idlists=expand("{dd}/gene/ids/{ap}",dd=config['download_directory'],ap=config['gene_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['gene_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['gene_outputs'])
    run:
        gene.build_gene_compendia(input.concords,input.idlists)

rule check_gene_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['gene_outputs'])
    output:
        report_file = config['output_directory']+'/reports/gene_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/gene/ids',input.input_compendia,output.report_file)

rule check_gene:
    input:
        infile=config['output_directory']+'/compendia/Gene.txt'
    output:
        outfile=config['output_directory']+'/reports/Gene.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule gene:
    input:
        config['output_directory']+'/reports/gene_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['gene_outputs'])
    output:
        x=config['output_directory']+'/reports/gene_done'
    shell:
        "echo 'done' >> {output.x}"

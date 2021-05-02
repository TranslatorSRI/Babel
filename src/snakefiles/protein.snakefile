import src.createcompendia.protein as protein

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
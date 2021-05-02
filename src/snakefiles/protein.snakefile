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

import src.createcompendia.protein as geneprotein
import src.assess_compendia as assessments

### Gene / Protein

rule protein_pr_ids:
    output:
        outfile=config['download_directory']+"/protein/ids/PR"
    run:
        geneprotein.write_pr_ids(output.outfile)

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
        geneprotein.write_protein_umls_ids(output.outfile)

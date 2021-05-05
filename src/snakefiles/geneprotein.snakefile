import src.createcompendia.geneprotein as geneprotein
import src.assess_compendia as assessments

### Gene / Protein

rule geneprotein_uniprot_relationships:
    input:
        infile = config['download_directory'] + '/UniProtKB/idmapping.dat'
    output:
        outfile_concords = config['download_directory'] + '/geneprotein/concords/UniProtNCBI'
    run:
        geneprotein.build_uniprotkb_ncbigene_relationships(input.infile,output.outfile_concords)

rule geneprotein_compendia:
    input:
        gene_compendium=config['output_directory']+'/compendia/'+'Gene.txt',
        protein_compendium=config['output_directory']+'/compendia/'+'Protein.txt',
        geneprotein_concord=config['download_directory']+'/geneprotein/concords/UniProtNCBI'
    output:
        outfile=config['output_directory']+'/compendia/'+config['geneprotein_outputs']
    run:
        geneprotein.build_compendium(input.gene_compendium,input.protein_compendium,input.geneprotein_concord,output.outfile)

rule check_geneprotein_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['geneprotein_outputs'])
    output:
        report_file = config['output_directory']+'/reports/geneprotein_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/protein/ids',input.input_compendia,output.report_file)

rule check_geneprotein:
    input:
        infile=config['output_directory']+'/compendia/GeneProtein.txt'
    output:
        outfile=config['output_directory']+'/reports/GeneProtein.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule geneprotein:
    input:
        config['output_directory']+'/reports/geneprotein_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['geneprotein_outputs'])
    output:
        x=config['output_directory']+'/reports/geneprotein_done'
    shell:
        "echo 'done' >> {output.x}"
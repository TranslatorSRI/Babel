import src.createcompendia.geneprotein as geneprotein
import src.assess_compendia as assessments

### Gene / Protein

rule geneprotein_uniprot_relationships:
    input:
        infile = config['download_directory'] + '/UniProtKB/idmapping.dat'
    output:
        outfile_concords = config['intermediate_directory'] + '/geneprotein/concords/UniProtNCBI',
        metadata_yaml = config['intermediate_directory'] + '/geneprotein/concords/metadata-UniProtNCBI.yaml'
    run:
        geneprotein.build_uniprotkb_ncbigene_relationships(input.infile,output.outfile_concords)

rule geneprotein_conflation:
    input:
        gene_compendium=config['output_directory']+'/compendia/'+'Gene.txt',
        protein_compendium=config['output_directory']+'/compendia/'+'Protein.txt',
        geneprotein_concord=config['intermediate_directory']+'/geneprotein/concords/UniProtNCBI'
    output:
        outfile=config['output_directory']+'/conflation/GeneProtein.txt'
    run:
        geneprotein.build_conflation(input.geneprotein_concord,input.gene_compendium,input.protein_compendium,output.outfile)

rule geneprotein:
    input:
        config['output_directory']+'/conflation/GeneProtein.txt'
    output:
        x=config['output_directory']+'/reports/geneprotein_done'
    shell:
        "echo 'done' >> {output.x}"

#rule check_geneprotein_completeness:
#    input:
#        input_compendia=[config['output_directory']+'/compendia/GeneProtein.txt']
#    output:
#        report_file = config['output_directory']+'/reports/geneprotein_completeness.txt'
#    run:
#        assessments.assess_completeness(config['intermediate_directory']+'/protein/ids',input.input_compendia,output.report_file)
#
#rule check_geneprotein:
#    input:
#        infile=config['output_directory']+'/compendia/GeneProtein.txt'
#    output:
#        outfile=config['output_directory']+'/reports/GeneProtein.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)

#rule geneprotein:
#    input:
#        config['output_directory']+'/reports/geneprotein_completeness.txt',
#        reports = expand("{od}/reports/GeneProtein.txt",od=config['output_directory'])
#    output:
#        x=config['output_directory']+'/reports/geneprotein_done'

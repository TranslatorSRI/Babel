import src.createcompendia.geneprotein as geneprotein
import src.assess_compendia as assessments

### Gene / Protein

rule geneprotein_uniprot_relationships:
    input:
        infile = config['download_directory'] + '/UniProtKB/idmapping.dat'
    output:
        outfile_concords = config['intermediate_directory'] + '/geneprotein/concords/UniProtNCBI'
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

rule geneprotein_conflated_synonyms:
    input:
        geneprotein_conflation=config['output_directory']+'/conflation/GeneProtein.txt',
        gene_outputs=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['gene_outputs']),
        protein_outputs=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['protein_outputs']),
    output:
        geneprotein_conflated_synonyms=config['output_directory']+'/synonyms/GeneProteinConflated.txt'
    run:
        synonymconflation.conflate_synonyms(input.gene_outputs + input.protein_outputs, input.geneprotein_conflation, output=geneprotein_conflated_synonyms)

rule geneprotein:
    input:
        config['output_directory']+'/conflation/GeneProtein.txt',
        config['output_directory']+'/synonyms/GeneProteinConflated.txt'
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

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
        gene_labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['gene_labels']),
        protein_labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['protein_labels']),
        gene_synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['gene_labels']),
        protein_synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['protein_synonyms']),
        gene_concords=expand("{dd}/gene/concords/{ap}",dd=config['download_directory'],ap=config['gene_concords']),
        protein_concords=expand("{dd}/protein/concords/{ap}",dd=config['download_directory'],ap=config['protein_concords']),
        geneprotein_concords=[config['download_directory']+'/geneprotein/concords/UniProtNCBI'],
        gene_idlists=expand("{dd}/gene/ids/{ap}",dd=config['download_directory'],ap=config['gene_ids']),
        protein_idlists=expand("{dd}/protein/ids/{ap}",dd=config['download_directory'],ap=config['protein_ids'])
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['geneprotein_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['geneprotein_outputs'])
    run:
        geneprotein.build_compendium(input.gene_concords+input.protein_concords+input.geneprotein_concords,
            input.gene_idlists+input.protein_idlists)

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
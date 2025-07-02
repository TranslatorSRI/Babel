import src.createcompendia.protein as protein
import src.assess_compendia as assessments
#import src.filter_compendia as filter
import src.snakefiles.util as util

### Gene / Protein

rule protein_pr_ids:
    output:
        outfile=config['intermediate_directory']+"/protein/ids/PR"
    run:
        protein.write_pr_ids(output.outfile)

rule protein_uniprotkb_ids:
    input:
        infile=config['download_directory']+'/UniProtKB/labels'
    output:
        outfile=config['intermediate_directory']+"/protein/ids/UniProtKB"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1}}' {input.infile} > {output.outfile}"

rule extract_taxon_ids_from_uniprotkb:
    input:
        infile=config['download_directory']+'/UniProtKB/idmapping.dat'
    output:
        outfile=config['download_directory']+'/UniProtKB/taxa'
    run:
        protein.extract_taxon_ids_from_uniprotkb(input.infile, output.outfile)

rule protein_umls_ids:
    input:
        mrsty=config['download_directory']+"/UMLS/MRSTY.RRF"
    output:
        outfile=config['intermediate_directory']+"/protein/ids/UMLS"
    run:
        protein.write_umls_ids(input.mrsty, output.outfile)

rule protein_ensembl_ids:
    input:
        infile=config['download_directory']+'/ENSEMBL/BioMartDownloadComplete'
    output:
        outfile=config['intermediate_directory']+"/protein/ids/ENSEMBL"
    run:
        protein.write_ensembl_ids(config['download_directory'] + '/ENSEMBL',output.outfile)

rule get_protein_uniprotkb_ensembl_relationships:
    input:
        infile = config['download_directory'] + '/UniProtKB/idmapping.dat'
    output:
        outfile = config['intermediate_directory'] + '/protein/concords/UniProtKB',
        metadata_yaml = config['intermediate_directory'] + '/protein/concords/metadata-UniProtKB.yaml',
    run:
        protein.build_protein_uniprotkb_ensemble_relationships(input.infile,output.outfile, output.metadata_yaml)

rule get_protein_pr_uniprotkb_relationships:
    output:
        outfile  = config['intermediate_directory'] + '/protein/concords/PR',
        metadata_yaml = config['intermediate_directory'] + '/protein/concords/metadata-PR.yaml'
    run:
        protein.build_pr_uniprot_relationships(output.outfile, output.metadata_yaml)

rule get_protein_ncit_uniprotkb_relationships:
    input:
        infile = config['download_directory'] + '/NCIT/NCIt-SwissProt_Mapping.txt'
    output:
        outfile  = config['intermediate_directory'] + '/protein/concords/NCIT_UniProtKB',
        metadata_yaml = config['intermediate_directory'] + '/protein/concords/metadata-NCIT_UniProtKB.yaml',
    run:
        protein.build_ncit_uniprot_relationships(input.infile, output.outfile, output.metadata_yaml)

rule get_protein_ncit_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/protein/ids/UMLS"
    output:
        outfile=config['intermediate_directory']+'/protein/concords/NCIT_UMLS',
        metadata_yaml=config['intermediate_directory']+'/protein/concords/metadata-NCIT_UMLS.yaml'
    run:
        protein.build_umls_ncit_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)

rule protein_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['protein_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['protein_synonyms']),
        concords=expand("{dd}/protein/concords/{ap}",dd=config['intermediate_directory'],ap=config['protein_concords']),
        metadata_yamls=expand("{dd}/protein/concords/metadata-{ap}.yaml",dd=config['intermediate_directory'],ap=config['protein_concords']),
        idlists=expand("{dd}/protein/ids/{ap}",dd=config['intermediate_directory'],ap=config['protein_ids']),
        icrdf_filename=config['download_directory'] + '/icRDF.tsv',
        # Include the taxon information from UniProtKB
        uniprotkb_taxa_file=config['download_directory']+'/UniProtKB/taxa',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['protein_outputs']),
        temp(expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['protein_outputs']))
    run:
        protein.build_protein_compendia(input.concords, input.metadata_yamls, input.idlists, input.icrdf_filename)

rule check_protein_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['protein_outputs'])
    output:
        report_file = config['output_directory']+'/reports/protein_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/protein/ids',input.input_compendia,output.report_file)

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
        synonyms=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['protein_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['protein_outputs'])
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od = config['output_directory'], ap = config['protein_outputs']),
        x=config['output_directory']+'/reports/protein_done'
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)

#
#rule filter_protein:
#    input:
#        full=config['output_directory'] + '/compendia/Protein.txt'
#    output:
#        filtered=config['output_directory'] + '/compendia/Protein_filtered.txt'
#    run:
#        filter.filter_compendium(input.full,output.filtered)

import src.createcompendia.diseasephenotype as diseasephenotype
import src.assess_compendia as assessments

### Disease / Phenotypic Feature


# SNOMEDCT will not have an independent list
# MEDDRA will not have an independent list
# They will only have identifiers that enter via links in UMLS

rule disease_mondo_ids:
    output:
        outfile=config['intermediate_directory']+"/disease/ids/MONDO"
    run:
        diseasephenotype.write_mondo_ids(output.outfile)

rule disease_doid_ids:
    input:
        infile=config['download_directory']+'/DOID/labels'
    output:
        outfile=config['intermediate_directory']+"/disease/ids/DOID"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Disease\"}}' {input.infile} > {output.outfile}"

rule disease_orphanet_ids:
    input:
        infile=config['download_directory']+'/Orphanet/labels'
    output:
        outfile=config['intermediate_directory']+"/disease/ids/Orphanet"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Disease\"}}' {input.infile} > {output.outfile}"

rule disease_efo_ids:
    output:
        outfile=config['intermediate_directory']+"/disease/ids/EFO"
    run:
        diseasephenotype.write_efo_ids(output.outfile)

rule disease_ncit_ids:
    output:
        outfile=config['intermediate_directory']+"/disease/ids/NCIT"
    run:
        diseasephenotype.write_ncit_ids(output.outfile)

rule disease_mesh_ids:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['intermediate_directory']+"/disease/ids/MESH"
    run:
        diseasephenotype.write_mesh_ids(output.outfile)

rule disease_umls_ids:
    input:
        badumls = config['input_directory']+"/badumls",
        mrsty = config['download_directory'] + "/UMLS/MRSTY.RRF"
    output:
        outfile=config['intermediate_directory']+"/disease/ids/UMLS"
    run:
        diseasephenotype.write_umls_ids(input.mrsty, output.outfile, input.badumls)

rule disease_hp_ids:
    #The location of the RRFs is known to the guts, but should probably come out here.
    output:
        outfile=config['intermediate_directory']+"/disease/ids/HP"
    run:
        diseasephenotype.write_hp_ids(output.outfile)

rule disease_omim_ids:
    input:
        infile=config['download_directory']+"/OMIM/mim2gene.txt"
    output:
        outfile=config['intermediate_directory']+"/disease/ids/OMIM"
    run:
        diseasephenotype.write_omim_ids(input.infile,output.outfile)

### Concords

rule get_disease_obo_relationships:
    output:
        config['intermediate_directory']+'/disease/concords/MONDO',
        config['intermediate_directory']+'/disease/concords/MONDO_close',
        config['intermediate_directory']+'/disease/concords/HP',
    run:
        diseasephenotype.build_disease_obo_relationships(config['intermediate_directory']+'/disease/concords')

rule get_disease_efo_relationships:
    input:
        infile=config['intermediate_directory']+"/disease/ids/EFO",
    output:
        outfile=config['intermediate_directory']+'/disease/concords/EFO'
    run:
        diseasephenotype.build_disease_efo_relationships(input.infile,output.outfile)

rule get_disease_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/disease/ids/UMLS",
        omim=config['intermediate_directory']+'/disease/ids/OMIM',
        ncit=config['intermediate_directory'] + '/disease/ids/NCIT'
    output:
        outfile=config['intermediate_directory']+'/disease/concords/UMLS',
    run:
        diseasephenotype.build_disease_umls_relationships(input.mrconso, input.infile,output.outfile,input.omim,input.ncit)

rule get_disease_doid_relationships:
    input:
        infile = config['download_directory']+'/DOID/doid.json'
    output:
        outfile=config['intermediate_directory']+'/disease/concords/DOID',
    run:
        diseasephenotype.build_disease_doid_relationships(input.infile,output.outfile)

rule disease_compendia:
    input:
        bad_hpo_xrefs = "input_data/badHPx.txt",
        bad_mondo_xrefs = "input_data/mondo_badxrefs.txt",
        bad_umls_xrefs = "input_data/umls_badxrefs.txt",
        close_matches = config['intermediate_directory']+"/disease/concords/MONDO_close",
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['disease_labelsandsynonyms']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['disease_labelsandsynonyms']),
        concords=expand("{dd}/disease/concords/{ap}",dd=config['intermediate_directory'],ap=config['disease_concords']),
        idlists=expand("{dd}/disease/ids/{ap}",dd=config['intermediate_directory'],ap=config['disease_ids']),
        icrdf_filename = config['download_directory'] + '/icRDF.tsv',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['disease_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['disease_outputs'])
    run:
        diseasephenotype.build_compendium(input.concords,input.idlists,input.close_matches,{'HP':input.bad_hpo_xrefs,
                                                                        'MONDO':input.bad_mondo_xrefs,
                                                                        'UMLS':input.bad_umls_xrefs}, input.icrdf_filename )

rule check_disease_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['disease_outputs'])
    output:
        report_file = config['output_directory']+'/reports/disease_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/disease/ids',input.input_compendia,output.report_file)

rule check_disease:
    input:
        infile=config['output_directory']+'/compendia/Disease.txt'
    output:
        outfile=config['output_directory']+'/reports/Disease.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_phenotypic_feature:
    input:
        infile=config['output_directory']+'/compendia/PhenotypicFeature.txt'
    output:
        outfile=config['output_directory']+'/reports/PhenotypicFeature.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule disease:
    input:
        config['output_directory']+'/reports/disease_completeness.txt',
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['disease_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['disease_outputs'])
    output:
        x=config['output_directory']+'/reports/disease_done'
    shell:
        "echo 'done' >> {output.x}"
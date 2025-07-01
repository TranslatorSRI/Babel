import src.createcompendia.diseasephenotype as diseasephenotype
import src.assess_compendia as assessments
import src.snakefiles.util as util
from src.metadata.provenance import write_concord_metadata

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
        mondo_metadata_yaml=config['intermediate_directory']+'/disease/concords/metadata-MONDO.yaml',
        mondo_close_metadata_yaml=config['intermediate_directory']+'/disease/concords/metadata-MONDO_close.yaml',
        hp_metadata_yaml=config['intermediate_directory']+'/disease/concords/metadata-HP.yaml',
    run:
        diseasephenotype.build_disease_obo_relationships(config['intermediate_directory']+'/disease/concords', {
            'MONDO': output.mondo_metadata_yaml,
            'MONDO_close': output.mondo_close_metadata_yaml,
            'HP': output.hp_metadata_yaml,
        })

rule get_disease_efo_relationships:
    input:
        infile=config['intermediate_directory']+"/disease/ids/EFO",
    output:
        outfile=config['intermediate_directory']+'/disease/concords/EFO',
        metadata_yaml=config['intermediate_directory']+'/disease/concords/metadata-EFO.yaml',
    run:
        diseasephenotype.build_disease_efo_relationships(input.infile,output.outfile, output.metadata_yaml)

rule get_disease_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/disease/ids/UMLS",
        omim=config['intermediate_directory']+'/disease/ids/OMIM',
        ncit=config['intermediate_directory'] + '/disease/ids/NCIT'
    output:
        outfile=config['intermediate_directory']+'/disease/concords/UMLS',
        metadata_yaml=config['intermediate_directory']+'/disease/concords/metadata-UMLS.yaml',
    run:
        diseasephenotype.build_disease_umls_relationships(input.mrconso, input.infile,output.outfile,input.omim,input.ncit, output.metadata_yaml)

rule get_disease_doid_relationships:
    input:
        infile = config['download_directory']+'/DOID/doid.json'
    output:
        outfile=config['intermediate_directory']+'/disease/concords/DOID',
        metadata_yaml=config['intermediate_directory']+'/disease/concords/metadata-DOID.yaml',
    run:
        diseasephenotype.build_disease_doid_relationships(input.infile,output.outfile,output.metadata_yaml)

rule disease_manual_concord:
    input:
        infile = 'input_data/manual_concords/disease.txt'
    output:
        outfile = config['intermediate_directory']+'/disease/concords/Manual',
        metadata_yaml = config['intermediate_directory']+'/disease/concords/metadata-Manual.yaml'
    run:
        count_manual_concords = 0
        with open(input.infile, 'r') as inp, open(output.outfile, 'w') as outp:
            for line in inp:
                # Remove any lines starting with '#', which we treat as comments.
                lstripped_line = line.lstrip()
                if lstripped_line == '' or lstripped_line.startswith('#'):
                    continue
                # Make sure the line has three tab-delimited values, and fail otherwise.
                elements = lstripped_line.split('\t')
                if len(elements) != 3:
                    raise RuntimeError(f"Found {len(elements)} elements on line {lstripped_line}, expected 3: {elements}")
                outp.writelines(["\t".join(elements)])
                count_manual_concords += 1

        write_concord_metadata(
            output.metadata_yaml,
            name='Manual Disease/Phenotype Concords',
            description='Manually curated Disease/Phenotype cross-references from the Babel repository',
            sources=[{
                'name': 'Babel repository',
                'url': 'https://github.com/TranslatorSRI/Babel',
            }],
            url='https://github.com/TranslatorSRI/Babel/blob/master/input_data/manual_concords/disease.txt',
            counts={
                'concords': count_manual_concords,
            },
        )

rule disease_compendia:
    input:
        bad_hpo_xrefs = "input_data/badHPx.txt",
        bad_mondo_xrefs = "input_data/mondo_badxrefs.txt",
        bad_umls_xrefs = "input_data/umls_badxrefs.txt",
        close_matches = config['intermediate_directory']+"/disease/concords/MONDO_close",
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['disease_labelsandsynonyms']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['disease_labelsandsynonyms']),
        concords=expand("{dd}/disease/concords/{ap}",dd=config['intermediate_directory'],ap=config['disease_concords']),
        metadata_yamls=expand("{dd}/disease/concords/metadata-{ap}.yaml",dd=config['intermediate_directory'],ap=config['disease_concords']),
        idlists=expand("{dd}/disease/ids/{ap}",dd=config['intermediate_directory'],ap=config['disease_ids']),
        icrdf_filename = config['download_directory'] + '/icRDF.tsv',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['disease_outputs']),
        temp(expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['disease_outputs']))
    run:
        diseasephenotype.build_compendium(input.concords, input.metadata_yamls, input.idlists,input.close_matches,
            {
                'HP':input.bad_hpo_xrefs,
                'MONDO':input.bad_mondo_xrefs,
                'UMLS':input.bad_umls_xrefs
            }, input.icrdf_filename )

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
        synonyms = expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['disease_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['disease_outputs'])
    output:
        synonyms_gzipped = expand("{od}/synonyms/{ap}.gz", od = config['output_directory'], ap = config['disease_outputs']),
        x=config['output_directory']+'/reports/disease_done'
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)

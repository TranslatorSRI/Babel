import src.createcompendia.publications as publications

### PubMed

rule download_pubmed:
    output:
        done_file = config['download_directory'] + '/PubMed/downloaded',
        baseline_dir = directory(config['download_directory'] + '/PubMed/baseline'),
        updatefiles_dir = directory(config['download_directory'] + '/PubMed/updatefiles'),
    run:
        publications.download_pubmed(output.done_file)

rule generate_pubmed_concords:
    input:
        config['download_directory'] + '/PubMed/downloaded',
        baseline_dir = directory(config['download_directory'] + '/PubMed/baseline'),
        updatefiles_dir = directory(config['download_directory'] + '/PubMed/updatefiles'),
    output:
        titles_file = config['download_directory'] + '/PubMed/titles',
        status_file = config['download_directory'] + '/PubMed/statuses',
        pmid_id_file = config['intermediate_directory'] + '/publications/ids/PMID',
        pmid_doi_concord_file = config['intermediate_directory'] + '/publications/concords/PMID_DOI',
    run:
        publications.parse_pubmed_into_tsvs(
            input.baseline_dir,
            input.updatefiles_dir,
            output.titles_file,
            output.status_file,
            output.pmid_id_file,
            output.pmid_doi_concord_file)

rule generate_pubmed_compendia:
    input:
        pmid_id_file = config['intermediate_directory'] + '/publications/ids/PMID',
        pmid_doi_concord_file = config['intermediate_directory'] + '/publications/concords/PMID_DOI',
        icrdf_filename=config['download_directory'] + '/icRDF.tsv',
    output:
        publication_compendium = config['output_directory'] + '/compendia/Publication.txt',
    run:
        publications.generate_compendium(
            [input.pmid_doi_concord_file],
            [input.pmid_id_file],
            output.publication_compendium,
            input.icrdf_filename
        )

#
# rule anatomy_uberon_ids:
#     output:
#         outfile=config['intermediate_directory']+"/anatomy/ids/UBERON"
#     run:
#         anatomy.write_uberon_ids(output.outfile)
#
# rule anatomy_cl_ids:
#     output:
#         outfile=config['intermediate_directory']+"/anatomy/ids/CL"
#     run:
#         anatomy.write_cl_ids(output.outfile)
#
# rule anatomy_go_ids:
#     output:
#         outfile=config['intermediate_directory']+"/anatomy/ids/GO"
#     run:
#         anatomy.write_go_ids(output.outfile)
#
# rule anatomy_ncit_ids:
#     output:
#         outfile=config['intermediate_directory']+"/anatomy/ids/NCIT"
#     run:
#         anatomy.write_ncit_ids(output.outfile)
#
# rule anatomy_mesh_ids:
#     input:
#         config['download_directory']+'/MESH/mesh.nt'
#     output:
#         outfile=config['intermediate_directory']+"/anatomy/ids/MESH"
#     run:
#         anatomy.write_mesh_ids(output.outfile)
#
# rule anatomy_umls_ids:
#     input:
#         mrsty=config['download_directory'] + "/UMLS/MRSTY.RRF"
#     output:
#         outfile=config['intermediate_directory']+"/anatomy/ids/UMLS"
#     run:
#         anatomy.write_umls_ids(input.mrsty, output.outfile)
#
# rule get_anatomy_obo_relationships:
#     output:
#         config['intermediate_directory']+'/anatomy/concords/UBERON',
#         config['intermediate_directory']+'/anatomy/concords/CL',
#         config['intermediate_directory']+'/anatomy/concords/GO',
#     run:
#         anatomy.build_anatomy_obo_relationships(config['intermediate_directory']+'/anatomy/concords')
#
# rule get_anatomy_umls_relationships:
#     input:
#         mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
#         infile=config['intermediate_directory']+"/anatomy/ids/UMLS"
#     output:
#         outfile=config['intermediate_directory']+'/anatomy/concords/UMLS',
#     run:
#         anatomy.build_anatomy_umls_relationships(input.mrconso, input.infile, output.outfile)
#
# rule anatomy_compendia:
#     input:
#         labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['anatomy_prefixes']),
#         synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['anatomy_prefixes']),
#         concords=expand("{dd}/anatomy/concords/{ap}",dd=config['intermediate_directory'],ap=config['anatomy_concords']),
#         idlists=expand("{dd}/anatomy/ids/{ap}",dd=config['intermediate_directory'],ap=config['anatomy_ids']),
#         icrdf_filename=config['download_directory']+'/icRDF.tsv',
#     output:
#         expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
#         expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
#     run:
#         anatomy.build_compendia(input.concords, input.idlists, input.icrdf_filename)
#
# rule check_anatomy_completeness:
#     input:
#         input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
#     output:
#         report_file = config['output_directory']+'/reports/anatomy_completeness.txt'
#     run:
#         assessments.assess_completeness(config['intermediate_directory']+'/anatomy/ids',input.input_compendia,output.report_file)
#
# rule check_anatomical_entity:
#     input:
#         infile=config['output_directory']+'/compendia/AnatomicalEntity.txt'
#     output:
#         outfile=config['output_directory']+'/reports/AnatomicalEntity.txt'
#     run:
#         assessments.assess(input.infile, output.outfile)
#
# rule check_gross_anatomical_structure:
#     input:
#         infile=config['output_directory']+'/compendia/GrossAnatomicalStructure.txt'
#     output:
#         outfile=config['output_directory']+'/reports/GrossAnatomicalStructure.txt'
#     run:
#         assessments.assess(input.infile, output.outfile)
#
# rule check_cell:
#     input:
#         infile=config['output_directory']+'/compendia/Cell.txt'
#     output:
#         outfile=config['output_directory']+'/reports/Cell.txt'
#     run:
#         assessments.assess(input.infile, output.outfile)
#
# rule check_cellular_component:
#     input:
#         infile=config['output_directory']+'/compendia/CellularComponent.txt'
#     output:
#         outfile=config['output_directory']+'/reports/CellularComponent.txt'
#     run:
#         assessments.assess(input.infile, output.outfile)
#
# rule anatomy:
#     input:
#         config['output_directory']+'/reports/anatomy_completeness.txt',
#         synonyms=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
#         reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['anatomy_outputs'])
#     output:
#         x=config['output_directory']+'/reports/anatomy_done'
#     shell:
#         "echo 'done' >> {output.x}"


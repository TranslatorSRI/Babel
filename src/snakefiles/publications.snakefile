import src.createcompendia.publications as publications
import src.assess_compendia as assessments

### PubMed

rule download_pubmed:
    output:
        baseline_dir = directory(config['download_directory'] + '/PubMed/baseline'),
        updatefiles_dir = directory(config['download_directory'] + '/PubMed/updatefiles'),
        done_file = config['download_directory'] + '/PubMed/downloaded',
    run:
        publications.download_pubmed(output.done_file)

rule verify_pubmed:
    input:
        config['download_directory'] + '/PubMed/downloaded',
    output:
        done_file = config['download_directory'] + '/PubMed/verified',
    run:
        publications.verify_pubmed_downloads(
            [
                config['download_directory'] + '/PubMed/baseline',
                config['download_directory'] + '/PubMed/updatefiles'
            ],
            output.done_file
        )

rule generate_pubmed_concords:
    input:
        config['download_directory'] + '/PubMed/verified',
        baseline_dir = config['download_directory'] + '/PubMed/baseline',
        updatefiles_dir = config['download_directory'] + '/PubMed/updatefiles',
    output:
        titles_file = config['download_directory'] + '/PubMed/titles.tsv',
        status_file = config['download_directory'] + '/PubMed/statuses.jsonl.gz',
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
        titles = [
            config['download_directory'] + '/PubMed/titles.tsv',
        ],
        icrdf_filename=config['download_directory'] + '/icRDF.tsv',
    output:
        publication_compendium = config['output_directory'] + '/compendia/Publication.txt',
    run:
        publications.generate_compendium(
            [input.pmid_doi_concord_file],
            [input.pmid_id_file],
            input.titles,
            output.publication_compendium,
            input.icrdf_filename
        )

rule check_publications_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['publication_outputs'])
    output:
        report_file = config['output_directory']+'/reports/publication_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/publications/ids',input.input_compendia,output.report_file)

rule check_publications:
    input:
        infile=config['output_directory']+'/compendia/Publication.txt'
    output:
        outfile=config['output_directory']+'/reports/Publication.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule publications:
    input:
        config['output_directory']+'/reports/publication_completeness.txt',
        # No synonyms for Publication.txt yet.
        # synonyms=expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['publication_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['publication_outputs'])
    output:
        x=config['output_directory']+'/reports/publications_done'
    shell:
        "echo 'done' >> {output.x}"

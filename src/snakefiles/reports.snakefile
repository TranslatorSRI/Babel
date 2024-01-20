import os

from src.reports.compendia_per_file_reports import assert_files_in_directory, \
    generate_content_report_for_compendium, summarize_content_report_for_compendia
from src.reports.index_wide_synonym_tests import report_on_index_wide_synonym_tests

# Some paths we will use at multiple times in these reports.
compendia_path = config['output_directory'] + '/compendia'
synonyms_path = config['output_directory'] + '/synonyms'
conflations_path = config['output_directory'] + '/conflation'

# Expected compendia files.
compendia_files = config['anatomy_outputs'] + config['gene_outputs'] + config['protein_outputs'] + \
    config['disease_outputs'] + config['process_outputs'] + \
    config['chemical_outputs'] + config['taxon_outputs'] + config['genefamily_outputs'] + \
    config['umls_outputs'] + config['macromolecularcomplex_outputs']

# Expected synonym files.
synonyms_files = config['anatomy_outputs'] + config['gene_outputs'] + config['protein_outputs'] + \
    config['disease_outputs'] + config['process_outputs'] + \
    config['chemical_outputs'] + config['taxon_outputs'] + config['genefamily_outputs'] + \
    config['drugchemicalconflated_synonym_outputs'] + \
    config['umls_outputs'] + config['macromolecularcomplex_outputs']

# Expected conflation files.
conflation_files = config['geneprotein_outputs'] + config['drugchemical_outputs']

# Make sure we have all the expected Compendia files.
rule check_compendia_files:
    input:
        # Don't run this until all the outputs have been generated.
        config['output_directory'] + '/reports/outputs_done'
    output:
        donefile = config['output_directory']+'/reports/check_compendia_files.done'
    run:
        assert_files_in_directory(compendia_path,
            compendia_files,
            output.donefile
        )

# Make sure we have all the expected Synonyms files.
rule check_synonyms_files:
    input:
        # Don't run this until all the outputs have been generated.
        config['output_directory'] + '/reports/outputs_done'
    output:
        donefile = config['output_directory']+'/reports/check_synonyms_files.done'
    run:
        assert_files_in_directory(synonyms_path,
            synonyms_files,
            output.donefile
        )

# Make sure we have all the expected Conflation files.
rule check_conflation_files:
    input:
        # Don't run this until all the outputs have been generated.
        config['output_directory'] + '/reports/outputs_done'
    output:
        donefile = config['output_directory']+'/reports/check_conflation_files.done'
    run:
        assert_files_in_directory(conflations_path,
            conflation_files,
            output.donefile
        )

# Generate a report of CURIE prefixes by file.
expected_content_reports = []
for compendium_filename in compendia_files:
    # Remove the extension from compendium_filename using os.path
    compendium_basename = os.path.splitext(compendium_filename)[0]
    report_filename = f"{config['output_directory']}/reports/content/compendia/{compendium_basename}.json"

    expected_content_reports.append(report_filename)

    rule:
        name: f"generate_content_report_for_compendium_{compendium_basename}"
        input:
            compendium_file = f"{config['output_directory']}/compendia/{compendium_filename}",
        output:
            report_file = report_filename
        run:
            generate_content_report_for_compendium(input.compendium_file, output.report_file)


rule generate_summary_content_report_for_compendia:
    input:
        expected_content_reports = expected_content_reports,
    output:
        report_path = config['output_directory']+'/reports/content/compendia_report.json',
    run:
        summarize_content_report_for_compendia(input.expected_content_reports, output.report_path)


rule test_synonyms_for_duplication:
    input:
        synonyms_files = synonyms_files,
    output:
        sqlite_file = config['output_directory']+'/reports/duplication/synonyms.sqlite3',
        report_path = config['output_directory']+'/reports/duplication/synonym_duplication_report.json',
    run:
        report_on_index_wide_synonym_tests(input.synonyms_file, output.sqlite_file, output.report_path)


# Check that all the reports were built correctly.
rule all_reports:
    input:
        config['output_directory']+'/reports/content/compendia_report.json',
        config['output_directory']+'/reports/check_compendia_files.done',
        config['output_directory']+'/reports/check_synonyms_files.done',
        config['output_directory']+'/reports/check_conflation_files.done',
    output:
        x = config['output_directory']+'/reports/reports_done',
    shell:
        "echo 'done' >> {output.x}"


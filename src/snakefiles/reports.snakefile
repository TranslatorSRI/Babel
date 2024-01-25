from src.snakefiles.util import get_all_compendia, get_all_synonyms_with_drugchemicalconflated
import os

from src.reports.compendia_per_file_reports import assert_files_in_directory, \
    generate_content_report_for_compendium, summarize_content_report_for_compendia

# Some paths we will use at multiple times in these reports.
compendia_path = config['output_directory'] + '/compendia'
synonyms_path = config['output_directory'] + '/synonyms'
conflations_path = config['output_directory'] + '/conflation'

# Expected compendia files.
compendia_files = get_all_compendia(config)

# Expected synonym files.
synonyms_files = get_all_synonyms_with_drugchemicalconflated(config)

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


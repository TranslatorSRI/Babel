from src.reports.compendia_per_file_reports import generate_curie_prefixes_per_file_report, assert_files_in_directory

# Some paths we will use at multiple times in these reports.
compendia_path = config['output_directory'] + '/compendia'
synonyms_path = config['output_directory'] + '/synonyms'
conflations_path = config['output_directory'] + '/conflation'

# Make sure we have all the expected Compendia files.
rule check_compendia_files:
    input:
        # Don't run this until all the outputs have been generated.
        config['output_directory'] + '/reports/outputs_done'
    output:
        donefile = config['output_directory']+'/reports/check_compendia_files.done'
    run:
        assert_files_in_directory(compendia_path,
            config['anatomy_outputs'] + config['gene_outputs'] + config['protein_outputs'] +
            config['disease_outputs'] + config['process_outputs'] +
            config['chemical_outputs'] + config['taxon_outputs'] + config['genefamily_outputs'] +
            config['umls_outputs'] + config['macromolecularcomplex_outputs'],
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
            config['anatomy_outputs'] + config['gene_outputs'] + config['protein_outputs'] +
            config['disease_outputs'] + config['process_outputs'] +
            config['chemical_outputs'] + config['taxon_outputs'] + config['genefamily_outputs'] +
            config['drugchemicalconflated_synonym_outputs'] +
            config['umls_outputs'] + config['macromolecularcomplex_outputs'],
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
            config['geneprotein_outputs'] + config['drugchemical_outputs'],
            output.donefile
        )

# Generate a report of CURIE prefixes by file.
rule curie_prefix_counts_by_file_report:
    input:
        # Don't run this until all the outputs have been generated.
        config['output_directory'] + '/reports/outputs_done'
    output:
        outfile = config['output_directory']+'/reports/curie_prefixes_by_file.json'
    run:
        generate_curie_prefixes_per_file_report(compendia_path, output.outfile)


# Check that all the reports were built correctly.
rule all_reports:
    input:
        config['output_directory']+'/reports/curies_by_file.json'
    output:
        x = config['output_directory']+'/reports/reports_done',
    shell:
        "echo 'done' >> {output.x}"


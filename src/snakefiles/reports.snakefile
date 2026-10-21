from src.reports import report_tables
from src.reports import prefix_comparison
from src.reports.umls_mesh_skew import generate_umls_mesh_skew_report
from src.reports.source_impact_details import NEW_CLIQUES_CSV, NEW_XREFS_SUMMARY_CSV
from src.snakefiles.util import get_all_compendia, get_all_synonyms, get_all_gzipped
import os

from src.reports.compendia_per_file_reports import (
    assert_files_in_directory,
    generate_content_report_for_compendium,
    summarize_content_report_for_compendia,
)

# Some paths we will use at multiple times in these reports.
compendia_path = config["output_directory"] + "/compendia"
synonyms_path = config["output_directory"] + "/synonyms"
conflations_path = config["output_directory"] + "/conflation"

# Expected compendia files.
compendia_files = get_all_compendia(config)

# Expected synonym files.
synonyms_files = get_all_synonyms(config)

# Expected conflation files.
conflation_files = config["geneprotein_outputs"] + config["drugchemical_outputs"]


# Trivial aggregation rules run locally so they don't consume a SLURM slot.
localrules:
    all_reports,


# Make sure we have all the expected Compendia files.
rule check_compendia_files:
    input:
        # Don't run this until all the outputs have been generated.
        config["output_directory"] + "/reports/outputs_done",
    output:
        donefile=config["output_directory"] + "/reports/check_compendia_files.done",
    benchmark:
        config["output_directory"] + "/benchmarks/check_compendia_files.tsv"
    run:
        assert_files_in_directory(compendia_path, compendia_files, output.donefile)


# Make sure we have all the expected Synonyms files.
rule check_synonyms_gzipped_files:
    input:
        # Don't run this until all the outputs have been generated.
        config["output_directory"] + "/reports/outputs_done",
    output:
        donefile=config["output_directory"] + "/reports/check_synonyms_files.done",
    benchmark:
        config["output_directory"] + "/benchmarks/check_synonyms_gzipped_files.tsv"
    run:
        assert_files_in_directory(synonyms_path, get_all_gzipped(synonyms_files), output.donefile)


# Make sure we have all the expected Conflation files.
rule check_conflation_files:
    input:
        # Don't run this until all the outputs have been generated.
        config["output_directory"] + "/reports/outputs_done",
    output:
        donefile=config["output_directory"] + "/reports/check_conflation_files.done",
    benchmark:
        config["output_directory"] + "/benchmarks/check_conflation_files.tsv"
    run:
        assert_files_in_directory(conflations_path, conflation_files, output.donefile)


# Generate a report of CURIE prefixes by file.
expected_content_reports = []
for compendium_filename in compendia_files:
    # Remove the extension from compendium_filename using os.path
    compendium_basename = os.path.splitext(compendium_filename)[0]
    report_filename = config["output_directory"] + "/reports/content/compendia/" + compendium_basename + ".json"

    expected_content_reports.append(report_filename)

    rule:
        name:
            "generate_content_report_for_compendium_" + compendium_basename
        input:
            compendium_file=config["output_directory"] + "/compendia/" + compendium_filename,
        output:
            report_file=report_filename,
        benchmark:
            (
                config["output_directory"]
                + "/benchmarks/generate_content_report_for_compendium_"
                + compendium_basename
                + ".tsv"
            )
        run:
            generate_content_report_for_compendium(input.compendium_file, output.report_file)


rule generate_compendia_summary_report:
    input:
        expected_content_reports=expected_content_reports,
    output:
        report_path=config["output_directory"] + "/reports/content/compendia_report.json",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_compendia_summary_report.tsv"
    run:
        summarize_content_report_for_compendia(input.expected_content_reports, output.report_path)


#
# REPORT TABLES
#


# Generate a prefix table.
rule generate_prefix_table:
    input:
        prefix_report=config["output_directory"] + "/reports/duckdb/prefix_report.json",
    output:
        prefix_table=config["output_directory"] + "/reports/tables/prefix_table.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_prefix_table.tsv"
    run:
        report_tables.generate_prefix_table(input.prefix_report, output.prefix_table)


# Generate a cliques table.
rule generate_cliques_table:
    input:
        prefix_report=config["output_directory"] + "/reports/duckdb/prefix_report.json",
    output:
        cliques_table=config["output_directory"] + "/reports/tables/cliques_table.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_cliques_table.tsv"
    run:
        report_tables.generate_cliques_table(input.prefix_report, output.cliques_table)


# Compare this build's prefix report against the previous release pinned in config.yaml.
rule generate_prefix_comparison:
    input:
        prefix_report=config["output_directory"] + "/reports/duckdb/prefix_report.json",
    output:
        overall_csv=config["output_directory"] + "/reports/tables/prefix_comparison_overall.csv",
        by_clique_csv=config["output_directory"] + "/reports/tables/prefix_comparison_by_clique_prefix.csv",
        md=config["output_directory"] + "/reports/tables/prefix_comparison.md",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_prefix_comparison.tsv"
    params:
        # The baseline is a committed repo file (read-only during the build), passed as a param rather
        # than an input: an input would hard-error on the first run before any baseline is committed,
        # and generate_prefix_comparison handles a missing baseline gracefully. Trade-off: because it
        # isn't a tracked input, bumping previous_release (or editing the baseline) won't by itself
        # retrigger this rule -- it only reruns when prefix_report.json does. A full run regenerates
        # prefix_report.json, so this only matters if you rerun reports incrementally after changing
        # the pin; force it then with `-R generate_prefix_comparison`.
        baseline_json=prefix_comparison.baseline_path(config["previous_release"]),
        # The release that baseline belongs to. Passed separately because the file is named
        # `prefix_report.json` inside a per-release directory, so its name is not recoverable from
        # the path -- and the self-comparison guard is really a check that the two pins differ.
        baseline_release=config["previous_release"],
        warn_abs=config["prefix_comparison_warn_abs"],
        warn_pct=config["prefix_comparison_warn_pct"],
    run:
        prefix_comparison.generate_prefix_comparison(
            input.prefix_report,
            params.baseline_json,
            params.baseline_release,
            output.overall_csv,
            output.by_clique_csv,
            output.md,
            params.warn_abs,
            params.warn_pct,
        )


# Generate a table of mapping sources.
rule generate_mapping_sources_table:
    input:
        metadata_yaml_files=expand(
            "{od}/metadata/{compendia_filename}.yaml",
            od=config["output_directory"],
            compendia_filename=get_all_compendia(config),
        ),
    output:
        mapping_sources_table=config["output_directory"] + "/reports/tables/mapping_sources_table.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_mapping_sources_table.tsv"
    run:
        report_tables.generate_mapping_sources_table(input.metadata_yaml_files, output.mapping_sources_table)


# Count UMLS MSH codes that have no record in the MeSH dump, so UMLS/MeSH version skew is visible.
rule generate_umls_mesh_skew_report:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        mesh_nt=config["download_directory"] + "/MESH/mesh.nt",
    output:
        report=config["output_directory"] + "/reports/umls_mesh_skew.json",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_umls_mesh_skew_report.tsv"
    params:
        umls_version=config["umls_version"],
        mesh_year=config["mesh_year"],
    run:
        generate_umls_mesh_skew_report(
            input.mrconso, input.mesh_nt, params.umls_version, params.mesh_year, output.report
        )


# Optional per-source impact report. Not part of all_reports.
# Invoke with e.g.:
#   snakemake babel_outputs/reports/source_impact/EMAPA.md
# See src/tools/source_impact_report/cli.py for the underlying CLI.
rule report_source_impact:
    output:
        # The markdown report plus the four detail files written into the <output-stem>/
        # subdirectory beside it (see src/reports/source_impact_details.py). The new-cliques file
        # and new-xrefs files are the two committed reductions, whose names encode how they reduce,
        # so take both from the writer's constants rather than repeating literals that could drift.
        # The full tables they reduce land in the same directory but are not declared outputs.
        md=config["output_directory"] + "/reports/source_impact/{source}.md",
        new_cliques=config["output_directory"] + f"/reports/source_impact/{{source}}/{NEW_CLIQUES_CSV}",
        modified_csv=config["output_directory"] + "/reports/source_impact/{source}/modified-cliques.csv",
        modified_json=config["output_directory"] + "/reports/source_impact/{source}/modified-cliques.json",
        new_xrefs=config["output_directory"] + f"/reports/source_impact/{{source}}/{NEW_XREFS_SUMMARY_CSV}",
    benchmark:
        config["output_directory"] + "/benchmarks/report_source_impact_{source}.tsv"
    params:
        # Derive the roots the CLI reads from the same config the outputs use, so changing
        # output_directory/intermediate_directory doesn't desync inputs from outputs (the
        # CLI's own defaults are the babel_outputs/* literals).
        intermediate_root=config["intermediate_directory"],
        compendia_root=config["output_directory"] + "/compendia",
        downloads_root=config["download_directory"],
    shell:
        "uv run source-impact-report --source {wildcards.source} --output {output.md}"
        " --intermediate-root {params.intermediate_root}"
        " --compendia-root {params.compendia_root}"
        " --downloads-root {params.downloads_root}"


# Check that all the reports were built correctly.
rule all_reports:
    input:
        config["output_directory"] + "/reports/content/compendia_report.json",
        config["output_directory"] + "/reports/check_compendia_files.done",
        config["output_directory"] + "/reports/check_synonyms_files.done",
        config["output_directory"] + "/reports/check_conflation_files.done",
        config["output_directory"] + "/reports/tables/prefix_table.csv",
        config["output_directory"] + "/reports/tables/cliques_table.csv",
        config["output_directory"] + "/reports/tables/mapping_sources_table.csv",
        config["output_directory"] + "/reports/tables/prefix_comparison.md",
        config["output_directory"] + "/reports/umls_mesh_skew.json",
    output:
        x=config["output_directory"] + "/reports/reports_done",
    shell:
        "echo 'done' >> {output.x}"

import src.createcompendia.taxon as taxon
import src.assess_compendia as assessments
import src.snakefiles.util as util


rule taxon_ncbi_ids:
    input:
        infile=config["download_directory"] + "/NCBITaxon/labels",
    output:
        outfile=config["intermediate_directory"] + "/taxon/ids/NCBITaxon",
    benchmark:
        config["output_directory"] + "/benchmarks/taxon_ncbi_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:OrganismTaxon\"}}' {input.infile} > {output.outfile}"


rule taxon_mesh_ids:
    input:
        infile=config["download_directory"] + "/MESH/mesh.nt",
    output:
        outfile=config["intermediate_directory"] + "/taxon/ids/MESH",
    benchmark:
        config["output_directory"] + "/benchmarks/taxon_mesh_ids.tsv"
    run:
        taxon.write_mesh_ids(output.outfile)


rule taxon_umls_ids:
    input:
        mrsty=config["download_directory"] + "/UMLS/MRSTY.RRF",
    output:
        outfile=config["intermediate_directory"] + "/taxon/ids/UMLS",
    benchmark:
        config["output_directory"] + "/benchmarks/taxon_umls_ids.tsv"
    run:
        taxon.write_umls_ids(input.mrsty, output.outfile)


rule get_taxon_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/taxon/ids/UMLS",
    output:
        outfile=config["intermediate_directory"] + "/taxon/concords/UMLS",
        metadata_yaml=config["intermediate_directory"] + "/taxon/concords/metadata-UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_taxon_umls_relationships.tsv"
    run:
        taxon.build_taxon_umls_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)


rule get_taxon_relationships:
    input:
        meshfile=config["download_directory"] + "/MESH/mesh.nt",
        meshids=config["intermediate_directory"] + "/taxon/ids/MESH",
    output:
        outfile=config["intermediate_directory"] + "/taxon/concords/NCBI_MESH",
        metadata_yaml=config["intermediate_directory"] + "/taxon/concords/metadata-NCBI_MESH.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_taxon_relationships.tsv"
    run:
        taxon.build_relationships(output.outfile, input.meshids, output.metadata_yaml)


rule taxon_compendia:
    input:
        labels=expand("{dd}/{ap}/labels", dd=config["download_directory"], ap=config["taxon_labels"]),
        synonyms=expand("{dd}/{ap}/synonyms", dd=config["download_directory"], ap=config["taxon_synonyms"]),
        concords=expand("{dd}/taxon/concords/{ap}", dd=config["intermediate_directory"], ap=config["taxon_concords"]),
        metadata_yamls=expand(
            "{dd}/taxon/concords/metadata-{ap}.yaml", dd=config["intermediate_directory"], ap=config["taxon_concords"]
        ),
        idlists=expand("{dd}/taxon/ids/{ap}", dd=config["intermediate_directory"], ap=config["taxon_ids"]),
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    output:
        expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["taxon_outputs"]),
        temp(expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["taxon_outputs"])),
        output_metadata=expand("{od}/metadata/{ap}.yaml", od=config["output_directory"], ap=config["taxon_outputs"]),
    benchmark:
        config["output_directory"] + "/benchmarks/taxon_compendia.tsv"
    resources:
        # babel-1.17 flagged this as the tightest rule still on the 16 GB default and the first to
        # watch for an OOM as NCBITaxon grows; 2026jul22 peaked at 14.1 GiB = 15.1 GB, 95% of the
        # default, so it gets a block.
        mem="24G",
    run:
        taxon.build_compendia(input.concords, input.metadata_yamls, input.idlists, input.icrdf_filename)


rule check_taxon_completeness:
    input:
        input_compendia=expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["taxon_outputs"]),
    output:
        report_file=config["output_directory"] + "/reports/taxon_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_taxon_completeness.tsv"
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/taxon/ids", input.input_compendia, output.report_file
        )


rule check_taxon:
    input:
        infile=config["output_directory"] + "/compendia/OrganismTaxon.txt",
    output:
        outfile=config["output_directory"] + "/reports/OrganismTaxon.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_taxon.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule taxon:
    input:
        config["output_directory"] + "/reports/taxon_completeness.txt",
        synonyms=expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["taxon_outputs"]),
        reports=expand("{od}/reports/{ap}", od=config["output_directory"], ap=config["taxon_outputs"]),
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od=config["output_directory"], ap=config["taxon_outputs"]),
        x=config["output_directory"] + "/reports/taxon_done",
    benchmark:
        config["output_directory"] + "/benchmarks/taxon.tsv"
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)


_missing = util.find_missing_id_prefixes(workflow, config["intermediate_directory"] + "/taxon/ids", config["taxon_ids"])
if _missing:
    raise WorkflowError(f"config.yaml taxon_ids has no rule producing intermediate/taxon/ids/{{prefix}}: {_missing}")

import src.createcompendia.protein as protein
import src.assess_compendia as assessments

# import src.filter_compendia as filter
import src.snakefiles.util as util

### Gene / Protein


rule protein_mesh_ids:
    input:
        infile=config["download_directory"] + "/MESH/mesh.nt",
    output:
        outfile=config["intermediate_directory"] + "/protein/ids/MESH",
    run:
        protein.write_mesh_ids(output.outfile)


rule protein_pr_ids:
    output:
        outfile=config["intermediate_directory"] + "/protein/ids/PR",
    benchmark:
        config["output_directory"] + "/benchmarks/protein_pr_ids.tsv"
    run:
        protein.write_pr_ids(output.outfile)


rule protein_uniprotkb_ids:
    input:
        infile=config["download_directory"] + "/UniProtKB/labels",
    output:
        outfile=config["intermediate_directory"] + "/protein/ids/UniProtKB",
    benchmark:
        config["output_directory"] + "/benchmarks/protein_uniprotkb_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1}}' {input.infile} > {output.outfile}"


rule extract_taxon_ids_from_uniprotkb:
    input:
        infile=config["download_directory"] + "/UniProtKB/idmapping.dat",
    output:
        outfile=config["download_directory"] + "/UniProtKB/taxa",
    benchmark:
        config["output_directory"] + "/benchmarks/extract_taxon_ids_from_uniprotkb.tsv"
    run:
        protein.extract_taxon_ids_from_uniprotkb(input.infile, output.outfile)


rule protein_umls_ids:
    input:
        mrsty=config["download_directory"] + "/UMLS/MRSTY.RRF",
    output:
        outfile=config["intermediate_directory"] + "/protein/ids/UMLS",
    benchmark:
        config["output_directory"] + "/benchmarks/protein_umls_ids.tsv"
    run:
        protein.write_umls_ids(input.mrsty, output.outfile)


rule protein_ensembl_ids:
    input:
        infile=config["download_directory"] + "/ENSEMBL/BioMartDownloadComplete",
    output:
        outfile=config["intermediate_directory"] + "/protein/ids/ENSEMBL",
    benchmark:
        config["output_directory"] + "/benchmarks/protein_ensembl_ids.tsv"
    run:
        protein.write_ensembl_protein_ids(config["download_directory"] + "/ENSEMBL", output.outfile)


rule get_protein_uniprotkb_ensembl_relationships:
    input:
        infile=config["download_directory"] + "/UniProtKB/idmapping.dat",
    output:
        outfile=config["intermediate_directory"] + "/protein/concords/UniProtKB",
        metadata_yaml=config["intermediate_directory"] + "/protein/concords/metadata-UniProtKB.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_protein_uniprotkb_ensembl_relationships.tsv"
    run:
        protein.build_protein_uniprotkb_ensemble_relationships(input.infile, output.outfile, output.metadata_yaml)


rule get_protein_pr_uniprotkb_relationships:
    output:
        outfile=config["intermediate_directory"] + "/protein/concords/PR",
        metadata_yaml=config["intermediate_directory"] + "/protein/concords/metadata-PR.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_protein_pr_uniprotkb_relationships.tsv"
    # Because we get this from UberGraph, we sometimes end up with incomplete/failed transfers and need to retry.
    retries: 3
    run:
        protein.build_pr_uniprot_relationships(output.outfile, metadata_yaml=output.metadata_yaml)


rule get_protein_ncit_uniprotkb_relationships:
    input:
        infile=config["download_directory"] + "/NCIT/NCIt-SwissProt_Mapping.txt",
    output:
        outfile=config["intermediate_directory"] + "/protein/concords/NCIT_UniProtKB",
        metadata_yaml=config["intermediate_directory"] + "/protein/concords/metadata-NCIT_UniProtKB.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_protein_ncit_uniprotkb_relationships.tsv"
    run:
        protein.build_ncit_uniprot_relationships(input.infile, output.outfile, output.metadata_yaml)


rule get_protein_ncit_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/protein/ids/UMLS",
    output:
        outfile=config["intermediate_directory"] + "/protein/concords/NCIT_UMLS",
        metadata_yaml=config["intermediate_directory"] + "/protein/concords/metadata-NCIT_UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_protein_ncit_umls_relationships.tsv"
    run:
        protein.build_umls_ncit_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)


rule get_protein_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/protein/ids/UMLS",
    output:
        outfile=config["intermediate_directory"] + "/protein/concords/UMLS",
        metadata_yaml=config["intermediate_directory"] + "/protein/concords/metadata-UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_protein_umls_relationships.tsv"
    run:
        protein.build_umls_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)


rule protein_compendia:
    input:
        labels=expand("{dd}/{ap}/labels", dd=config["download_directory"], ap=config["protein_labels"]),
        synonyms=expand("{dd}/{ap}/synonyms", dd=config["download_directory"], ap=config["protein_synonyms"]),
        concords=expand(
            "{dd}/protein/concords/{ap}", dd=config["intermediate_directory"], ap=config["protein_concords"]
        ),
        metadata_yamls=expand(
            "{dd}/protein/concords/metadata-{ap}.yaml",
            dd=config["intermediate_directory"],
            ap=config["protein_concords"],
        ),
        idlists=expand("{dd}/protein/ids/{ap}", dd=config["intermediate_directory"], ap=config["protein_ids"]),
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
        # Include the taxon information from UniProtKB
        uniprotkb_taxa_file=config["download_directory"] + "/UniProtKB/taxa",
    output:
        expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["protein_outputs"]),
        temp(expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["protein_outputs"])),
        expand("{od}/metadata/{ap}.yaml", od=config["output_directory"], ap=config["protein_outputs"]),
    benchmark:
        config["output_directory"] + "/benchmarks/protein_compendia.tsv"
    resources:
        # Do NOT size this from 2026jul22 alone: UniProtKB shrank ~41% upstream that release, so it
        # ran 5.5h/246G where babel-1.17 took 7.6h/337G. Against the babel-1.17 figures these limits
        # are 63% and 66% used, which is right. `babel-slurm-resources` will report this rule as
        # over-provisioned until UniProtKB recovers; ignore that unless a second full-size run agrees.
        runtime="12h",
        mem="512G",
    run:
        protein.build_protein_compendia(input.concords, input.metadata_yamls, input.idlists, input.icrdf_filename)


rule check_protein_completeness:
    input:
        input_compendia=expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["protein_outputs"]),
    output:
        report_file=config["output_directory"] + "/reports/protein_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_protein_completeness.tsv"
    resources:
        # Peaks at ~21 GB on babel-1.17 (see docs/tools/Resources.md); over the 16 GB default.
        mem="24G",
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/protein/ids", input.input_compendia, output.report_file
        )


rule check_protein:
    input:
        infile=config["output_directory"] + "/compendia/Protein.txt",
    output:
        outfile=config["output_directory"] + "/reports/Protein.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_protein.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule protein:
    input:
        config["output_directory"] + "/reports/protein_completeness.txt",
        synonyms=expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["protein_outputs"]),
        reports=expand("{od}/reports/{ap}", od=config["output_directory"], ap=config["protein_outputs"]),
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od=config["output_directory"], ap=config["protein_outputs"]),
        x=config["output_directory"] + "/reports/protein_done",
    benchmark:
        config["output_directory"] + "/benchmarks/protein.tsv"
    resources:
        cpus_per_task=6,
        # 2.1h on babel-1.17, 1.6h on 2026jul22.
        runtime="4h",
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)


#
# rule filter_protein:
#    input:
#        full=config['output_directory'] + '/compendia/Protein.txt'
#    output:
#        filtered=config['output_directory'] + '/compendia/Protein_filtered.txt'
#    run:
#        filter.filter_compendium(input.full,output.filtered)


_missing = util.find_missing_id_prefixes(
    workflow, config["intermediate_directory"] + "/protein/ids", config["protein_ids"]
)
if _missing:
    raise WorkflowError(
        f"config.yaml protein_ids has no rule producing intermediate/protein/ids/{{prefix}}: {_missing}"
    )

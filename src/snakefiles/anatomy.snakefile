import src.createcompendia.anatomy as anatomy
import src.assess_compendia as assessments
import src.snakefiles.util as util

### AnatomicalEntity / Cell / CellularComponent


rule anatomy_uberon_ids:
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/UBERON",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_uberon_ids.tsv"
    run:
        anatomy.write_uberon_ids(output.outfile)


rule anatomy_cl_ids:
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/CL",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_cl_ids.tsv"
    run:
        anatomy.write_cl_ids(output.outfile)


rule anatomy_emapa_ids:
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/EMAPA",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_emapa_ids.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    run:
        anatomy.write_emapa_ids(output.outfile)


rule anatomy_go_ids:
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/GO",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_go_ids.tsv"
    run:
        anatomy.write_go_ids(output.outfile)


rule anatomy_ncit_ids:
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/NCIT",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_ncit_ids.tsv"
    run:
        anatomy.write_ncit_ids(output.outfile)


rule anatomy_mesh_ids:
    input:
        config["download_directory"] + "/MESH/mesh.nt",
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/MESH",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_mesh_ids.tsv"
    run:
        anatomy.write_mesh_ids(output.outfile)


rule anatomy_umls_ids:
    input:
        mrsty=config["download_directory"] + "/UMLS/MRSTY.RRF",
    output:
        outfile=config["intermediate_directory"] + "/anatomy/ids/UMLS",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_umls_ids.tsv"
    run:
        anatomy.write_umls_ids(input.mrsty, output.outfile)


rule get_anatomy_obo_relationships:
    output:
        config["intermediate_directory"] + "/anatomy/concords/UBERON",
        config["intermediate_directory"] + "/anatomy/concords/CL",
        config["intermediate_directory"] + "/anatomy/concords/GO",
        config["intermediate_directory"] + "/anatomy/concords/EMAPA",
        uberon_metadata=config["intermediate_directory"] + "/anatomy/concords/metadata-UBERON.yaml",
        cl_metadata=config["intermediate_directory"] + "/anatomy/concords/metadata-CL.yaml",
        go_metadata=config["intermediate_directory"] + "/anatomy/concords/metadata-GO.yaml",
        emapa_metadata=config["intermediate_directory"] + "/anatomy/concords/metadata-EMAPA.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_anatomy_obo_relationships.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    run:
        anatomy.build_anatomy_obo_relationships(
            config["intermediate_directory"] + "/anatomy/concords",
            {
                "UBERON": output.uberon_metadata,
                "CL": output.cl_metadata,
                "GO": output.go_metadata,
                "EMAPA": output.emapa_metadata,
            },
        )


rule get_wikidata_cell_relationships:
    output:
        config["intermediate_directory"] + "/anatomy/concords/WIKIDATA",
        wikidata_metadata=config["intermediate_directory"] + "/anatomy/concords/metadata-WIKIDATA.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_wikidata_cell_relationships.tsv"
    run:
        anatomy.build_wikidata_cell_relationships(
            config["intermediate_directory"] + "/anatomy/concords", output.wikidata_metadata
        )


rule get_anatomy_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/anatomy/ids/UMLS",
    output:
        outfile=config["intermediate_directory"] + "/anatomy/concords/UMLS",
        umls_metadata=config["intermediate_directory"] + "/anatomy/concords/metadata-UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_anatomy_umls_relationships.tsv"
    run:
        anatomy.build_anatomy_umls_relationships(input.mrconso, input.infile, output.outfile, output.umls_metadata)


rule anatomy_compendia:
    input:
        labels=os.path.join(config["download_directory"], "common", config["common"]["labels"][0]),
        synonyms=os.path.join(config["download_directory"], "common", config["common"]["synonyms"][0]),
        concords=expand(
            "{dd}/anatomy/concords/{ap}", dd=config["intermediate_directory"], ap=config["anatomy_concords"]
        ),
        metadata_yamls=expand(
            "{dd}/anatomy/concords/metadata-{ap}.yaml",
            dd=config["intermediate_directory"],
            ap=config["anatomy_concords"],
        ),
        idlists=expand("{dd}/anatomy/ids/{ap}", dd=config["intermediate_directory"], ap=config["anatomy_ids"]),
        # Declared as an input so editing the file re-triggers this rule. Referenced through the
        # constant rather than repeating the literal: the impact report resolves the same file via
        # ANATOMY_BAD_XREFS, and if the two drifted the report would stop matching the build.
        badxrefs=anatomy.ANATOMY_BAD_XREFS,
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    output:
        expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["anatomy_outputs"]),
        temp(expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["anatomy_outputs"])),
        expand("{od}/metadata/{ap}.yaml", od=config["output_directory"], ap=config["anatomy_outputs"]),
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy_compendia.tsv"
    run:
        anatomy.build_compendia(
            input.concords, input.metadata_yamls, input.idlists, input.icrdf_filename, badxrefs=input.badxrefs
        )


rule check_anatomy_completeness:
    input:
        input_compendia=expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["anatomy_outputs"]),
    output:
        report_file=config["output_directory"] + "/reports/anatomy_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_anatomy_completeness.tsv"
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/anatomy/ids", input.input_compendia, output.report_file
        )


rule check_anatomical_entity:
    input:
        infile=config["output_directory"] + "/compendia/AnatomicalEntity.txt",
    output:
        outfile=config["output_directory"] + "/reports/AnatomicalEntity.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_anatomical_entity.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule check_gross_anatomical_structure:
    input:
        infile=config["output_directory"] + "/compendia/GrossAnatomicalStructure.txt",
    output:
        outfile=config["output_directory"] + "/reports/GrossAnatomicalStructure.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_gross_anatomical_structure.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule check_cell:
    input:
        infile=config["output_directory"] + "/compendia/Cell.txt",
    output:
        outfile=config["output_directory"] + "/reports/Cell.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_cell.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule check_cellular_component:
    input:
        infile=config["output_directory"] + "/compendia/CellularComponent.txt",
    output:
        outfile=config["output_directory"] + "/reports/CellularComponent.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_cellular_component.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule anatomy:
    input:
        config["output_directory"] + "/reports/anatomy_completeness.txt",
        synonyms=expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["anatomy_outputs"]),
        metadata=expand("{od}/metadata/{ap}.yaml", od=config["output_directory"], ap=config["anatomy_outputs"]),
        reports=expand("{od}/reports/{ap}", od=config["output_directory"], ap=config["anatomy_outputs"]),
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od=config["output_directory"], ap=config["anatomy_outputs"]),
        x=config["output_directory"] + "/reports/anatomy_done",
    benchmark:
        config["output_directory"] + "/benchmarks/anatomy.tsv"
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)


_missing = util.find_missing_id_prefixes(
    workflow, config["intermediate_directory"] + "/anatomy/ids", config["anatomy_ids"]
)
if _missing:
    raise WorkflowError(
        f"config.yaml anatomy_ids has no rule producing intermediate/anatomy/ids/{{prefix}}: {_missing}"
    )

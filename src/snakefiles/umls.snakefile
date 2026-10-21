import src.datahandlers.umls as umls


rule umls_mesh_ownership:
    # "A UMLS CUI follows its MeSH descriptor" (config.yaml: umls_mesh_owning_pipelines). Decides once, for every
    # MeSH-owning pipeline, which pipeline owns each CUI, so that a CUI one pipeline drops is always added by
    # another; those pipelines' *_umls_ids and compendium rules read this file. It needs every MeSH-owning
    # pipeline's ids/MESH, so a single-target run of one of them builds all five (docs/RunningBabel.md,
    # "Per-target sizing").
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        mesh_ids=expand(
            config["intermediate_directory"] + "/{pipeline}/ids/MESH",
            pipeline=config["umls_mesh_owning_pipelines"],
        ),
        # The disease pipeline refuses these CUIs, so ownership must never hand them to it.
        badumls=config["input_directory"] + "/badumls",
        # Gene and process concord UMLS to HGNC and GO, never to MeSH, so no pipeline may take their CUIs.
        non_mesh_umls_ids=[
            config["intermediate_directory"] + "/gene/ids/UMLS",
            config["intermediate_directory"] + "/process/ids/UMLS",
        ],
    output:
        outfile=config["intermediate_directory"] + "/umls/mesh_ownership.tsv",
    benchmark:
        config["output_directory"] + "/benchmarks/umls_mesh_ownership.tsv"
    run:
        umls.write_mesh_ownership(
            input.mrconso,
            dict(zip(config["umls_mesh_owning_pipelines"], input.mesh_ids)),
            output.outfile,
            blocklist_files_by_pipeline={"disease": input.badumls},
            non_mesh_umls_ids_files=input.non_mesh_umls_ids,
        )

import src.datahandlers.umls as umls
import src.createcompendia.diseasephenotype as diseasephenotype


rule umls_mesh_ownership:
    # "A UMLS CUI follows its MeSH descriptor" (config.yaml: umls_mesh_owning_pipelines). Decides once, for every
    # pipeline, which pipeline owns each CUI, so that a CUI one pipeline drops is always added by another; every
    # *_umls_ids rule reads this file. It needs every MeSH-owning pipeline's ids/MESH, so a single-target run
    # builds all five (docs/RunningBabel.md, "Per-target sizing").
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        mesh_ids=expand(
            config["intermediate_directory"] + "/{pipeline}/ids/MESH",
            pipeline=config["umls_mesh_owning_pipelines"],
        ),
        # The disease pipeline refuses these CUIs, so ownership must never hand them to it.
        badumls=config["input_directory"] + "/badumls",
    output:
        outfile=config["intermediate_directory"] + "/umls/mesh_ownership.tsv",
    benchmark:
        config["output_directory"] + "/benchmarks/umls_mesh_ownership.tsv"
    run:
        umls.write_mesh_ownership(
            input.mrconso,
            dict(zip(config["umls_mesh_owning_pipelines"], input.mesh_ids)),
            output.outfile,
            never_add={"disease": {f"UMLS:{cui}" for cui in diseasephenotype.read_badumls(input.badumls)}},
        )

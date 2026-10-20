import src.createcompendia.chemical_role as chemical_role
import src.assess_compendia as assessments
import src.snakefiles.util as util

### ChemicalRole

# Chemical roles from ChEBI (the CHEBI:50906 branch), MeSH (D27 descriptors in no other compendium's trees) and
# UMLS (substance-typed CUIs whose MeSH atoms are all on those descriptors). Chemicals leaves each of these out.


rule chemical_role_chebi_ids:
    output:
        outfile=config["intermediate_directory"] + "/chemical_role/ids/CHEBI",
        metadata_yaml=config["intermediate_directory"] + "/chemical_role/metadata-CHEBI.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/chemical_role_chebi_ids.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    run:
        chemical_role.write_chebi_ids(output.outfile, output.metadata_yaml)


rule chemical_role_mesh_ids:
    input:
        infile=config["download_directory"] + "/MESH/mesh.nt",
    output:
        outfile=config["intermediate_directory"] + "/chemical_role/ids/MESH",
    benchmark:
        config["output_directory"] + "/benchmarks/chemical_role_mesh_ids.tsv"
    run:
        chemical_role.write_mesh_ids(output.outfile)


rule chemical_role_umls_ids:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        mrsty=config["download_directory"] + "/UMLS/MRSTY.RRF",
        mesh_ids=config["intermediate_directory"] + "/chemical_role/ids/MESH",
    output:
        outfile=config["intermediate_directory"] + "/chemical_role/ids/UMLS",
    benchmark:
        config["output_directory"] + "/benchmarks/chemical_role_umls_ids.tsv"
    run:
        chemical_role.write_umls_ids(input.mrconso, input.mrsty, input.mesh_ids, output.outfile)


### Concords


rule get_chemical_role_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/chemical_role/ids/UMLS",
    output:
        outfile=config["intermediate_directory"] + "/chemical_role/concords/UMLS",
        metadata_yaml=config["intermediate_directory"] + "/chemical_role/concords/metadata-UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_chemical_role_umls_relationships.tsv"
    run:
        chemical_role.build_umls_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)


rule get_chemical_role_wikidata_relationships:
    input:
        chebi_ids=config["intermediate_directory"] + "/chemical_role/ids/CHEBI",
        mesh_ids=config["intermediate_directory"] + "/chemical_role/ids/MESH",
    output:
        outfile=config["intermediate_directory"] + "/chemical_role/concords/wikidata_mesh_chebi",
        metadata_yaml=config["intermediate_directory"] + "/chemical_role/concords/metadata-wikidata_mesh_chebi.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_chemical_role_wikidata_relationships.tsv"
    retries: 3  # Wikidata's SPARQL endpoint occasionally times out.
    run:
        chemical_role.build_wikidata_relationships(
            input.chebi_ids, input.mesh_ids, output.outfile, output.metadata_yaml
        )


rule chemical_role_compendia:
    input:
        labels=expand("{dd}/{ap}/labels", dd=config["download_directory"], ap=config["chemical_role_labels"]),
        concords=expand(
            "{dd}/chemical_role/concords/{cc}",
            dd=config["intermediate_directory"],
            cc=config["chemical_role_concords"],
        ),
        metadata_yamls=expand(
            "{dd}/chemical_role/concords/metadata-{cc}.yaml",
            dd=config["intermediate_directory"],
            cc=config["chemical_role_concords"],
        )
        + [config["intermediate_directory"] + "/chemical_role/metadata-CHEBI.yaml"],
        idlists=expand(
            "{dd}/chemical_role/ids/{ap}", dd=config["intermediate_directory"], ap=config["chemical_role_ids"]
        ),
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    output:
        config["output_directory"] + "/compendia/ChemicalRole.txt",
        temp(config["output_directory"] + "/synonyms/ChemicalRole.txt"),
        config["output_directory"] + "/metadata/ChemicalRole.txt.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/chemical_role_compendia.tsv"
    run:
        chemical_role.build_compendia(input.concords, input.metadata_yamls, input.idlists, input.icrdf_filename)


rule check_chemical_role_completeness:
    input:
        input_compendia=config["output_directory"] + "/compendia/ChemicalRole.txt",
    output:
        report_file=config["output_directory"] + "/reports/chemical_role_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_chemical_role_completeness.tsv"
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/chemical_role/ids", [input.input_compendia], output.report_file
        )


rule check_chemical_role:
    input:
        infile=config["output_directory"] + "/compendia/ChemicalRole.txt",
    output:
        outfile=config["output_directory"] + "/reports/ChemicalRole.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_chemical_role.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule chemical_role:
    input:
        config["output_directory"] + "/reports/chemical_role_completeness.txt",
        config["output_directory"] + "/reports/ChemicalRole.txt",
        chemical_role_synonyms=config["output_directory"] + "/synonyms/ChemicalRole.txt",
        metadata=config["output_directory"] + "/metadata/ChemicalRole.txt.yaml",
    output:
        config["output_directory"] + "/synonyms/ChemicalRole.txt.gz",
        x=config["output_directory"] + "/reports/chemical_role_done",
    benchmark:
        config["output_directory"] + "/benchmarks/chemical_role.tsv"
    run:
        util.gzip_files([input.chemical_role_synonyms])
        util.write_done(output.x)

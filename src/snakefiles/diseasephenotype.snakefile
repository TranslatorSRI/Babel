import src.createcompendia.diseasephenotype as diseasephenotype
import src.assess_compendia as assessments
import src.snakefiles.util as util
from src.metadata.provenance import write_concord_metadata

### Disease / Phenotypic Feature


# SNOMEDCT will not have an independent list
# MEDDRA will not have an independent list
# They will only have identifiers that enter via links in UMLS


rule disease_mondo_ids:
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/MONDO",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_mondo_ids.tsv"
    run:
        diseasephenotype.write_mondo_ids(output.outfile)


rule disease_doid_ids:
    input:
        infile=config["download_directory"] + "/DOID/labels",
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/DOID",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_doid_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Disease\"}}' {input.infile} > {output.outfile}"


rule disease_orphanet_ids:
    input:
        infile=config["download_directory"] + "/Orphanet/labels",
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/Orphanet",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_orphanet_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Disease\"}}' {input.infile} > {output.outfile}"


rule disease_efo_ids:
    input:
        efo_owl_file_path=config["download_directory"] + "/EFO/efo.owl",
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/EFO",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_efo_ids.tsv"
    run:
        diseasephenotype.write_efo_ids(input.efo_owl_file_path, output.outfile)


rule disease_ncit_ids:
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/NCIT",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_ncit_ids.tsv"
    run:
        diseasephenotype.write_ncit_ids(output.outfile)


rule disease_mesh_ids:
    input:
        config["download_directory"] + "/MESH/mesh.nt",
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/MESH",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_mesh_ids.tsv"
    run:
        diseasephenotype.write_mesh_ids(output.outfile)


rule disease_umls_ids:
    input:
        badumls=config["input_directory"] + "/badumls",
        mrsty=config["download_directory"] + "/UMLS/MRSTY.RRF",
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/UMLS",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_umls_ids.tsv"
    run:
        diseasephenotype.write_umls_ids(input.mrsty, output.outfile, input.badumls)


rule disease_hp_ids:
    # The location of the RRFs is known to the guts, but should probably come out here.
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/HP",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_hp_ids.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download; per-request retry-with-backoff lives in TripleStore.
    run:
        diseasephenotype.write_hp_ids(output.outfile)


rule disease_mp_ids:
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/MP",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_mp_ids.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download; per-request retry-with-backoff lives in TripleStore.
    run:
        diseasephenotype.write_mp_ids(output.outfile)


rule disease_hp_taxa:
    # Every HP phenotype we ingest describes a human; tag each with NCBITaxon:9606 (Homo sapiens).
    # Derived from the ids file so the taxa cover exactly the ingested identifiers.
    input:
        idfile=config["intermediate_directory"] + "/disease/ids/HP",
    output:
        outfile=config["download_directory"] + "/HP/taxa",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_hp_taxa.tsv"
    run:
        diseasephenotype.write_phenotype_taxa(input.idfile, config["disease_phenotype_taxa"]["HP"], output.outfile)


rule disease_mp_taxa:
    # Every MP phenotype describes a mammal; tag each with NCBITaxon:40674 (Mammalia).
    input:
        idfile=config["intermediate_directory"] + "/disease/ids/MP",
    output:
        outfile=config["download_directory"] + "/MP/taxa",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_mp_taxa.tsv"
    run:
        diseasephenotype.write_phenotype_taxa(input.idfile, config["disease_phenotype_taxa"]["MP"], output.outfile)


rule disease_omim_ids:
    input:
        infile=config["download_directory"] + "/OMIM/mim2gene.txt",
    output:
        outfile=config["intermediate_directory"] + "/disease/ids/OMIM",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_omim_ids.tsv"
    run:
        diseasephenotype.write_omim_ids(input.infile, output.outfile)


### Concords


rule get_disease_obo_relationships:
    output:
        config["intermediate_directory"] + "/disease/concords/MONDO",
        config["intermediate_directory"] + "/disease/concords/MONDO_close",
        config["intermediate_directory"] + "/disease/concords/HP",
        config["intermediate_directory"] + "/disease/concords/MP",
        mondo_metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-MONDO.yaml",
        mondo_close_metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-MONDO_close.yaml",
        hp_metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-HP.yaml",
        mp_metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-MP.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_disease_obo_relationships.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download; per-request retry-with-backoff lives in TripleStore.
    run:
        diseasephenotype.build_disease_obo_relationships(
            config["intermediate_directory"] + "/disease/concords",
            {
                "MONDO": output.mondo_metadata_yaml,
                "MONDO_close": output.mondo_close_metadata_yaml,
                "HP": output.hp_metadata_yaml,
                "MP": output.mp_metadata_yaml,
            },
        )


rule get_disease_efo_relationships:
    input:
        efo_owl_file_path=config["download_directory"] + "/EFO/efo.owl",
        infile=config["intermediate_directory"] + "/disease/ids/EFO",
    output:
        outfile=config["intermediate_directory"] + "/disease/concords/EFO",
        metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-EFO.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_disease_efo_relationships.tsv"
    run:
        diseasephenotype.build_disease_efo_relationships(
            input.efo_owl_file_path, input.infile, output.outfile, output.metadata_yaml
        )


rule get_disease_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/disease/ids/UMLS",
        omim=config["intermediate_directory"] + "/disease/ids/OMIM",
        ncit=config["intermediate_directory"] + "/disease/ids/NCIT",
    output:
        outfile=config["intermediate_directory"] + "/disease/concords/UMLS",
        metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_disease_umls_relationships.tsv"
    run:
        diseasephenotype.build_disease_umls_relationships(
            input.mrconso, input.infile, output.outfile, input.omim, input.ncit, output.metadata_yaml
        )


rule get_disease_doid_relationships:
    input:
        infile=config["download_directory"] + "/DOID/doid.json",
    output:
        outfile=config["intermediate_directory"] + "/disease/concords/DOID",
        metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-DOID.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_disease_doid_relationships.tsv"
    run:
        diseasephenotype.build_disease_doid_relationships(input.infile, output.outfile, output.metadata_yaml)


rule disease_manual_concord:
    input:
        infile="input_data/manual_concords/disease.txt",
    output:
        outfile=config["intermediate_directory"] + "/disease/concords/Manual",
        metadata_yaml=config["intermediate_directory"] + "/disease/concords/metadata-Manual.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/disease_manual_concord.tsv"
    run:
        count_manual_concords = 0
        with open(input.infile, "r") as inp, open(output.outfile, "w") as outp:
            for line in inp:
                # Remove any lines starting with '#', which we treat as comments.
                lstripped_line = line.lstrip()
                if lstripped_line == "" or lstripped_line.startswith("#"):
                    continue
                    # Make sure the line has three tab-delimited values, and fail otherwise.
                elements = lstripped_line.split("\t")
                if len(elements) != 3:
                    raise RuntimeError(
                        f"Found {len(elements)} elements on line {lstripped_line}, expected 3: {elements}"
                    )
                outp.writelines(["\t".join(elements)])
                count_manual_concords += 1
        write_concord_metadata(
            output.metadata_yaml,
            name="Manual Disease/Phenotype Concords",
            description="Manually curated Disease/Phenotype cross-references from the Babel repository",
            sources=[
                {
                    "name": "Babel repository",
                    "url": "https://github.com/NCATSTranslator/Babel",
                }
            ],
            url="https://github.com/NCATSTranslator/Babel/blob/main/input_data/manual_concords/disease.txt",
            concord_filename=output.outfile,
        )


rule disease_compendia:
    input:
        bad_hpo_xrefs="input_data/badHPx.txt",
        bad_mondo_xrefs="input_data/mondo_badxrefs.txt",
        bad_mp_xrefs="input_data/mp_badxrefs.txt",
        bad_umls_xrefs="input_data/umls_badxrefs.txt",
        close_matches=config["intermediate_directory"] + "/disease/concords/MONDO_close",
        labels=expand("{dd}/{ap}/labels", dd=config["download_directory"], ap=config["disease_labelsandsynonyms"]),
        synonyms=expand("{dd}/{ap}/synonyms", dd=config["download_directory"], ap=config["disease_labelsandsynonyms"]),
        concords=expand(
            "{dd}/disease/concords/{ap}", dd=config["intermediate_directory"], ap=config["disease_concords"]
        ),
        metadata_yamls=expand(
            "{dd}/disease/concords/metadata-{ap}.yaml",
            dd=config["intermediate_directory"],
            ap=config["disease_concords"],
        ),
        idlists=expand("{dd}/disease/ids/{ap}", dd=config["intermediate_directory"], ap=config["disease_ids"]),
        # Per-prefix taxa files (read by TaxonFactory in write_compendium to set each
        # identifier's `t` field); declared here so they exist before the compendium is built.
        taxa=expand("{dd}/{ap}/taxa", dd=config["download_directory"], ap=config["disease_phenotype_taxa"]),
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    output:
        expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["disease_outputs"]),
        temp(expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["disease_outputs"])),
        expand("{od}/metadata/{ap}.yaml", od=config["output_directory"], ap=config["disease_outputs"]),
    benchmark:
        config["output_directory"] + "/benchmarks/disease_compendia.tsv"
    run:
        # A new prefix's bad-xref file must be registered HERE (as a rule input above and a key
        # in this dict) and in diseasephenotype.DEFAULT_BAD_XREFS. A key that matches no concord
        # basename (a typo, or the two dicts drifting) is caught by compute_cliques_for_impact_report().
        diseasephenotype.build_compendium(
            input.concords,
            input.metadata_yamls,
            input.idlists,
            input.close_matches,
            {
                "HP": input.bad_hpo_xrefs,
                "MONDO": input.bad_mondo_xrefs,
                "MP": input.bad_mp_xrefs,
                "UMLS": input.bad_umls_xrefs,
            },
            input.icrdf_filename,
        )


rule check_disease_completeness:
    input:
        input_compendia=expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["disease_outputs"]),
    output:
        report_file=config["output_directory"] + "/reports/disease_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_disease_completeness.tsv"
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/disease/ids", input.input_compendia, output.report_file
        )


rule check_disease:
    input:
        infile=config["output_directory"] + "/compendia/Disease.txt",
    output:
        outfile=config["output_directory"] + "/reports/Disease.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_disease.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule check_phenotypic_feature:
    input:
        infile=config["output_directory"] + "/compendia/PhenotypicFeature.txt",
    output:
        outfile=config["output_directory"] + "/reports/PhenotypicFeature.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_phenotypic_feature.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule disease:
    input:
        config["output_directory"] + "/reports/disease_completeness.txt",
        synonyms=expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["disease_outputs"]),
        reports=expand("{od}/reports/{ap}", od=config["output_directory"], ap=config["disease_outputs"]),
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od=config["output_directory"], ap=config["disease_outputs"]),
        x=config["output_directory"] + "/reports/disease_done",
    benchmark:
        config["output_directory"] + "/benchmarks/disease.tsv"
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)


_missing = util.find_missing_id_prefixes(
    workflow, config["intermediate_directory"] + "/disease/ids", config["disease_ids"]
)
if _missing:
    raise WorkflowError(
        f"config.yaml disease_ids has no rule producing intermediate/disease/ids/{{prefix}}: {_missing}"
    )

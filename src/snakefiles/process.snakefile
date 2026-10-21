import src.createcompendia.processactivitypathway as pap
import src.assess_compendia as assessments
import src.snakefiles.util as util

### Process / Activity / Pathway


rule process_go_ids:
    output:
        outfile=config["intermediate_directory"] + "/process/ids/GO",
    benchmark:
        config["output_directory"] + "/benchmarks/process_go_ids.tsv"
    run:
        pap.write_go_ids(output.outfile)


rule process_reactome_ids:
    input:
        infile=config["download_directory"] + "/REACT/Events.json",
    output:
        outfile=config["intermediate_directory"] + "/process/ids/REACT",
    benchmark:
        config["output_directory"] + "/benchmarks/process_reactome_ids.tsv"
    run:
        pap.write_react_ids(input.infile, output.outfile)


rule process_rhea_ids:
    input:
        infile=config["download_directory"] + "/RHEA/labels",
    output:
        outfile=config["intermediate_directory"] + "/process/ids/RHEA",
    benchmark:
        config["output_directory"] + "/benchmarks/process_rhea_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:MolecularActivity\"}}' {input.infile} > {output.outfile}"


rule process_ec_ids:
    input:
        infile=config["download_directory"] + "/EC/enzyme.rdf",
    output:
        outfile=config["intermediate_directory"] + "/process/ids/EC",
    benchmark:
        config["output_directory"] + "/benchmarks/process_ec_ids.tsv"
    run:
        pap.write_ec_ids(input.infile, output.outfile)


rule process_smpdb_ids:
    input:
        infile=config["download_directory"] + "/SMPDB/labels",
    output:
        outfile=config["intermediate_directory"] + "/process/ids/SMPDB",
    benchmark:
        config["output_directory"] + "/benchmarks/process_smpdb_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Pathway\"}}' {input.infile} > {output.outfile}"


rule process_panther_ids:
    input:
        infile=config["download_directory"] + "/PANTHER.PATHWAY/labels",
    output:
        outfile=config["intermediate_directory"] + "/process/ids/PANTHER.PATHWAY",
    benchmark:
        config["output_directory"] + "/benchmarks/process_panther_ids.tsv"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:Pathway\"}}' {input.infile} > {output.outfile}"


rule process_umls_ids:
    input:
        mrsty=config["download_directory"] + "/UMLS/MRSTY.RRF",
    output:
        outfile=config["intermediate_directory"] + "/process/ids/UMLS",
    benchmark:
        config["output_directory"] + "/benchmarks/process_umls_ids.tsv"
    run:
        pap.write_umls_ids(input.mrsty, output.outfile)


### Concords


rule get_process_go_relationships:
    output:
        config["intermediate_directory"] + "/process/concords/GO",
        metadata_yaml=config["intermediate_directory"] + "/process/concords/metadata-GO.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_process_go_relationships.tsv"
    run:
        pap.build_process_obo_relationships(
            config["intermediate_directory"] + "/process/concords", output.metadata_yaml
        )


rule get_process_rhea_relationships:
    input:
        infile=config["download_directory"] + "/RHEA/rhea.rdf",
    output:
        outfile=config["intermediate_directory"] + "/process/concords/RHEA",
        metadata_yaml=config["intermediate_directory"] + "/process/concords/metadata-RHEA.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_process_rhea_relationships.tsv"
    run:
        pap.build_process_rhea_relationships(output.outfile, output.metadata_yaml)


rule get_process_umls_relationships:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
        infile=config["intermediate_directory"] + "/process/ids/UMLS",
    output:
        outfile=config["intermediate_directory"] + "/process/concords/UMLS",
        metadata_yaml=config["intermediate_directory"] + "/process/concords/metadata-UMLS.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_process_umls_relationships.tsv"
    run:
        pap.build_process_umls_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)


rule process_compendia:
    input:
        labels=expand("{dd}/{ap}/labels", dd=config["download_directory"], ap=config["process_labels"]),
        #synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['process_labelsandsynonyms']),
        concords=expand(
            "{dd}/process/concords/{ap}", dd=config["intermediate_directory"], ap=config["process_concords"]
        ),
        metadata_yamls=expand(
            "{dd}/process/concords/metadata-{ap}.yaml",
            dd=config["intermediate_directory"],
            ap=config["process_concords"],
        ),
        idlists=expand("{dd}/process/ids/{ap}", dd=config["intermediate_directory"], ap=config["process_ids"]),
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    output:
        expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["process_outputs"]),
        temp(expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["process_outputs"])),
        expand("{od}/metadata/{ap}.yaml", od=config["output_directory"], ap=config["process_outputs"]),
    benchmark:
        config["output_directory"] + "/benchmarks/process_compendia.tsv"
    run:
        pap.build_compendia(input.concords, input.metadata_yamls, input.idlists, input.icrdf_filename)


rule check_process_completeness:
    input:
        input_compendia=expand("{od}/compendia/{ap}", od=config["output_directory"], ap=config["process_outputs"]),
    output:
        report_file=config["output_directory"] + "/reports/process_completeness.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_process_completeness.tsv"
    run:
        assessments.assess_completeness(
            config["intermediate_directory"] + "/process/ids", input.input_compendia, output.report_file
        )


rule check_process:
    input:
        infile=config["output_directory"] + "/compendia/BiologicalProcess.txt",
    output:
        outfile=config["output_directory"] + "/reports/BiologicalProcess.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_process.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule check_activity:
    input:
        infile=config["output_directory"] + "/compendia/MolecularActivity.txt",
    output:
        outfile=config["output_directory"] + "/reports/MolecularActivity.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_activity.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule check_pathway:
    input:
        infile=config["output_directory"] + "/compendia/Pathway.txt",
    output:
        outfile=config["output_directory"] + "/reports/Pathway.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/check_pathway.tsv"
    run:
        assessments.assess(input.infile, output.outfile)


rule process:
    input:
        config["output_directory"] + "/reports/process_completeness.txt",
        synonyms=expand("{od}/synonyms/{ap}", od=config["output_directory"], ap=config["process_outputs"]),
        reports=expand("{od}/reports/{ap}", od=config["output_directory"], ap=config["process_outputs"]),
    output:
        synonyms_gzipped=expand("{od}/synonyms/{ap}.gz", od=config["output_directory"], ap=config["process_outputs"]),
        x=config["output_directory"] + "/reports/process_done",
    benchmark:
        config["output_directory"] + "/benchmarks/process.tsv"
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)

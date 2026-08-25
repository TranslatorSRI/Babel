from src.snakefiles.util import get_all_compendia, get_all_synonyms_with_drugchemicalconflated
import src.exporters.kgx as kgx
import src.exporters.sapbert as sapbert
import os

### Export compendia/synonyms into downstream outputs


# Trivial aggregation rules run locally so they don't consume a SLURM slot.
localrules:
    export_all_to_kgx,
    export_all_to_sapbert_training,


# Export all compendia to KGX, then create `babel_outputs/kgx/done` to signal that we're done.
rule export_all_to_kgx:
    input:
        nodes_files=expand(
            "{od}/kgx/{fn}",
            od=config["output_directory"],
            fn=map(lambda fn: os.path.splitext(fn)[0] + "_nodes.jsonl.gz", get_all_compendia(config)),
        ),
        edges_files=expand(
            "{od}/kgx/{fn}",
            od=config["output_directory"],
            fn=map(lambda fn: os.path.splitext(fn)[0] + "_edges.jsonl.gz", get_all_compendia(config)),
        ),
    output:
        x=config["output_directory"] + "/kgx/done",
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the KGX files for a particular compendia file.
rule generate_kgx:
    input:
        compendium_file=config["output_directory"] + "/compendia/{filename}.txt",
    output:
        nodes_file=config["output_directory"] + "/kgx/{filename}_nodes.jsonl.gz",
        edges_file=config["output_directory"] + "/kgx/{filename}_edges.jsonl.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_kgx_{filename}.tsv"
    resources:
        # Slowest of the 25 wildcard instances on 2026jul22 was SmallMolecule at 2.7h.
        runtime="4h",
    run:
        kgx.convert_compendium_to_kgx(input.compendium_file, output.nodes_file, output.edges_file)


# Export all synonym files to SAPBERT export, then create `babel_outputs/sapbert-training-data/done` to signal that we're done.
rule export_all_to_sapbert_training:
    input:
        sapbert_training_file=expand(
            "{od}/sapbert-training-data/{fn}.gz",
            od=config["output_directory"],
            fn=get_all_synonyms_with_drugchemicalconflated(config),
        ),
    output:
        x=config["output_directory"] + "/sapbert-training-data/done",
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the KGX files for a particular compendia file.
rule generate_sapbert_training_data:
    input:
        synonym_file_gz=config["output_directory"] + "/synonyms/{filename}.gz",
    output:
        sapbert_training_data_file=config["output_directory"] + "/sapbert-training-data/{filename}.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/generate_sapbert_training_data_{filename}.tsv"
    resources:
        # Slowest of the 18 wildcard instances on 2026jul22 was GeneProteinConflated at 1.9h.
        runtime="3h",
        # The exporter remembers a digest of every synonym pair it writes so it can skip duplicates,
        # so memory grows with the size of the output: GeneProteinConflated is the worst case at
        # roughly 300M pairs x ~95 bytes per set entry. Recheck against the benchmark TSVs after the
        # first run that includes the deduplication (see docs/tools/Resources.md).
        mem="64G",
    run:
        sapbert.convert_synonyms_to_sapbert(input.synonym_file_gz, output.sapbert_training_data_file)

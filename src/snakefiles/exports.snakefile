from src.snakefiles.util import get_all_compendia, get_all_synonyms_with_drugchemicalconflated
import src.exporters.kgx as kgx
import src.exporters.sssom as sssom
import src.exporters.sapbert as sapbert
import os

### Export compendia/synonyms into downstream outputs

# Export all compendia to KGX, then create `babel_outputs/kgx/done` to signal that we're done.
rule export_all_to_kgx:
    input:
        nodes_files=expand("{od}/kgx/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '_nodes.jsonl.gz', get_all_compendia(config))
        ),
        edges_files=expand("{od}/kgx/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '_edges.jsonl.gz', get_all_compendia(config))
        )
    output:
        x = config['output_directory'] + '/kgx/done',
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the KGX files for a particular compendia file.
rule generate_kgx:
    input:
        compendium_file=config['output_directory'] + "/compendia/{filename}.txt",
    output:
        nodes_file=config['output_directory'] + "/kgx/{filename}_nodes.jsonl.gz",
        edges_file=config['output_directory'] + "/kgx/{filename}_edges.jsonl.gz",
    run:
        kgx.convert_compendium_to_kgx(input.compendium_file, output.nodes_file, output.edges_file)


# Export all compendia to SSSOM, then create `babel_outputs/sssom/done` to signal that we're done.
rule export_all_to_sssom:
    input:
        sssom_files=expand("{od}/sssom/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '.tsv.gz', get_all_compendia(config))
        ),
    output:
        x = config['output_directory'] + '/sssom/done',
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the KGX files for a particular compendia file.
rule generate_sssom:
    input:
        compendium_file=config['output_directory'] + "/compendia/{filename}.txt",
    output:
        sssom_file=config['output_directory'] + "/sssom/{filename}.tsv.gz",
    run:
        sssom.convert_compendium_to_sssom(input.compendium_file, output.sssom_file)


# Export all synonym files to SAPBERT export, then create `babel_outputs/sapbert-training-data/done` to signal that we're done.
rule export_all_to_sapbert_training:
    input:
        sapbert_training_file=expand("{od}/sapbert-training-data/{fn}.gz",
            od=config['output_directory'],
            fn=get_all_synonyms_with_drugchemicalconflated(config)
        )
    output:
        x = config['output_directory'] + '/sapbert-training-data/done',
    shell:
        "echo 'done' >> {output.x}"


# Generic rule for generating the SAPBERT training files for a particular compendia file.
rule generate_sapbert_training_data:
    input:
        synonym_file_gz=config['output_directory'] + "/synonyms/{filename}.gz",
    output:
        sapbert_training_data_file=config['output_directory'] + "/sapbert-training-data/{filename}.gz",
    run:
        sapbert.convert_synonyms_to_sapbert(input.synonym_file_gz, output.sapbert_training_data_file)

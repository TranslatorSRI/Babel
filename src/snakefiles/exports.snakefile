import src.exporters.kgx as kgx
import os

### Export compendia/synonyms into downstream outputs

output_filenames = config['anatomy_outputs']

rule export_kgx:
    input:
        nodes_files=expand("{od}/kgx/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '_nodes.jsonl', output_filenames)
        ),
        edges_files=expand("{od}/kgx/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '_edges.jsonl', output_filenames)
        )
    output:
        x = config['output_directory'] + '/kgx/done',
    shell:
        "echo 'done' >> {output.x}"


rule generate_kgx:
    input:
        compendium_file=config['output_directory'] + "/compendia/{filename}.txt",
    output:
        nodes_file=config['output_directory'] + "/kgx/{filename}_nodes.jsonl",
        edges_file=config['output_directory'] + "/kgx/{filename}_edges.jsonl",
    run:
        kgx.convert_compendium_to_kgx(input.compendium_file, output.nodes_file, output.edges_file)

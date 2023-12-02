import src.exporters.kgx as kgx
import os

### Export compendia/synonyms into downstream outputs

rule export_kgx:
    input:
        compendium_files=expand("{od}/compendia/{ap}",
            od=config['output_directory'],
            ap=config['anatomy_outputs']
        ),
    output:
        nodes_files=expand("{od}/kgx/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '_nodes.jsonl', config['anatomy_outputs'])
        ),
        edges_files=expand("{od}/kgx/{fn}",
            od=config['output_directory'],
            fn=map(lambda fn: os.path.splitext(fn)[0] + '_edges.jsonl', config['anatomy_outputs'])
        ),
    run:
        for compendium_file, nodes_file, edges_file in zip(input.compendium_files, output.nodes_files, output.edges_files):
            kgx.convert_compendium_to_kgx(compendium_file, nodes_file, edges_file)

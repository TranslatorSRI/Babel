import src.datahandlers.clo as clo
import src.createcompendia.cell_line as cell_line
import src.assess_compendia as assessments

### Cell Line

# Cell line is pretty simple.  It's just stuff out of CLO.  There are no equivalences.

rule get_clo_ids:
    input:
        infile=config['download_directory']+"/CLO/clo.owl"
    output:
        outfile=config['intermediate_directory']+"/cell_line/ids/CLO"
    run:
        clo.write_clo_ids(input.infile, output.outfile)

### Concords

# Again, CLO doesn't provide any equivalences....

rule cell_line_compendia:
    input:
        ids=config['intermediate_directory']+"/cell_line/ids/CLO",
        icrdf_filename=config['download_directory']+'/icRDF.tsv',
    output:
        config['output_directory']+"/compendia/CellLine.txt",
        config['output_directory']+"/synonyms/CellLine.txt"
    run:
        cell_line.build_compendia(input.ids,input.icrdf_filename)

rule check_cell_line_completeness:
    input:
        input_compendia = config['output_directory']+"/compendia/CellLine.txt",
    output:
        report_file = config['output_directory']+'/reports/cell_line_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/cell_line/ids',[input.input_compendia],output.report_file)

rule check_cell_line:
    input:
        infile=config['output_directory']+'/compendia/CellLine.txt'
    output:
        outfile=config['output_directory']+'/reports/CellLine.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule cell_line:
    input:
        config['output_directory']+'/reports/cell_line_completeness.txt',
        config['output_directory'] + "/synonyms/CellLine.txt",
        config['output_directory'] + "/reports/CellLine.txt"
    output:
        x=config['output_directory']+'/reports/cell_line_done'
    shell:
        "echo 'done' >> {output.x}"
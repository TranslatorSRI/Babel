import src.datahandlers.clo as clo
import src.createcompendia.cell_line as cell_line
import src.assess_compendia as assessments
import src.snakefiles.util as util

### Cell Line

# Cell line is pretty simple.  It's just stuff out of CLO.  There are no equivalences.

rule get_clo_ids:
    input:
        infile=config['download_directory']+"/CLO/clo.owl"
    output:
        outfile=config['intermediate_directory']+"/cell_line/ids/CLO",
    run:
        clo.write_clo_ids(input.infile, output.outfile)

### Concords

# Again, CLO doesn't provide any equivalences....

rule cell_line_compendia:
    input:
        ids=config['intermediate_directory']+"/cell_line/ids/CLO",
        labelfile=config['download_directory'] + '/CLO/labels',
        synonymfile=config['download_directory'] + '/CLO/synonyms',
        metadatafile=config['download_directory'] + '/CLO/metadata.yaml',
        icrdf_filename=config['download_directory']+'/icRDF.tsv',
    output:
        config['output_directory']+"/compendia/CellLine.txt",
        temp(config['output_directory']+"/synonyms/CellLine.txt")
    run:
        cell_line.build_compendia(input.ids, [input.metadatafile], input.icrdf_filename)

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
        config['output_directory'] + "/reports/CellLine.txt",
        cell_line_synonyms=config['output_directory'] + "/synonyms/CellLine.txt",
    output:
        config['output_directory'] + "/synonyms/CellLine.txt.gz",
        x=config['output_directory']+'/reports/cell_line_done'
    run:
        util.gzip_files([input.cell_line_synonyms])
        util.write_done(output.x)

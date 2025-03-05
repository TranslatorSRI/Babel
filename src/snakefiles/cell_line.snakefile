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
        labels=config['download_directory']+"/CLO/labels",
        synonyms=config['download_directory']+"/CLO/synonyms",
        ids=config['intermediate_directory']+"/cell_line/ids/CLO",
        icrdf_filename=config['download_directory']+'/icRDF.tsv',
    output:
        config['output_directory']+"/compendia/CellLine.txt",
        config['output_directory']+"/synonyms/CellLine.txt"
    run:
        cell_line.build_compendia(input.ids,input.labels,input.synonyms,input.icrdf_filename)

#rule check_process_completeness:
#    input:
#        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['process_outputs'])
#    output:
#        report_file = config['output_directory']+'/reports/process_completeness.txt'
#    run:
#        assessments.assess_completeness(config['intermediate_directory']+'/process/ids',input.input_compendia,output.report_file)
#
#rule check_process:
#    input:
#        infile=config['output_directory']+'/compendia/BiologicalProcess.txt'
#    output:
#        outfile=config['output_directory']+'/reports/BiologicalProcess.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule check_activity:
#    input:
#        infile=config['output_directory']+'/compendia/MolecularActivity.txt'
#    output:
#        outfile=config['output_directory']+'/reports/MolecularActivity.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule check_pathway:
#    input:
#        infile=config['output_directory']+'/compendia/Pathway.txt'
#    output:
#        outfile=config['output_directory']+'/reports/Pathway.txt'
#    run:
#        assessments.assess(input.infile, output.outfile)
#
#rule process:
#    input:
#        config['output_directory']+'/reports/process_completeness.txt',
#        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['process_outputs']),
#        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['process_outputs'])
#    output:
#        x=config['output_directory']+'/reports/process_done'
#    shell:
#        "echo 'done' >> {output.x}"
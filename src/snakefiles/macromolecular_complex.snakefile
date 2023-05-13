import src.createcompendia.macromolecular_complex as macromolecular_complex
import src.assess_compendia as assessments

rule macromolecular_complex_ids:
    input:
        infile = config['download_directory']+'/ComplexPortal/559292_labels.tsv'
    output:
        outfile = config['intermediate_directory']+'/macromolecular_complex/ids/ComplexPortal'
    shell:
        "awk '{{print $1\"\tbiolink:MacromolecularComplex\"}}' {input.infile} > {output.outfile}"

rule macromolecular_complex_compendia:
    input:
        labels = config['download_directory']+'/ComplexPortal/559292_labels.tsv',
        synonyms = config['download_directory']+'/ComplexPortal/559292_synonyms.tsv',
        idlists = config['intermediate_directory']+'/macromolecular_complex/ids/ComplexPortal',
    output:
        config['output_directory']+'/compendia/MacromolecularComplex.txt',
        config['output_directory']+'/synonyms/MacromolecularComplex.txt'
    run:
        macromolecular_complex.build_compendia([input.idlists])

rule check_macromolecular_complex_completeness:
    input:
        input_compendia = [config['output_directory']+'/compendia/MacromolecularComplex.txt']
    output:
        report_file = config['output_directory']+'/reports/macromolecular_complex_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/macromolecular_complex/ids', input.input_compendia, output.report_file)

rule check_macromolecular_complex:
    input:
        infile = config['output_directory']+'/compendia/MacromolecularComplex.txt'
    output:
        outfile = config['output_directory']+'/reports/MacromolecularComplex.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule macromolecular_complex:
    input:
        config['output_directory']+'/synonyms/MacromolecularComplex.txt',
        config['output_directory']+'/reports/macromolecular_complex_completeness.txt',
        reports = config['output_directory']+'/reports/MacromolecularComplex.txt'
    output:
        x = config['output_directory']+'/reports/macromolecular_complex_done'
    shell:
        "echo 'done' >> {output.x}"


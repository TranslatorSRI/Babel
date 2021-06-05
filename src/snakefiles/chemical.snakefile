import src.createcompendia.chemicals as chemicals
import src.assess_compendia as assessments

rule chemical_pubchem_ids:
    input:
        infile=config['download_directory']+"/PUBCHEM/labels"
    output:
        outfile=config['download_directory']+"/chemicals/ids/PUBCHEM"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"


rule chemical_chembl_ids:
    input:
        infile=config['download_directory']+"/CHEMBLCOMPOUND/labels"
    output:
        outfile=config['download_directory']+"/chemicals/ids/CHEMBLCOMPOUND"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"

rule chemical_gtopdb_ids:
    input:
        infile=config['download_directory']+"/GTOPDB/labels"
    output:
        outfile=config['download_directory']+"/chemicals/ids/GTOPDB"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"

rule chemical_kegg_ids:
    input:
        infile=config['download_directory']+"/KEGGCOMPOUND/labels"
    output:
        outfile=config['download_directory']+"/chemicals/ids/KEGGCOMPOUND"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"

rule chemical_unii_ids:
    input:
        infile=config['download_directory']+"/UNII/Latest_UNII_Records.txt"
    output:
        outfile=config['download_directory']+"/chemicals/ids/UNII"
    run:
        chemicals.write_unii_ids(input.infile,output.outfile)

rule chemical_hmdb_ids:
    input:
        infile=config['download_directory']+"/HMDB/labels"
    output:
        outfile=config['download_directory']+"/chemicals/ids/HMDB"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"

rule chemical_drugcentral_ids:
    input:
        infile=config['download_directory']+"/DRUGCENTRAL/labels"
    output:
        outfile=config['download_directory']+"/chemicals/ids/DRUGCENTRAL"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"

rule chemical_chebi_ids:
    output:
        outfile=config['download_directory']+"/chemicals/ids/CHEBI"
    run:
        chemicals.write_chebi_ids(output.outfile)

rule chemical_drugbank_ids:
    input:
        infile=config['download_directory']+"/UNICHEM/UC_XREF.srcfiltered.txt"
    output:
        outfile=config['download_directory']+"/chemicals/ids/DRUGBANK"
    run:
        chemicals.write_drugbank_ids(input.infile,output.outfile)


######

rule get_protein_uniprotkb_ensembl_relationships:
    input:
        infile = config['download_directory'] + '/UniProtKB/idmapping.dat'
    output:
        outfile = config['download_directory'] + '/protein/concords/UniProtKB'
    run:
        protein.build_protein_uniprotkb_ensemble_relationships(input.infile,output.outfile)

rule chemical_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['protein_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['protein_synonyms']),
        concords=expand("{dd}/protein/concords/{ap}",dd=config['download_directory'],ap=config['protein_concords']),
        idlists=expand("{dd}/protein/ids/{ap}",dd=config['download_directory'],ap=config['protein_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['protein_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['protein_outputs'])
    run:
        protein.build_protein_compendia(input.concords,input.idlists)

rule check_chemical_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['protein_outputs'])
    output:
        report_file = config['output_directory']+'/reports/protein_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/protein/ids',input.input_compendia,output.report_file)

rule check_chemical:
    input:
        infile=config['output_directory']+'/compendia/Protein.txt'
    output:
        outfile=config['output_directory']+'/reports/Protein.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule chemical:
    input:
        config['output_directory']+'/reports/protein_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['protein_outputs'])
    output:
        x=config['output_directory']+'/reports/protein_done'
    shell:
        "echo 'done' >> {output.x}"
import src.createcompendia.chemicals as chemicals
import src.assess_compendia as assessments

rule chemical_mesh_ids:
    input:
        infile=config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['download_directory']+'/chemicals/ids/MESH'
    run:
        chemicals.write_mesh_ids(output.outfile)

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

rule get_chemical_wikipedia_relationships:
    output:
        outfile = config['download_directory'] + '/chemicals/concords/wikipedia_mesh_chebi'
    run:
        chemicals.get_wikipedia_relationships(output.outfile)

rule get_chemical_mesh_relationships:
    output:
        casout = config['download_directory'] + '/chemicals/concords/mesh_cas',
        uniout = config['download_directory'] + '/chemicals/concords/mesh_unii'
    run:
        chemicals.get_mesh_relationships(output.casout,output.uniout)

#This is about a 2 hour step and requires something more than 256G of RAM.  512G works.
rule get_chemical_unichem_relationships:
    input:
        structfile = config['download_directory'] + '/UNICHEM/UC_STRUCTURE.txt',
        reffile = config['download_directory'] + '/UNICHEM/UC_XREF.srcfiltered.txt'
    output:
        outfiles = expand('{dd}/chemicals/concords/UNICHEM/UNICHEM_{ucc}',dd=config['download_directory'], ucc=config['unichem_datasources'] )
    run:
        chemicals.write_unichem_concords(input.structfile,input.reffile,config['download_directory']+'/chemicals/concords')

rule get_chemical_pubchem_mesh_concord:
    input:
        pubchemfile=config['download_directory'] + '/PUBCHEM/CID-MeSH',
        meshlabels=config['download_directory'] + '/MESH/labels'
    output:
        outfile =  config['download_directory'] + '/chemicals/concord/PUBCHEM_MESH'
    run:
        chemicals.make_pubchem_mesh_concord(input.pubchemfile,input.meshlabels,output.outfile)

rule get_chemical_pubchem_cas_concord:
    input:
        pubchemsynonyms=config['download_directory'] + '/PUBCHEM/synonyms'
    output:
        outfile = config['download_directory'] + '/chemicals/concord/PUBCHEM_CAS'
    run:
        chemicals.make_pubchem_cas_concord(input.pubchemsynonyms, output.outfile)

# There are some gtopdb inchikey relations that for some reason are not in unichem
rule get_gtopdb_inchikey_concord:
    input:
        infile=config['download_directory']+'/GTOPDB/ligands.tsv'
    output:
        outfile=config['download_directory'] + '/chemicals/concords/GTOPDB'
    run:
        chemicals.make_gtopdb_relations(input.infile,output.outfile)

rule get_chebi_concord:
    input:
        sdf=config['download_directory']+'/CHEBI/ChEBI_complete.sdf',
        dbx=config['download_directory']+'/CHEBI/database_accession.tsv'
    output:
        outfile=config['download_directory']+'/chemicals/concords/CHEBI'
    run:
        chemicals.make_chebi_relations(input.sdf,input.dbx,output.outfile)

rule chemical_unichem_concordia:
    input:
        concords = expand('{dd}/chemicals/concords/UNICHEM/UNICHEM_{ucc}',dd=config['download_directory'], ucc=config['unichem_datasources'] ),
    output:
        unichemgroup = config['download_directory']+'/chemicals/partials/UNICHEM'
    run:
        chemicals.combine_unichem(input.concords,output.unichemgroup)

rule chemical_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['chemical_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['chemical_synonyms']),
        unichemgroup = config['download_directory']+'/chemicals/partials/UNICHEM',
        concords = expand('{dd}/chemicals/concords/{cc}',dd=config['download_directory'], cc=config['chemical_concords'] ),
        idlists=expand("{dd}/chemicals/ids/{ap}",dd=config['download_directory'],ap=config['chemical_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['chemical_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['chemical_outputs'])
    run:
        chemicals.build_compendia(input.concords,input.idlists,input.unichemgroup)

rule check_chemical_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['chemical_outputs'])
    output:
        report_file = config['output_directory']+'/reports/chemical_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/chemical/ids',input.input_compendia,output.report_file)

rule check_chemical:
    input:
        infile=config['output_directory']+'/compendia/ChemicalSubstance.txt'
    output:
        outfile=config['output_directory']+'/reports/ChemicalSubstance.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule chemical:
    input:
        config['output_directory']+'/reports/chemical_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['chemical_outputs'])
    output:
        x=config['output_directory']+'/reports/chemicals_done'
    shell:
        "echo 'done' >> {output.x}"
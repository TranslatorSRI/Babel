import src.createcompendia.chemicals as chemicals
import src.assess_compendia as assessments
import src.snakefiles.util as util

rule chemical_umls_ids:
    input:
        mrsty=config['download_directory'] + "/UMLS/MRSTY.RRF"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/UMLS"
    run:
        chemicals.write_umls_ids(input.mrsty, output.outfile)

rule chemical_rxnorm_ids:
    input:
        infile=config['download_directory']+"/RxNorm/RXNCONSO.RRF"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/RXNORM"
    run:
        chemicals.write_rxnorm_ids(input.infile, output.outfile)

rule chemical_mesh_ids:
    input:
        infile=config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['intermediate_directory']+'/chemicals/ids/MESH'
    run:
        chemicals.write_mesh_ids(output.outfile)

rule chemical_pubchem_ids:
    input:
        infile=config['download_directory']+"/PUBCHEM.COMPOUND/labels",
        smilesfile=config['download_directory']+"/PUBCHEM.COMPOUND/CID-SMILES.gz"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/PUBCHEM.COMPOUND"
    run:
        #This one is a simple enough transform to do with awk
        chemicals.write_pubchem_ids(input.infile,input.smilesfile,output.outfile)
        #"awk '{{print $1\"\tbiolink:ChemicalSubstance\"}}' {input.infile} > {output.outfile}"

rule chemical_chembl_ids:
    input:
        labelfile=config['download_directory']+"/CHEMBL.COMPOUND/labels",
        smifile  =config['download_directory'] + "/CHEMBL.COMPOUND/smiles"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/CHEMBL.COMPOUND"
    run:
        chemicals.write_chemical_ids_from_labels_and_smiles(input.labelfile,input.smifile,output.outfile)

rule chemical_gtopdb_ids:
    input:
        infile=config['download_directory']+"/GTOPDB/ligands.tsv"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/GTOPDB"
    run:
        chemicals.write_gtopdb_ids(input.infile,output.outfile)

rule chemical_kegg_ids:
    input:
        infile=config['download_directory']+"/KEGG.COMPOUND/labels"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/KEGG.COMPOUND"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1\"\tbiolink:ChemicalEntity\"}}' {input.infile} > {output.outfile}"

rule chemical_unii_ids:
    input:
        infile=config['download_directory']+"/UNII/Latest_UNII_Records.txt"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/UNII"
    run:
        chemicals.write_unii_ids(input.infile,output.outfile)

rule chemical_hmdb_ids:
    input:
        labelfile=config['download_directory']+"/HMDB/labels",
        smifile=config['download_directory'] + "/HMDB/smiles"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/HMDB"
    run:
        chemicals.write_chemical_ids_from_labels_and_smiles(input.labelfile,input.smifile,output.outfile)

rule chemical_drugcentral_ids:
    input:
        structfile=config['download_directory']+"/DrugCentral/structures"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/DrugCentral"
    run:
        chemicals.write_drugcentral_ids(input.structfile,output.outfile)

rule chemical_chebi_ids:
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/CHEBI"
    run:
        chemicals.write_chebi_ids(output.outfile)

rule chemical_drugbank_ids:
    input:
        infile=config['download_directory']+"/UNICHEM/reference.filtered.tsv"
    output:
        outfile=config['intermediate_directory']+"/chemicals/ids/DRUGBANK"
    run:
        chemicals.write_drugbank_ids(input.infile,output.outfile)


######

rule get_chemical_drugcentral_relationships:
    input:
        xreffile=config['download_directory']+"/DrugCentral/xrefs"
    output:
        outfile=config['intermediate_directory']+'/chemicals/concords/DrugCentral',
        metadata_yaml=config['intermediate_directory']+'/chemicals/concords/metadata-DrugCentral.yaml',
    run:
        chemicals.build_drugcentral_relations(input.xreffile,output.outfile, output.metadata_yaml)

rule get_chemical_umls_relationships:
    input:
        mrconso=config['download_directory']+"/UMLS/MRCONSO.RRF",
        infile=config['intermediate_directory']+"/chemicals/ids/UMLS",
    output:
        outfile=config['intermediate_directory']+'/chemicals/concords/UMLS',
        metadata_yaml=config['intermediate_directory']+'/chemicals/concords/metadata-UMLS.yaml',
    run:
        chemicals.build_chemical_umls_relationships(input.mrconso, input.infile, output.outfile, output.metadata_yaml)

rule get_chemical_rxnorm_relationships:
    input:
        infile=config['intermediate_directory']+"/chemicals/ids/RXNORM",
        conso=config['download_directory'] + "/RxNorm/RXNCONSO.RRF"
    output:
        outfile=config['intermediate_directory']+'/chemicals/concords/RXNORM',
        metadata_yaml=config['intermediate_directory']+'/chemicals/concords/metadata-RXNORM.yaml',
    run:
        chemicals.build_chemical_rxnorm_relationships(input.conso, input.infile, output.outfile, output.metadata_yaml)

rule get_chemical_wikipedia_relationships:
    output:
        outfile = config['intermediate_directory'] + '/chemicals/concords/wikipedia_mesh_chebi',
        metadata_yaml = config['intermediate_directory'] + '/chemicals/concords/metadata-wikipedia_mesh_chebi.yaml'
    run:
        chemicals.get_wikipedia_relationships(output.outfile, output.metadata_yaml)

rule get_chemical_mesh_relationships:
    input:
        infile = config['intermediate_directory'] + '/chemicals/ids/MESH'
    output:
        casout = config['intermediate_directory'] + '/chemicals/concords/mesh_cas',
        uniout = config['intermediate_directory'] + '/chemicals/concords/mesh_unii',
        casout_metadata_yaml = config['intermediate_directory'] + '/chemicals/concords/metadata-mesh_cas.yaml',
        uniout_metadata_yaml = config['intermediate_directory'] + '/chemicals/concords/metadata-mesh_unii.yaml',
    run:
        chemicals.get_mesh_relationships(input.infile,output.casout,output.uniout,output.casout_metadata_yaml,output.uniout_metadata_yaml)

#This is about a 2 hour step and requires something more than 256G of RAM.  512G works.
rule get_chemical_unichem_relationships:
    input:
        structfile = config['download_directory'] + '/UNICHEM/structure.tsv.gz',
        reffile = config['download_directory'] + '/UNICHEM/reference.filtered.tsv'
    output:
        outfiles = expand('{dd}/chemicals/concords/UNICHEM/UNICHEM_{ucc}',dd=config['intermediate_directory'], ucc=config['unichem_datasources'] )
    run:
        chemicals.write_unichem_concords(input.structfile,input.reffile,config['intermediate_directory']+'/chemicals/concords/UNICHEM')

rule get_chemical_pubchem_mesh_concord:
    input:
        pubchemfile=config['download_directory'] + '/PUBCHEM.COMPOUND/CID-MeSH',
        meshlabels=config['download_directory'] + '/MESH/labels'
    output:
        outfile =  config['intermediate_directory'] + '/chemicals/concords/PUBCHEM_MESH',
        metadata_yaml = config['intermediate_directory'] + '/chemicals/concords/metadata-PUBCHEM_MESH.yaml'
    run:
        chemicals.make_pubchem_mesh_concord(input.pubchemfile,input.meshlabels,output.outfile, output.metadata_yaml)

rule get_chemical_pubchem_cas_concord:
    input:
        pubchemsynonyms=config['download_directory'] + '/PUBCHEM.COMPOUND/synonyms'
    output:
        outfile = config['intermediate_directory'] + '/chemicals/concords/PUBCHEM_CAS',
        metadata_yaml = config['intermediate_directory'] + '/chemicals/concords/metadata-PUBCHEM_CAS.yaml'
    run:
        chemicals.make_pubchem_cas_concord(input.pubchemsynonyms, output.outfile, output.metadata_yaml)

# There are some gtopdb inchikey relations that for some reason are not in unichem
rule get_gtopdb_inchikey_concord:
    input:
        infile=config['download_directory']+'/GTOPDB/ligands.tsv'
    output:
        outfile=config['intermediate_directory'] + '/chemicals/concords/GTOPDB',
        metadata_yaml=config['intermediate_directory'] + '/chemicals/concords/metadata-GTOPDB.yaml',
    run:
        chemicals.make_gtopdb_relations(input.infile,output.outfile, output.metadata_yaml)

rule get_chebi_concord:
    input:
        sdf=config['download_directory']+'/CHEBI/ChEBI_complete.sdf',
        dbx=config['download_directory']+'/CHEBI/database_accession.tsv'
    output:
        outfile=config['intermediate_directory']+'/chemicals/concords/CHEBI',
        metadata_yaml=config['intermediate_directory']+'/chemicals/concords/metadata-CHEBI.yaml'
    run:
        chemicals.make_chebi_relations(input.sdf,input.dbx,output.outfile, output.metadata_yaml)

rule chemical_unichem_concordia:
    input:
        concords = expand('{dd}/chemicals/concords/UNICHEM/UNICHEM_{ucc}',dd=config['intermediate_directory'], ucc=config['unichem_datasources'] ),
    output:
        unichemgroup = config['intermediate_directory']+'/chemicals/partials/UNICHEM'
    run:
        chemicals.combine_unichem(input.concords,output.unichemgroup)

rule untyped_chemical_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['chemical_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['chemical_synonyms']),
        unichemgroup = config['intermediate_directory']+'/chemicals/partials/UNICHEM',
        concords = expand('{dd}/chemicals/concords/{cc}',dd=config['intermediate_directory'], cc=config['chemical_concords'] ),
        metadata_yamls = expand('{dd}/chemicals/concords/metadata-{cc}.yaml',dd=config['intermediate_directory'], cc=config['chemical_concords'] ),
        idlists=expand("{dd}/chemicals/ids/{ap}",dd=config['intermediate_directory'],ap=config['chemical_ids']),
    output:
        typesfile    = config['intermediate_directory'] + '/chemicals/partials/types',
        untyped_file = config['intermediate_directory'] + '/chemicals/partials/untyped_compendium',
        untyped_meta = config['intermediate_directory'] + '/chemicals/partials/metadata-untyped_compendium.yaml'
    run:
        chemicals.build_untyped_compendia(input.concords,input.idlists,input.unichemgroup,output.untyped_file,output.typesfile, output.untyped_meta, input.metadata_yamls)


rule chemical_compendia:
    input:
        typesfile    = config['intermediate_directory'] + '/chemicals/partials/types',
        untyped_file = config['intermediate_directory'] + '/chemicals/partials/untyped_compendium',
        metadata_yamls = config['intermediate_directory'] + '/chemicals/partials/metadata-untyped_compendium.yaml',
        icrdf_filename = config['download_directory'] + '/icRDF.tsv',
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['chemical_outputs']),
        temp(expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['chemical_outputs'])),
        expand("{od}/metadata/{ap}.yaml", od = config['output_directory'], ap = config['chemical_outputs']),
    run:
        chemicals.build_compendia(input.typesfile, input.untyped_file, [input.metadata_yamls], input.icrdf_filename)

rule check_chemical_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['chemical_outputs'])
    output:
        report_file = config['output_directory']+'/reports/chemical_completeness.txt'
    run:
        assessments.assess_completeness(config['intermediate_directory']+'/chemicals/ids',input.input_compendia,output.report_file)

rule check_chemical_entity:
    input:
        infile=config['output_directory']+'/compendia/ChemicalEntity.txt'
    output:
        outfile=config['output_directory']+'/reports/ChemicalEntity.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_molecular_mixture:
    input:
        infile=config['output_directory']+'/compendia/MolecularMixture.txt'
    output:
        outfile=config['output_directory']+'/reports/MolecularMixture.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_small_molecule:
    input:
        infile=config['output_directory']+'/compendia/SmallMolecule.txt'
    output:
        outfile=config['output_directory']+'/reports/SmallMolecule.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_polypeptide:
    input:
        infile=config['output_directory']+'/compendia/Polypeptide.txt'
    output:
        outfile=config['output_directory']+'/reports/Polypeptide.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_complex_mixture:
    input:
        infile=config['output_directory']+'/compendia/ComplexMolecularMixture.txt'
    output:
        outfile=config['output_directory']+'/reports/ComplexMolecularMixture.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_chemical_mixture:
    input:
        infile=config['output_directory']+'/compendia/ChemicalMixture.txt'
    output:
        outfile=config['output_directory']+'/reports/ChemicalMixture.txt'
    run:
        assessments.assess(input.infile, output.outfile)


rule check_drug:
    input:
        infile=config['output_directory']+'/compendia/Drug.txt'
    output:
        outfile=config['output_directory']+'/reports/Drug.txt'
    run:
        assessments.assess(input.infile, output.outfile)


rule chemical:
    input:
        config['output_directory']+'/reports/chemical_completeness.txt',
        synonyms = expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['chemical_outputs']),
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['chemical_outputs']),
        metadata = expand("{od}/metadata/{ap}.yaml", od = config['output_directory'], ap = config['chemical_outputs']),
    output:
        synonyms_gzipped = expand("{od}/synonyms/{ap}.gz", od = config['output_directory'], ap = config['chemical_outputs']),
        x=config['output_directory']+'/reports/chemicals_done'
    run:
        util.gzip_files(input.synonyms)
        util.write_done(output.x)

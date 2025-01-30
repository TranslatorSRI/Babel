import src.createcompendia.drugchemical as drugchemical
import src.synonyms.synonymconflation as synonymconflation
import src.snakefiles.util as util

### Drug / Chemical

rule rxnorm_relationships:
    input:
        rxnconso = config['download_directory'] + "/RxNorm/RXNCONSO.RRF",
        rxnrel = config['download_directory'] + "/RxNorm/RXNREL.RRF",
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/RXNORM'
    run:
        drugchemical.build_rxnorm_relationships(input.rxnconso, input.rxnrel, output.outfile_concords)

rule umls_relationships:
    input:
        umlsconso = config['download_directory'] + "/UMLS/MRCONSO.RRF",
        umlsrel = config['download_directory'] + "/UMLS/MRREL.RRF",
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/UMLS'
    run:
        drugchemical.build_rxnorm_relationships(input.umlsconso, input.umlsrel, output.outfile_concords)

rule pubchem_rxnorm_relationships:
    input:
        infile = config['download_directory'] + '/PUBCHEM.COMPOUND/RXNORM.json',
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/PUBCHEM_RXNORM'
    run:
        drugchemical.build_pubchem_relationships(input.infile,output.outfile_concords)

rule drugchemical_conflation:
    input:
        drug_compendium=config['output_directory']+'/compendia/'+'Drug.txt',
        chemical_compendia=expand("{do}/compendia/{co}", do=config['output_directory'], co=config['chemical_outputs']),
        rxnorm_concord=config['intermediate_directory']+'/drugchemical/concords/RXNORM',
        umls_concord=config['intermediate_directory']+'/drugchemical/concords/UMLS',
        pubchem_concord=config['intermediate_directory']+'/drugchemical/concords/PUBCHEM_RXNORM',
        drugchemical_manual_concord=config['input_directory']+'/manual_concords/drugchemical.tsv',
    output:
        outfile=config['output_directory']+'/conflation/DrugChemical.txt'
    run:
        drugchemical.build_conflation(input.drugchemical_manual_concord,input.rxnorm_concord,input.umls_concord,input.pubchem_concord,input.drug_compendium,input.chemical_compendia,output.outfile)

rule drugchemical_conflated_synonyms:
    input:
        drugchemical_conflation=[config['output_directory']+'/conflation/DrugChemical.txt'],
        chemical_compendia=expand("{do}/compendia/{co}", do=config['output_directory'], co=config['chemical_outputs']),
        chemical_synonyms=expand("{do}/synonyms/{co}", do=config['output_directory'], co=config['chemical_outputs']),
    output:
        drugchemical_conflated=config['output_directory']+'/synonyms/DrugChemicalConflated.txt',
    run:
        synonymconflation.conflate_synonyms(input.chemical_synonyms, input.chemical_compendia, input.drugchemical_conflation, output.drugchemical_conflated)

rule drugchemical:
    input:
        config['output_directory']+'/conflation/DrugChemical.txt',
        config['output_directory']+'/synonyms/DrugChemicalConflated.txt',
        chemical_synonyms=expand("{do}/synonyms/{co}", do=config['output_directory'], co=config['chemical_outputs']),
    output:
        chemical_synonyms_gzipped=expand("{do}/synonyms/{co}.gz", do=config['output_directory'], co=config['chemical_outputs']),
        x=config['output_directory']+'/reports/drugchemical_done'
    run:
        util.gzip_files(input.chemical_synonyms)
        util.write_done(output.x)

import src.createcompendia.drugchemical as drugchemical
import src.synonyms.synonymconflation as synonymconflation
import src.snakefiles.util as util
from src.metadata.provenance import write_concord_metadata

### Drug / Chemical

rule rxnorm_relationships:
    input:
        rxnconso = config['download_directory'] + "/RxNorm/RXNCONSO.RRF",
        rxnrel = config['download_directory'] + "/RxNorm/RXNREL.RRF",
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/RXNORM',
        metadata_yaml = config['intermediate_directory'] + '/drugchemical/concords/metadata-RXNORM.yaml'
    run:
        drugchemical.build_rxnorm_relationships(input.rxnconso, input.rxnrel, output.outfile_concords, output.metadata_yaml)

rule umls_relationships:
    input:
        umlsconso = config['download_directory'] + "/UMLS/MRCONSO.RRF",
        umlsrel = config['download_directory'] + "/UMLS/MRREL.RRF",
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/UMLS',
        metadata_yaml = config['intermediate_directory'] + '/drugchemical/concords/metadata-UMLS.yaml'
    run:
        drugchemical.build_rxnorm_relationships(input.umlsconso, input.umlsrel, output.outfile_concords, output.metadata_yaml)

rule pubchem_rxnorm_relationships:
    input:
        infile = config['download_directory'] + '/PUBCHEM.COMPOUND/RXNORM.json',
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/PUBCHEM_RXNORM',
        metadata_yaml = config['intermediate_directory'] + '/drugchemical/concords/metadata-PUBCHEM_RXNORM.yaml'
    run:
        drugchemical.build_pubchem_relationships(input.infile,output.outfile_concords, output.metadata_yaml)

rule drugchemical_conflation:
    input:
        drug_compendium=config['output_directory']+'/compendia/'+'Drug.txt',
        chemical_compendia=expand("{do}/compendia/{co}", do=config['output_directory'], co=config['chemical_outputs']),
        rxnorm_concord=config['intermediate_directory']+'/drugchemical/concords/RXNORM',
        rxnorm_metadata=config['intermediate_directory']+'/drugchemical/concords/metadata-RXNORM.yaml',
        umls_concord=config['intermediate_directory']+'/drugchemical/concords/UMLS',
        umls_metadata=config['intermediate_directory']+'/drugchemical/concords/metadata-UMLS.yaml',
        pubchem_concord=config['intermediate_directory']+'/drugchemical/concords/PUBCHEM_RXNORM',
        pubchem_metadata=config['intermediate_directory']+'/drugchemical/concords/metadata-PUBCHEM_RXNORM.yaml',
        drugchemical_manual_concord=config['input_directory']+'/manual_concords/drugchemical.tsv',
        icrdf_filename=config['download_directory']+'/icRDF.tsv',
    output:
        outfile=config['output_directory']+'/conflation/DrugChemical.txt',
        metadata_yaml=config['output_directory']+'/conflation/metadata.yaml',
        drugchemical_manual_metadata=config['intermediate_directory']+'/drugchemical/concords/metadata-Manual.yaml',
    run:
        write_concord_metadata(input.drugchemical_manual_metadata,
            name='Manual DrugChemical Concords',
            description='Manually curated DrugChemical conflation cross-references from the Babel repository',
            sources=[{
                'name': 'Babel repository',
                'url': 'https://github.com/TranslatorSRI/Babel',
            }],
            url='https://github.com/TranslatorSRI/Babel/blob/master/input_data/manual_concords/drugchemical.tsv',
            concord_filename=input.drugchemical_manual_concord,
        )
        drugchemical.build_conflation(
            input.drugchemical_manual_concord,
            input.rxnorm_concord,
            input.umls_concord,
            input.pubchem_concord,
            input.drug_compendium,
            input.chemical_compendia,
            input.icrdf_filename,
            output.outfile,
            input_metadata_yamls={
                'RXNORM': input.rxnorm_metadata,
                'UMLS': input.umls_metadata,
                'PUBCHEM_RXNORM': input.pubchem_metadata,
                'Manual': input.drugchemical_manual_metadata,
            }, output_metadata_yaml=output.metadata_yaml)

rule drugchemical_conflated_synonyms:
    input:
        drugchemical_conflation=[config['output_directory']+'/conflation/DrugChemical.txt'],
        chemical_compendia=expand("{do}/compendia/{co}", do=config['output_directory'], co=config['chemical_outputs']),
        chemical_synonyms_gz=expand("{do}/synonyms/{co}.gz", do=config['output_directory'], co=config['chemical_outputs']),
    output:
        drugchemical_conflated_gz=config['output_directory']+'/synonyms/DrugChemicalConflated.txt.gz',
    run:
        synonymconflation.conflate_synonyms(input.chemical_synonyms_gz, input.chemical_compendia, input.drugchemical_conflation, output.drugchemical_conflated_gz)

rule drugchemical:
    input:
        config['output_directory']+'/conflation/DrugChemical.txt',
        config['output_directory']+'/synonyms/DrugChemicalConflated.txt.gz',
    output:
        done=config['output_directory']+'/reports/drugchemical_done'
    run:
        util.write_done(output.done)

import src.createcompendia.drugchemical as drugchemical
import src.assess_compendia as assessments

### Drug / Chemical

rule rxnorm_relationships:
    #Expects input_data/private/RXNREL.RRF to exist
    output:
        outfile_concords = config['intermediate_directory'] + '/drugchemical/concords/RXNORM'
    run:
        drugchemical.build_rxnorm_relationships(output.outfile_concords)

rule drugchemical_conflation:
    input:
        drug_compendium=config['output_directory']+'/compendia/'+'Drug.txt',
        chemical_compendia=expand("{do}/compendia/{co}", do=config['output_directory'], co=config['chemical_outputs']),
        drug_chemical_concord=config['intermediate_directory']+'/drugchemical/concords/RXNORM'
    output:
        outfile=config['output_directory']+'/conflation/DrugChemical.txt'
    run:
        drugchemical.build_conflation(input.drug_chemical_concord,input.drug_compendium,input.chemical_compendia,output.outfile)

rule drugchemical:
    input:
        config['output_directory']+'/conflation/DrugChemical.txt'
    output:
        x=config['output_directory']+'/reports/drugchemical_done'
    shell:
        "echo 'done' >> {output.x}"

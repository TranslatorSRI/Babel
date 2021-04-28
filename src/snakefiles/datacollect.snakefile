import src.datahandlers.mesh as mesh
import src.datahandlers.obo as obo
import src.datahandlers.umls as umls
import src.datahandlers.ncbigene as ncbigene
import src.datahandlers.ensembl as ensembl
import src.datahandlers.hgnc as hgnc
import src.datahandlers.omim as omim
import src.datahandlers.uniprotkb as uniprotkb
import src.createcompendia.anatomy as anatomy
import src.createcompendia.geneprotein as geneprotein
import src.assess_compendia as assessments

configfile: "config.json"

rule all:
    input:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])

rule clean_compendia:
    params:
        dir=config['output_directory']
    shell:
        "rm {params.dir}/compendia/*; rm {params.dir}/synonyms/*"

rule clean_data:
    params:
        dir=config['download_directory']
    shell:
        "rm -rf {params.dir}/*"

#####
#
# Data sets: pull data sets, and parse them to get labels and synonyms
#
####

### UniProtKB

rule get_uniprotkb:
    output:
        config['download_directory']+'/UniProtKB/uniprot_sprot.fasta',
        config['download_directory']+'/UniProtKB/uniprot_trembl.fasta'
    run:
        uniprotkb.pull_uniprotkb()

rule get_uniprotkb_labels:
    input:
        sprot_input=config['download_directory']+'/UniProtKB/uniprot_sprot.fasta',
        trembl_input=config['download_directory']+'/UniProtKB/uniprot_trembl.fasta',
    output:
        config['download_directory']+'/UniProtKB/labels'
    run:
        mesh.pull_mesh_labels()

### MESH

rule get_mesh:
    output:
        config['download_directory']+'/MESH/mesh.nt'
    run:
        mesh.pull_mesh()

rule get_mesh_labels:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        config['download_directory']+'/MESH/labels'
    run:
        mesh.pull_mesh_labels()

rule get_mesh_synonyms:
    #We don't actually get any.  Maybe we could from the nt?
    output:
        ofn=config['download_directory']+'/MESH/synonyms'
    shell:
        "touch {output.ofn}"

### UMLS / SNOMEDCT

rule get_umls_labels_and_synonyms:
    output:
        config['download_directory']+'/UMLS/labels',
        config['download_directory']+'/UMLS/synonyms',
        config['download_directory']+'/SNOMEDCT/labels',
        config['download_directory']+'/SNOMEDCT/synonyms'
    run:
        umls.pull_umls()

### OBO Ontologies

rule get_ontology_labels_and_synonyms:
    output:
        expand("{download_directory}/{onto}/labels", download_directory = config['download_directory'], onto = config['ubergraph_ontologies']),
        expand("{download_directory}/{onto}/synonyms", download_directory = config['download_directory'], onto = config['ubergraph_ontologies'])
    run:
        obo.pull_uber(config['ubergraph_ontologies'])

### NCBIGene

rule get_ncbigene:
    output:
        getfiles=expand("{download_directory}/NCBIGene/{ncbi_files}", download_directory=config['download_directory'],ncbi_files=config['ncbi_files'])
    run:
        ncbigene.pull_ncbigene(config['ncbi_files'])

rule get_ncbigene_labels_and_synonyms:
    output:
        config['download_directory']+'/NCBIGene/labels',
        config['download_directory']+'/NCBIGene/synonyms'
    input:
        config['download_directory']+'/NCBIGene/gene_info.gz'
    run:
        ncbigene.pull_ncbigene_labels_and_synonyms()

### ENSEMBL

rule get_ensembl:
    output:
        outfile=config['download_directory']+'/ENSEMBL/BioMartDownloadComplete'
    run:
        ensembl.pull_ensembl(output.outfile)

### HGNC

rule get_hgnc:
    output:
        outfile=config['download_directory']+'/HGNC/hgnc_complete_set.json'
    run:
        hgnc.pull_hgnc()

rule get_hgnc_labels_and_synonyms:
    output:
        config['download_directory']+'/HGNC/labels',
        config['download_directory']+'/HGNC/synonyms'
    input:
        infile=rules.get_hgnc.output.outfile
    run:
        hgnc.pull_hgnc_labels_and_synonyms(input.infile)

### OMIM

rule get_omim:
    output:
        outfile=config['download_directory']+'/OMIM/mim2gene.txt'
    run:
        omim.pull_omim()

####
#
# Categories: For a given biolink class (or set of related classes), build files showing possible cross-vocabulary
#   relationships. Then combine those to create the compendia and related synonym files.
#
####

### AnatomicalEntity / Cell / CellularComponent

rule anatomy_uberon_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/UBERON"
    run:
        anatomy.write_uberon_ids(output.outfile)

rule anatomy_cl_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/CL"
    run:
        anatomy.write_cl_ids(output.outfile)

rule anatomy_go_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/GO"
    run:
        anatomy.write_go_ids(output.outfile)

rule anatomy_ncit_ids:
    output:
        outfile=config['download_directory']+"/anatomy/ids/NCIT"
    run:
        anatomy.write_ncit_ids(output.outfile)

rule anatomy_mesh_ids:
    input:
        config['download_directory']+'/MESH/mesh.nt'
    output:
        outfile=config['download_directory']+"/anatomy/ids/MESH"
    run:
        anatomy.write_mesh_ids(output.outfile)

rule anatomy_umls_ids:
    #The location of the RRFs is known to the guts, but should probably come out here.
    output:
        outfile=config['download_directory']+"/anatomy/ids/UMLS"
    run:
        anatomy.write_umls_ids(output.outfile)

rule get_anatomy_obo_relationships:
    output:
        config['download_directory']+'/anatomy/concords/UBERON',
        config['download_directory']+'/anatomy/concords/CL',
        config['download_directory']+'/anatomy/concords/GO',
    run:
        anatomy.build_anatomy_obo_relationships(config['download_directory']+'/anatomy/concords')

rule get_anatomy_umls_relationships:
    input:
        infile=config['download_directory']+"/anatomy/ids/UMLS"
    output:
        outfile=config['download_directory']+'/anatomy/concords/UMLS',
    run:
        anatomy.build_anatomy_umls_relationships(input.infile,output.outfile)

rule anatomy_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['anatomy_prefixes']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['anatomy_prefixes']),
        concords=expand("{dd}/anatomy/concords/{ap}",dd=config['download_directory'],ap=config['anatomy_concords']),
        idlists=expand("{dd}/anatomy/ids/{ap}",dd=config['download_directory'],ap=config['anatomy_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
    run:
        anatomy.build_compendia(input.concords,input.idlists)

rule check_anatomy_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['anatomy_outputs'])
    output:
        report_file = config['output_directory']+'/reports/anatomy_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/anatomy/ids',input.input_compendia,output.report_file)

rule check_anatomical_entity:
    input:
        infile=config['output_directory']+'/compendia/AnatomicalEntity.txt'
    output:
        outfile=config['output_directory']+'/reports/AnatomicalEntity.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_gross_anatomical_structure:
    input:
        infile=config['output_directory']+'/compendia/GrossAnatomicalStructure.txt'
    output:
        outfile=config['output_directory']+'/reports/GrossAnatomicalStructure.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_cell:
    input:
        infile=config['output_directory']+'/compendia/Cell.txt'
    output:
        outfile=config['output_directory']+'/reports/Cell.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule check_cellular_component:
    input:
        infile=config['output_directory']+'/compendia/CellularComponent.txt'
    output:
        outfile=config['output_directory']+'/reports/CellularComponent.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule anatomy:
    input:
        config['output_directory']+'/reports/anatomy_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['anatomy_outputs'])
    output:
        x=config['output_directory']+'/reports/anatomy_done'
    shell:
        "echo 'done' >> {output.x}"

### Gene / Protein

rule gene_ncbi_ids:
    input:
        infile=config['download_directory']+'/NCBIGene/labels'
    output:
        outfile=config['download_directory']+"/gene/ids/NCBIGene"
    shell:
        #This one is a simple enough transform to do with awk
        "awk '{{print $1}}' {input.infile} > {output.outfile}"

rule gene_omim_ids:
    input:
        infile=config['download_directory']+'/OMIM/mim2gene.txt'
    output:
        outfile=config['download_directory']+"/gene/ids/OMIM"
    run:
        geneprotein.write_omim_ids(input.infile,output.outfile)

rule gene_ensembl_ids:
    input:
        infile=config['download_directory']+'/ENSEMBL/BioMartDownloadComplete'
    output:
        outfile=config['download_directory']+"/gene/ids/ENSEMBL"
    run:
        geneprotein.write_ensembl_ids(config['download_directory']+'/ENSEMBL',output.outfile)

rule gene_hgnc_ids:
    input:
        infile=config['download_directory']+"/HGNC/hgnc_complete_set.json"
    output:
        outfile=config['download_directory']+"/gene/ids/HGNC"
    run:
        geneprotein.write_hgnc_ids(input.infile,output.outfile)

rule gene_umls_ids:
    output:
        outfile=config['download_directory']+"/gene/ids/UMLS"
    run:
        geneprotein.write_umls_ids(output.outfile)

rule get_gene_ncbigene_ensembl_relationships:
    input:
        infile=config['download_directory']+"/NCBIGene/gene2ensembl.gz"
    output:
        outfile=config['download_directory']+'/gene/concords/NCBIGeneENSEMBL'
    run:
        geneprotein.build_gene_ncbi_ensemble_relationships(input.infile,output.outfile)

rule get_gene_ncbigene_relationships:
    input:
        infile=config['download_directory']+"/NCBIGene/gene_info.gz"
    output:
        outfile=config['download_directory']+'/gene/concords/NCBIGene'
    run:
        geneprotein.build_gene_ncbigene_xrefs(input.infile,output.outfile)

rule get_gene_medgen_relationships:
    input:
        infile=config['download_directory']+'/NCBIGene/mim2gene_medgen'
    output:
        outfile=config['download_directory']+'/gene/concords/medgen'
    run:
        geneprotein.build_gene_medgen_relationships(input.infile, output.outfile)

rule get_gene_umls_relationships:
    input:
        infile=config['download_directory']+'/gene/ids/UMLS'
    output:
        outfile=config['download_directory']+'/gene/concords/UMLS'
    run:
        geneprotein.build_gene_umls_hgnc_relationships(input.infile, output.outfile)

rule gene_compendia:
    input:
        labels=expand("{dd}/{ap}/labels",dd=config['download_directory'],ap=config['gene_labels']),
        synonyms=expand("{dd}/{ap}/synonyms",dd=config['download_directory'],ap=config['gene_labels']),
        concords=expand("{dd}/gene/concords/{ap}",dd=config['download_directory'],ap=config['gene_concords']),
        idlists=expand("{dd}/gene/ids/{ap}",dd=config['download_directory'],ap=config['gene_ids']),
    output:
        expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['gene_outputs']),
        expand("{od}/synonyms/{ap}", od = config['output_directory'], ap = config['gene_outputs'])
    run:
        geneprotein.build_gene_compendia(input.concords,input.idlists)

rule check_gene_completeness:
    input:
        input_compendia = expand("{od}/compendia/{ap}", od = config['output_directory'], ap = config['gene_outputs'])
    output:
        report_file = config['output_directory']+'/reports/gene_completeness.txt'
    run:
        assessments.assess_completeness(config['download_directory']+'/gene/ids',input.input_compendia,output.report_file)

rule check_gene:
    input:
        infile=config['output_directory']+'/compendia/Gene.txt'
    output:
        outfile=config['output_directory']+'/reports/Gene.txt'
    run:
        assessments.assess(input.infile, output.outfile)

rule gene:
    input:
        config['output_directory']+'/reports/gene_completeness.txt',
        reports = expand("{od}/reports/{ap}",od=config['output_directory'], ap = config['gene_outputs'])
    output:
        x=config['output_directory']+'/reports/gene_done'
    shell:
        "echo 'done' >> {output.x}"


###

rule protein_pr_ids:
    output:
        outfile=config['download_directory']+"/protein/ids/PR"
    run:
        geneprotein.write_pr_ids(output.outfile)

#rule protein_uniprot_ids:
#    output:

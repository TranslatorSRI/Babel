import src.datahandlers.mesh as mesh
import src.datahandlers.obo as obo
import src.datahandlers.umls as umls
import src.datahandlers.ncbigene as ncbigene
import src.datahandlers.ensembl as ensembl
import src.datahandlers.hgnc as hgnc
import src.datahandlers.omim as omim
import src.datahandlers.uniprotkb as uniprotkb
import src.datahandlers.mods as mods

#####
#
# Data sets: pull data sets, and parse them to get labels and synonyms
#
####

### MODS

rule get_mods:
    output:
        expand("{download_directory}/{mod}/GENE-DESCRIPTION-JSON_{mod}.json", download_directory = config['download_directory'], mod = config['mods']),
    run:
        mods.pull_mods()

rule get_mods_labels:
    input:
        expand("{download_directory}/{mod}/GENE-DESCRIPTION-JSON_{mod}.json",download_directory=config['download_directory'], mod=config['mods']),
    output:
        expand("{download_directory}/{mod}/labels",download_directory=config['download_directory'], mod=config['mods']),
    run:
        mods.write_labels(config['download_directory'])

### UniProtKB

rule get_uniprotkb:
    output:
        config['download_directory']+'/UniProtKB/uniprot_sprot.fasta',
        config['download_directory']+'/UniProtKB/uniprot_trembl.fasta',
        config['download_directory']+'/UniProtKB/idmapping.dat'
    run:
        uniprotkb.pull_uniprotkb()

rule get_uniprotkb_labels:
    input:
        sprot_input=config['download_directory']+'/UniProtKB/uniprot_sprot.fasta',
        trembl_input=config['download_directory']+'/UniProtKB/uniprot_trembl.fasta',
    output:
        outfile=config['download_directory']+'/UniProtKB/labels'
    run:
        uniprotkb.pull_uniprot_labels(input.sprot_input,input.trembl_input,output.outfile)

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


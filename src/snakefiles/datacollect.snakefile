import src.node as node
import src.datahandlers.mesh as mesh
import src.datahandlers.clo as clo
import src.datahandlers.obo as obo
import src.datahandlers.umls as umls
import src.datahandlers.ncbigene as ncbigene
import src.datahandlers.efo as efo
import src.datahandlers.ensembl as ensembl
import src.datahandlers.hgnc as hgnc
import src.datahandlers.omim as omim
import src.datahandlers.uniprotkb as uniprotkb
import src.datahandlers.mods as mods
import src.datahandlers.ncit as ncit
import src.datahandlers.doid as doid
import src.datahandlers.gard as gard
import src.datahandlers.orphanet as orphanet
import src.datahandlers.reactome as reactome
import src.datahandlers.rhea as rhea
import src.datahandlers.ec as ec
import src.datahandlers.smpdb as smpdb
import src.datahandlers.pantherpathways as pantherpathways
import src.datahandlers.unichem as unichem
import src.datahandlers.chembl as chembl
import src.datahandlers.gtopdb as gtopdb
import src.datahandlers.kegg as kegg
import src.datahandlers.unii as unii
import src.datahandlers.hmdb as hmdb
import src.datahandlers.pubchem as pubchem
import src.datahandlers.drugcentral as drugcentral
import src.datahandlers.ncbitaxon as ncbitaxon
import src.datahandlers.chebi as chebi
import src.datahandlers.hgncfamily as hgncfamily
import src.datahandlers.pantherfamily as pantherfamily
import src.datahandlers.complexportal as complexportal
import src.datahandlers.drugbank as drugbank
from src.babel_utils import pull_via_wget


# No-op placeholder rules run locally and don't need a SLURM slot.
localrules:
    get_mesh_synonyms,


#####
#
# Data sets: pull data sets, and parse them to get labels and synonyms
#
####

### EFO


rule get_EFO:
    output:
        config["download_directory"] + "/EFO" + "/efo.owl",
    benchmark:
        config["output_directory"] + "/benchmarks/get_EFO.tsv"
    retries: 3  # EFO OWL download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        efo.pull_efo()


rule get_EFO_labels:
    input:
        owlfile=config["download_directory"] + "/EFO/efo.owl",
    output:
        labelfile=config["download_directory"] + "/EFO/labels",
        synonymfile=config["download_directory"] + "/EFO/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_EFO_labels.tsv"
    run:
        efo.make_labels(input.owlfile, output.labelfile, output.synonymfile)


### Complex Portal
# https://www.ebi.ac.uk/complexportal/


rule get_complexportal:
    output:
        manifest=config["download_directory"] + "/ComplexPortal/" + complexportal.COMPLEXPORTAL_MANIFEST,
    benchmark:
        config["output_directory"] + "/benchmarks/get_complexportal.tsv"
    retries: 3  # ComplexPortal downloads occasionally fail transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        complexportal.pull_complexportal(output.manifest)


rule get_complexportal_labels_and_synonyms:
    input:
        manifest=config["download_directory"] + "/ComplexPortal/" + complexportal.COMPLEXPORTAL_MANIFEST,
    output:
        lfile=config["download_directory"] + "/ComplexPortal/labels",
        sfile=config["download_directory"] + "/ComplexPortal/synonyms",
        taxafile=config["download_directory"] + "/ComplexPortal/taxa",
        descfile=config["download_directory"] + "/ComplexPortal/descriptions",
        metadata_yaml=config["download_directory"] + "/ComplexPortal/metadata.yaml",
        idsfile=config["intermediate_directory"] + "/macromolecular_complex/ids/ComplexPortal",
    benchmark:
        config["output_directory"] + "/benchmarks/get_complexportal_labels_and_synonyms.tsv"
    run:
        complexportal.make_labels_synonyms_and_taxa(
            input.manifest,
            os.path.dirname(input.manifest),
            output.lfile,
            output.sfile,
            output.taxafile,
            output.descfile,
            output.metadata_yaml,
            output.idsfile,
        )


### MODS


rule get_mods:
    output:
        expand(
            "{download_directory}/{mod}/GENE-DESCRIPTION-JSON_{mod}.json",
            download_directory=config["download_directory"],
            mod=config["mods"],
        ),
    benchmark:
        config["output_directory"] + "/benchmarks/get_mods.tsv"
    retries: 3  # MOD gene-description downloads occasionally fail transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        mods.pull_mods()


rule get_mods_labels:
    input:
        expand(
            "{download_directory}/{mod}/GENE-DESCRIPTION-JSON_{mod}.json",
            download_directory=config["download_directory"],
            mod=config["mods"],
        ),
    output:
        expand("{download_directory}/{mod}/labels", download_directory=config["download_directory"], mod=config["mods"]),
    benchmark:
        config["output_directory"] + "/benchmarks/get_mods_labels.tsv"
    run:
        mods.write_labels(config["download_directory"])


### UniProtKB


rule get_uniprotkb_idmapping:
    output:
        idmapping=config["download_directory"] + "/UniProtKB/idmapping.dat",
    benchmark:
        config["output_directory"] + "/benchmarks/get_uniprotkb_idmapping.tsv"
    retries: 3  # Large UniProtKB FTP download may be interrupted transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
        runtime="6h",
    run:
        pull_via_wget(
            "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/",
            "idmapping.dat.gz",
            decompress=True,
            subpath="UniProtKB",
        )


rule get_uniprotkb_sprot:
    output:
        uniprot_sprot=config["download_directory"] + "/UniProtKB/uniprot_sprot.fasta",
    benchmark:
        config["output_directory"] + "/benchmarks/get_uniprotkb_sprot.tsv"
    retries: 3  # UniProtKB FTP download may be interrupted transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        pull_via_wget(
            "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/",
            "uniprot_sprot.fasta.gz",
            decompress=True,
            subpath="UniProtKB",
        )


rule get_uniprotkb_trembl:
    output:
        uniprot_trembl=config["download_directory"] + "/UniProtKB/uniprot_trembl.fasta",
    benchmark:
        config["output_directory"] + "/benchmarks/get_uniprotkb_trembl.tsv"
    retries: 3  # Large UniProtKB TrEMBL FTP download may be interrupted transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
        runtime="6h",
    run:
        pull_via_wget(
            "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/",
            "uniprot_trembl.fasta.gz",
            decompress=True,
            subpath="UniProtKB",
        )


rule get_uniprotkb_labels:
    input:
        sprot_input=config["download_directory"] + "/UniProtKB/uniprot_sprot.fasta",
        trembl_input=config["download_directory"] + "/UniProtKB/uniprot_trembl.fasta",
    output:
        outfile=config["download_directory"] + "/UniProtKB/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/get_uniprotkb_labels.tsv"
    resources:
        # Peaks at ~40 GB on babel-1.17 (see docs/tools/Resources.md); over the 16 GB default.
        mem="48G",
    run:
        uniprotkb.pull_uniprot_labels(input.sprot_input, input.trembl_input, output.outfile)


### MESH


rule get_mesh:
    output:
        config["download_directory"] + "/MESH/mesh.nt",
    benchmark:
        config["output_directory"] + "/benchmarks/get_mesh.tsv"
    retries: 3  # MeSH FTP download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        mesh.pull_mesh()


rule get_mesh_labels:
    input:
        config["download_directory"] + "/MESH/mesh.nt",
    output:
        config["download_directory"] + "/MESH/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/get_mesh_labels.tsv"
    run:
        mesh.pull_mesh_labels()


rule get_mesh_synonyms:
    # We don't actually get any.  Maybe we could from the nt?
    output:
        ofn=config["download_directory"] + "/MESH/synonyms",
    shell:
        "touch {output.ofn}"


### UMLS / SNOMEDCT


rule download_umls:
    output:
        config["download_directory"] + "/UMLS/MRCONSO.RRF",
        config["download_directory"] + "/UMLS/MRSTY.RRF",
        config["download_directory"] + "/UMLS/MRREL.RRF",
        config["download_directory"] + "/UMLS/UMLS.metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/download_umls.tsv"
    retries: 3  # UMLS download from NLM API occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
        runtime="6h",
    run:
        umls.download_umls(config["umls_version"], config["umls"]["subset"], config["download_directory"] + "/UMLS")


rule get_umls_labels_and_synonyms:
    input:
        mrconso=config["download_directory"] + "/UMLS/MRCONSO.RRF",
    output:
        config["download_directory"] + "/UMLS/labels",
        config["download_directory"] + "/UMLS/synonyms",
        config["download_directory"] + "/SNOMEDCT/labels",
        config["download_directory"] + "/SNOMEDCT/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_umls_labels_and_synonyms.tsv"
    run:
        umls.pull_umls(input.mrconso)


### OBO Ontologies


rule get_obo_labels:
    output:
        obo_labels=config["download_directory"] + "/common/ubergraph/labels",
        # A bunch of files depend on UberGraph labels being created in prefix directories (e.g. babel_downloads/GO/labels),
        # but these are now only included in the common labels file (i.e. babel_downloads/common/ubergraph/labels).
        # However, since they are needed to make Snakemake work, we'll generate these here.
        generated_labels=expand(
            "{download_directory}/{prefix}/labels",
            download_directory=config["download_directory"],
            prefix=config["generate_dirs_for_labels_and_synonyms_prefixes"],
        ),
    benchmark:
        config["output_directory"] + "/benchmarks/get_obo_labels.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        obo.pull_uber_labels(output.obo_labels, output.generated_labels)


rule get_obo_synonyms:
    output:
        obo_synonyms=config["download_directory"] + "/common/ubergraph/synonyms.jsonl",
        # A bunch of files depend on UberGraph labels being created in prefix directories (e.g. babel_downloads/GO/labels),
        # but these are now only included in the common labels file (i.e. babel_downloads/common/ubergraph/labels).
        # However, since they are needed to make Snakemake work, we'll generate these here.
        generated_synonyms=expand(
            "{download_directory}/{prefix}/synonyms",
            download_directory=config["download_directory"],
            prefix=config["generate_dirs_for_labels_and_synonyms_prefixes"],
        ),
    benchmark:
        config["output_directory"] + "/benchmarks/get_obo_synonyms.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        obo.pull_uber_synonyms(output.obo_synonyms, output.generated_synonyms)


rule get_obo_descriptions:
    output:
        obo_descriptions=config["download_directory"] + "/common/ubergraph/descriptions.jsonl",
    benchmark:
        config["output_directory"] + "/benchmarks/get_obo_descriptions.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        obo.pull_uber_descriptions(output.obo_descriptions)


rule get_icrdf:
    input:
        # Ideally, we would correctly mark all the dependencies for Ubergraph labels, synonyms and descriptions
        # throughout the system, but that would require a bunch of rewriting. Luckily, we already have the icRDF file
        # marked as required for all compendia, so we just need to make sure that OBO/Ubergraph has been downloaded
        # before the icRDF file is downloaded.
        config["download_directory"] + "/common/ubergraph/labels",
        config["download_directory"] + "/common/ubergraph/synonyms.jsonl",
        config["download_directory"] + "/common/ubergraph/descriptions.jsonl",
    output:
        icrdf_filename=config["download_directory"] + "/icRDF.tsv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_icrdf.tsv"
    retries: 3  # Ubergraph sometimes fails mid-download and needs a retry.
    run:
        obo.pull_uber_icRDF(output.icrdf_filename)
        # Try to load the icRDF.tsv file (this will produce an error if the file can't be read).
        node.InformationContentFactory(output.icrdf_filename)


### NCBIGene


rule get_ncbigene:
    output:
        getfiles=expand(
            "{download_directory}/NCBIGene/{ncbi_files}",
            download_directory=config["download_directory"],
            ncbi_files=config["ncbi_files"],
        ),
    benchmark:
        config["output_directory"] + "/benchmarks/get_ncbigene.tsv"
    retries: 3  # NCBIGene FTP download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        ncbigene.pull_ncbigene(config["ncbi_files"])


rule get_ncbigene_labels_synonyms_and_taxa:
    input:
        gene_info_filename=config["download_directory"] + "/NCBIGene/gene_info.gz",
    output:
        labels_filename=config["download_directory"] + "/NCBIGene/labels",
        synonyms_filename=config["download_directory"] + "/NCBIGene/synonyms",
        taxa_filename=config["download_directory"] + "/NCBIGene/taxa",
        descriptions_filename=config["download_directory"] + "/NCBIGene/descriptions",
    benchmark:
        config["output_directory"] + "/benchmarks/get_ncbigene_labels_synonyms_and_taxa.tsv"
    run:
        ncbigene.pull_ncbigene_labels_synonyms_and_taxa(
            input.gene_info_filename,
            output.labels_filename,
            output.synonyms_filename,
            output.taxa_filename,
            output.descriptions_filename,
        )


### ENSEMBL


rule get_ensembl:
    output:
        # Declare only the sentinel file, not the directory. Snakemake deletes all declared
        # outputs on failure; keeping the directory out of outputs preserves already-downloaded
        # per-dataset TSV files so the job can resume from where it left off on retry.
        complete_file=config["download_directory"] + "/ENSEMBL/BioMartDownloadComplete",
    benchmark:
        config["output_directory"] + "/benchmarks/get_ensembl.tsv"
    retries: 3  # BioMart occasionally returns an HTML error page instead of TSV data.
    resources:
        mem="8G",
        cpus_per_task=1,
        runtime="6h",
    run:
        ensembl_dir = config["download_directory"] + "/ENSEMBL"
        ensembl.pull_ensembl(ensembl_dir, output.complete_file)


### HGNC


rule get_hgnc:
    output:
        outfile=config["download_directory"] + "/HGNC/hgnc_complete_set.json",
    benchmark:
        config["output_directory"] + "/benchmarks/get_hgnc.tsv"
    retries: 3  # HGNC download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        hgnc.pull_hgnc()


rule get_hgnc_labels_and_synonyms:
    input:
        infile=rules.get_hgnc.output.outfile,
    output:
        config["download_directory"] + "/HGNC/labels",
        config["download_directory"] + "/HGNC/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_hgnc_labels_and_synonyms.tsv"
    run:
        hgnc.pull_hgnc_labels_and_synonyms(input.infile)


### HGNC.FAMILY


rule get_hgncfamily:
    output:
        outfile=config["download_directory"] + "/HGNC.FAMILY/family.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_hgncfamily.tsv"
    retries: 3  # HGNC family download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        hgncfamily.pull_hgncfamily()


rule get_hgncfamily_labels:
    input:
        infile=config["download_directory"] + "/HGNC.FAMILY/family.csv",
    output:
        labelsfile=config["download_directory"] + "/HGNC.FAMILY/labels",
        descriptionsfile=config["download_directory"] + "/HGNC.FAMILY/descriptions",
        metadata_yaml=config["download_directory"] + "/HGNC.FAMILY/metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_hgncfamily_labels.tsv"
    run:
        hgncfamily.pull_labels(input.infile, output.labelsfile, output.descriptionsfile, output.metadata_yaml)


### PANTHER.FAMILY


rule get_pantherfamily:
    output:
        outfile=config["download_directory"] + "/PANTHER.FAMILY/family.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_pantherfamily.tsv"
    retries: 3  # FTP connections to pantherdb.org are occasionally refused or dropped.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        pantherfamily.pull_pantherfamily()


rule get_pantherfamily_labels:
    input:
        infile=config["download_directory"] + "/PANTHER.FAMILY/family.csv",
    output:
        outfile=config["download_directory"] + "/PANTHER.FAMILY/labels",
        metadata_yaml=config["download_directory"] + "/PANTHER.FAMILY/metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_pantherfamily_labels.tsv"
    run:
        pantherfamily.pull_labels(input.infile, output.outfile, output.metadata_yaml)


### OMIM


rule get_omim:
    output:
        outfile=config["download_directory"] + "/OMIM/mim2gene.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/get_omim.tsv"
    retries: 3  # OMIM download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        omim.pull_omim()


rule get_omim_labels:
    input:
        infile=rules.get_omim.output.outfile,
    output:
        labels=config["download_directory"] + "/OMIM/labels",
        synonyms=config["download_directory"] + "/OMIM/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_omim_labels.tsv"
    resources:
        mem="1G",
        cpus_per_task=1,
    run:
        omim.pull_omim_labels(input.infile, output.labels, output.synonyms)


### NCIT


rule get_ncit:
    output:
        outfile=config["download_directory"] + "/NCIT/NCIt-SwissProt_Mapping.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/get_ncit.tsv"
    retries: 3  # NCI Thesaurus download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        ncit.pull_ncit()


### DOID


rule get_doid:
    output:
        outfile=config["download_directory"] + "/DOID/doid.json",
    benchmark:
        config["output_directory"] + "/benchmarks/get_doid.tsv"
    retries: 3  # Disease Ontology download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        doid.pull_doid()


rule get_doid_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/DOID/doid.json",
    output:
        labelfile=config["download_directory"] + "/DOID/labels",
        synonymfile=config["download_directory"] + "/DOID/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_doid_labels_and_synonyms.tsv"
    run:
        doid.pull_doid_labels_and_synonyms(input.infile, output.labelfile, output.synonymfile)


### Orphanet


rule get_orphanet:
    output:
        outfile=config["download_directory"] + "/Orphanet/Orphanet_Nomenclature_Pack_EN.zip",
    benchmark:
        config["output_directory"] + "/benchmarks/get_orphanet.tsv"
    retries: 3  # Orphanet download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        orphanet.pull_orphanet()


rule get_orphanet_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/Orphanet/Orphanet_Nomenclature_Pack_EN.zip",
    output:
        labelfile=config["download_directory"] + "/Orphanet/labels",
        synonymfile=config["download_directory"] + "/Orphanet/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_orphanet_labels_and_synonyms.tsv"
    run:
        orphanet.pull_orphanet_labels_and_synonyms(input.infile, output.labelfile, output.synonymfile)


### GARD


rule get_gard:
    # NCATS Genetic and Rare Diseases registry term list. The distribution is a Salesforce
    # ContentVersion download link (query string, no stable filename), fetched directly rather
    # than via pull_via_urllib (whose url + in_file_name assembly does not fit a query string).
    output:
        outfile=config["download_directory"] + "/GARD/gard.csv",
        # Provenance for the download: which upload, from where, when, and what Babel keeps of it.
        # Written here because this rule is the only place that sees the HTTP response, and the
        # response carries the two strings that stand in for GARD's missing version number.
        metadata_yaml=config["download_directory"] + "/GARD/metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_gard.tsv"
    retries: 3  # Salesforce CDN occasionally fails transiently.
    params:
        # Declared as params (not read from config inside run:) so that repointing
        # gard_download_url actually retriggers the download instead of reusing a stale CSV.
        url=config["gard_download_url"],
    run:
        gard.pull_gard(params.url, output.outfile, output.metadata_yaml)


rule get_gard_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/GARD/gard.csv",
    output:
        labelfile=config["download_directory"] + "/GARD/labels",
        synonymfile=config["download_directory"] + "/GARD/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_gard_labels_and_synonyms.tsv"
    run:
        gard.pull_gard_labels_and_synonyms(input.infile, output.labelfile, output.synonymfile)


### Reactome


rule get_reactome:
    output:
        outfile=config["download_directory"] + "/REACT/Events.json",
    benchmark:
        config["output_directory"] + "/benchmarks/get_reactome.tsv"
    retries: 3  # Reactome download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        reactome.pull_reactome(output.outfile)


rule get_reactome_labels:
    input:
        infile=config["download_directory"] + "/REACT/Events.json",
    output:
        labelfile=config["download_directory"] + "/REACT/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/get_reactome_labels.tsv"
    run:
        reactome.make_labels(input.infile, output.labelfile)


### RHEA


rule get_rhea:
    output:
        outfile=config["download_directory"] + "/RHEA/rhea.rdf",
    benchmark:
        config["output_directory"] + "/benchmarks/get_rhea.tsv"
    retries: 3  # RHEA download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        rhea.pull_rhea()


rule get_rhea_labels:
    input:
        infile=config["download_directory"] + "/RHEA/rhea.rdf",
    output:
        labelfile=config["download_directory"] + "/RHEA/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/get_rhea_labels.tsv"
    run:
        rhea.make_labels(output.labelfile)


### EC


rule get_EC:
    output:
        outfile=config["download_directory"] + "/EC/enzyme.rdf",
    benchmark:
        config["output_directory"] + "/benchmarks/get_EC.tsv"
    retries: 3  # Enzyme Classification download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        ec.pull_ec()


rule get_EC_labels:
    input:
        infile=config["download_directory"] + "/EC/enzyme.rdf",
    output:
        labelfile=config["download_directory"] + "/EC/labels",
        synonymfile=config["download_directory"] + "/EC/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_EC_labels.tsv"
    run:
        ec.make_labels(input.infile, output.labelfile, output.synonymfile)


### SMPDB


rule get_SMPDB:
    output:
        outfile=config["download_directory"] + "/SMPDB/smpdb_pathways.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_SMPDB.tsv"
    retries: 3  # SMPDB download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        smpdb.pull_smpdb()


rule get_SMPDB_labels:
    input:
        infile=config["download_directory"] + "/SMPDB/smpdb_pathways.csv",
    output:
        labelfile=config["download_directory"] + "/SMPDB/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/get_SMPDB_labels.tsv"
    run:
        smpdb.make_labels(input.infile, output.labelfile)


### PantherPathways


rule get_panther_pathways:
    output:
        outfile=config["download_directory"] + "/PANTHER.PATHWAY/SequenceAssociationPathway3.6.8.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/get_panther_pathways.tsv"
    retries: 3  # PANTHER pathway download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        pantherpathways.pull_panther_pathways()


rule get_panther_pathway_labels:
    input:
        infile=config["download_directory"] + "/PANTHER.PATHWAY/SequenceAssociationPathway3.6.8.txt",
    output:
        labelfile=config["download_directory"] + "/PANTHER.PATHWAY/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/get_panther_pathway_labels.tsv"
    run:
        pantherpathways.make_pathway_labels(input.infile, output.labelfile)


### Unichem


rule download_unichem_structure:
    output:
        config["download_directory"] + "/UNICHEM/structure.tsv.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/download_unichem_structure.tsv"
    retries: 3
    resources:
        mem="8G",
        disk="50G",
        cpus_per_task=1,
        runtime=240,
    run:
        unichem.download_unichem_structure()


rule download_unichem_reference:
    output:
        config["download_directory"] + "/UNICHEM/reference.tsv.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/download_unichem_reference.tsv"
    retries: 3
    resources:
        mem="8G",
        disk="8G",
        cpus_per_task=1,
        runtime=60,
    run:
        unichem.download_unichem_reference()


rule filter_unichem:
    input:
        reffile=config["download_directory"] + "/UNICHEM/reference.tsv.gz",
    output:
        filteredreffile=config["download_directory"] + "/UNICHEM/reference.filtered.tsv",
    benchmark:
        config["output_directory"] + "/benchmarks/filter_unichem.tsv"
    run:
        unichem.filter_unichem(input.reffile, output.filteredreffile)


### CHEMBL


rule get_chembl:
    output:
        moleculefile=config["download_directory"] + "/CHEMBL.COMPOUND/chembl_latest_molecule.ttl",
        ccofile=config["download_directory"] + "/CHEMBL.COMPOUND/cco.ttl",
    benchmark:
        config["output_directory"] + "/benchmarks/get_chembl.tsv"
    retries: 3  # FTP to EBI is occasionally refused or dropped.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        chembl.pull_chembl(output.moleculefile)


rule chembl_labels_and_smiles:
    input:
        infile=config["download_directory"] + "/CHEMBL.COMPOUND/chembl_latest_molecule.ttl",
        ccofile=config["download_directory"] + "/CHEMBL.COMPOUND/cco.ttl",
    output:
        outfile=config["download_directory"] + "/CHEMBL.COMPOUND/labels",
        smifile=config["download_directory"] + "/CHEMBL.COMPOUND/smiles",
    benchmark:
        config["output_directory"] + "/benchmarks/chembl_labels_and_smiles.tsv"
    resources:
        # ChemblRDF bulk-loads the ~17 GB molecule TTL into an in-memory pyoxigraph
        # store, so this rule needs a large-memory host (a 32 GB machine swap-thrashes
        # and never finishes). The matching test_chembl pipeline tests are tagged
        # @pytest.mark.min_memory_gb(128) and auto-skip below this.
        mem="128G",
    run:
        chembl.pull_chembl_labels_and_smiles(input.infile, input.ccofile, output.outfile, output.smifile)


### DrugBank requires a login... but not for basic vocabulary information.
rule get_drugbank_vocabulary:
    output:
        outfile=config["download_directory"] + "/DRUGBANK/drugbank vocabulary.csv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_drugbank_vocabulary.tsv"
    retries: 3  # DrugBank download occasionally fails transiently.
    run:
        drugbank.download_drugbank_vocabulary(config["drugbank_version"], output.outfile)


# Split out from get_drugbank_vocabulary so that rule's `retries: 3` only re-runs the flaky
# download, not the label/synonym extraction.
rule get_drugbank_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/DRUGBANK/drugbank vocabulary.csv",
    output:
        labels=config["download_directory"] + "/DRUGBANK/labels",
        synonyms=config["download_directory"] + "/DRUGBANK/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_drugbank_labels_and_synonyms.tsv"
    run:
        drugbank.extract_drugbank_labels_and_synonyms(input.infile, output.labels, output.synonyms)


### GTOPDB We're only pulling ligands.  Maybe one day we'll want the whole db?


rule get_gtopdb:
    output:
        outfile=config["download_directory"] + "/GTOPDB/ligands.tsv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_gtopdb.tsv"
    retries: 3  # Guide to Pharmacology download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        gtopdb.pull_gtopdb_ligands()


rule gtopdb_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/GTOPDB/ligands.tsv",
    output:
        labelfile=config["download_directory"] + "/GTOPDB/labels",
        synfile=config["download_directory"] + "/GTOPDB/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/gtopdb_labels_and_synonyms.tsv"
    run:
        gtopdb.make_labels_and_synonyms(input.infile, output.labelfile, output.synfile)


# KEGG We're also only getting compounds now.  And we're going through the api b/c data files are not available
# so no data pull, just making labels


rule keggcompound_labels:
    output:
        labelfile=config["download_directory"] + "/KEGG.COMPOUND/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/keggcompound_labels.tsv"
    retries: 3  # KEGG REST API calls occasionally fail transiently.
    run:
        kegg.pull_kegg_compound_labels(output.labelfile)


# UNII


rule get_unii:
    output:
        config["download_directory"] + "/UNII/Latest_UNII_Names.txt",
        config["download_directory"] + "/UNII/Latest_UNII_Records.txt",
    benchmark:
        config["output_directory"] + "/benchmarks/get_unii.tsv"
    retries: 3  # UNII download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        unii.pull_unii()


rule unii_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/UNII/Latest_UNII_Names.txt",
    output:
        labelfile=config["download_directory"] + "/UNII/labels",
        synfile=config["download_directory"] + "/UNII/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/unii_labels_and_synonyms.tsv"
    run:
        unii.make_labels_and_synonyms(input.infile, output.labelfile, output.synfile)


# HMDB


rule get_HMDB:
    output:
        outfile=config["download_directory"] + "/HMDB/hmdb_metabolites.xml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_HMDB.tsv"
    retries: 3  # HMDB occasionally returns HTTP errors; transient network failures need a retry.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        hmdb.pull_hmdb()


rule hmdb_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/HMDB/hmdb_metabolites.xml",
    output:
        labelfile=config["download_directory"] + "/HMDB/labels",
        synfile=config["download_directory"] + "/HMDB/synonyms",
        smifile=config["download_directory"] + "/HMDB/smiles",
    benchmark:
        config["output_directory"] + "/benchmarks/hmdb_labels_and_synonyms.tsv"
    resources:
        # Peaks at ~30 GB on babel-1.17 (see docs/tools/Resources.md); over the 16 GB default.
        mem="48G",
    run:
        hmdb.make_labels_and_synonyms_and_smiles(input.infile, output.labelfile, output.synfile, output.smifile)


# PUBCHEM:


rule get_pubchem:
    output:
        config["download_directory"] + "/PUBCHEM.COMPOUND/CID-MeSH",
        config["download_directory"] + "/PUBCHEM.COMPOUND/CID-Synonym-filtered.gz",
        config["download_directory"] + "/PUBCHEM.COMPOUND/CID-Title.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/get_pubchem.tsv"
    retries: 3  # PubChem FTP download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        pubchem.pull_pubchem()


rule get_pubchem_structures:
    output:
        config["download_directory"] + "/PUBCHEM.COMPOUND/CID-InChI-Key.gz",
        config["download_directory"] + "/PUBCHEM.COMPOUND/CID-SMILES.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/get_pubchem_structures.tsv"
    retries: 3  # PubChem FTP download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        pubchem.pull_pubchem_structures()


rule pubchem_labels:
    input:
        infile=config["download_directory"] + "/PUBCHEM.COMPOUND/CID-Title.gz",
    output:
        outfile=config["download_directory"] + "/PUBCHEM.COMPOUND/labels",
    benchmark:
        config["output_directory"] + "/benchmarks/pubchem_labels.tsv"
    run:
        pubchem.make_labels_or_synonyms(input.infile, output.outfile)


rule pubchem_synonyms:
    input:
        infile=config["download_directory"] + "/PUBCHEM.COMPOUND/CID-Synonym-filtered.gz",
    output:
        outfile=config["download_directory"] + "/PUBCHEM.COMPOUND/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/pubchem_synonyms.tsv"
    run:
        pubchem.make_labels_or_synonyms(input.infile, output.outfile)


rule download_rxnorm:
    output:
        config["download_directory"] + "/RxNorm/RXNCONSO.RRF",
        config["download_directory"] + "/RxNorm/RXNREL.RRF",
    benchmark:
        config["output_directory"] + "/benchmarks/download_rxnorm.tsv"
    retries: 3  # RxNorm download from NLM API occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        umls.download_rxnorm(config["rxnorm_version"], config["download_directory"] + "/RxNorm")


rule pubchem_rxnorm_annotations:
    output:
        outfile=config["download_directory"] + "/PUBCHEM.COMPOUND/RXNORM.json",
    benchmark:
        config["output_directory"] + "/benchmarks/pubchem_rxnorm_annotations.tsv"
    retries: 3  # PubChem RxNorm annotation API occasionally fails transiently.
    run:
        pubchem.pull_rxnorm_annotations(output.outfile)


# DRUGCENTRAL


rule get_drugcentral:
    output:
        structfile=config["download_directory"] + "/DrugCentral/structures",
        labelfile=config["download_directory"] + "/DrugCentral/labels",
        xreffile=config["download_directory"] + "/DrugCentral/xrefs",
    benchmark:
        config["output_directory"] + "/benchmarks/get_drugcentral.tsv"
    retries: 3  # DrugCentral download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        drugcentral.pull_drugcentral(output.structfile, output.labelfile, output.xreffile)


# NCBITaxon


rule get_ncbitaxon:
    output:
        config["download_directory"] + "/NCBITaxon/taxdump.tar",
    benchmark:
        config["output_directory"] + "/benchmarks/get_ncbitaxon.tsv"
    retries: 3  # NCBITaxon FTP download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        ncbitaxon.pull_ncbitaxon()


rule ncbitaxon_labels_and_synonyms:
    input:
        infile=config["download_directory"] + "/NCBITaxon/taxdump.tar",
    output:
        lfile=config["download_directory"] + "/NCBITaxon/labels",
        sfile=config["download_directory"] + "/NCBITaxon/synonyms",
        propfilegz=config["download_directory"] + "/NCBITaxon/properties.tsv.gz",
    benchmark:
        config["output_directory"] + "/benchmarks/ncbitaxon_labels_and_synonyms.tsv"
    run:
        ncbitaxon.make_labels_and_synonyms(input.infile, output.lfile, output.sfile, output.propfilegz)


# CHEBI: some comes via obo, but we need the SDF file too


rule get_chebi:
    output:
        config["download_directory"] + "/CHEBI/ChEBI_complete.sdf",
        config["download_directory"] + "/CHEBI/database_accession.tsv",
        config["download_directory"] + "/CHEBI/source.tsv",
        config["download_directory"] + "/CHEBI/status.tsv",
    benchmark:
        config["output_directory"] + "/benchmarks/get_chebi.tsv"
    retries: 3  # ChEBI FTP download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        chebi.pull_chebi()


# CLO: Cell Line Ontology


rule get_clo:
    output:
        config["download_directory"] + "/CLO/clo.owl",
        metadata=config["download_directory"] + "/CLO/metadata.yaml",
    benchmark:
        config["output_directory"] + "/benchmarks/get_clo.tsv"
    retries: 3  # Cell Line Ontology download occasionally fails transiently.
    resources:
        mem="8G",
        cpus_per_task=1,
    run:
        clo.pull_clo(output.metadata)


rule get_CLO_labels:
    input:
        infile=config["download_directory"] + "/CLO/clo.owl",
    output:
        labelfile=config["download_directory"] + "/CLO/labels",
        synonymfile=config["download_directory"] + "/CLO/synonyms",
    benchmark:
        config["output_directory"] + "/benchmarks/get_CLO_labels.tsv"
    run:
        clo.make_labels(input.infile, output.labelfile, output.synonymfile)

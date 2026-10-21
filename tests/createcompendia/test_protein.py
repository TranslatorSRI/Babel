"""Unit tests for src/createcompendia/protein.py: its Ensembl protein ids and its concord builders.

The BioMart fixtures are rows copied verbatim from the Ensembl BioMart downloads that
``src.datahandlers.ensembl.pull_ensembl`` writes (Ensembl release 115, September 2026): the yeast rows
from ``scerevisiae_gene_ensembl/BioMart.tsv`` and the human row from a ``hsapiens_gene_ensembl`` query
filtered to ``ENSG00000141510`` (TP53) with the same attribute set. Column order differs per dataset
because ``pull_ensembl`` requests the attributes each dataset offers, so each fixture keeps its own
header.
"""

import pytest

from src.createcompendia import gene, protein
from src.createcompendia.protein import build_ncit_uniprot_relationships

# Header + rows from scerevisiae_gene_ensembl/BioMart.tsv. Ensembl's yeast annotation reuses the SGD
# systematic name as gene, transcript and translation ID, so "Protein stable ID" == "Gene stable ID"
# on every protein-coding row (11,117 of 11,667 rows in the release-115 download).
YEAST_BIOMART = (
    "Gene stable ID\tSGD gene name ID\tGene Synonym\tGene description\tProtein stable ID\t"
    "NCBI gene (formerly Entrezgene) ID\tChromosome/scaffold name\tSource (gene)\tGene name\t"
    "Source of gene name\tGene type\n"
    # ETS1-1: a non-coding row with a blank Protein stable ID.
    "ETS1-1\tS000029717\t\tNon-coding region located immediately upstream of RDN18; transcribed as part of the "
    "35S rRNA precursor transcript; contains an essential U3 snoRNA binding site required for maturation of "
    "18S rRNA [Source:SGD;Acc:S000029717]\t\t\tXII\tsgd\t\t\trRNA\n"
    # YOR125C (CAT5/COQ7): the example in https://github.com/NCATSTranslator/Babel/issues/276.
    "YOR125C\tS000005651\tCOQ7\tProtein required for ubiquinone (Coenzyme Q) biosynthesis; localizes to the matrix "
    "face of the mitochondrial inner membrane in a large complex with ubiquinone biosynthetic enzymes; required "
    "for gluconeogenic gene activation [Source:SGD;Acc:S000005651]\tYOR125C\t854292.0\tXV\tsgd\tCAT5\t"
    "SGD gene name\tprotein_coding\n"
    # YAL068C (PAU8).
    "YAL068C\tS000002142\t\tProtein of unknown function; member of the seripauperin multigene family encoded "
    "mainly in subtelomeric regions [Source:SGD;Acc:S000002142]\tYAL068C\t852163.0\tI\tsgd\tPAU8\t"
    "SGD gene name\tprotein_coding\n"
)

# Header + rows for ENSG00000141510 (TP53) from hsapiens_gene_ensembl. One gene synonym per row, so the
# same protein stable ID repeats across rows.
HUMAN_BIOMART = (
    "Gene stable ID\tChromosome/scaffold name\tGene description\tProtein stable ID\t"
    "NCBI gene (formerly Entrezgene) ID\tGene name\tSource of gene name\tGene Synonym\tGene type\tSource (gene)\n"
    "ENSG00000141510\t17\ttumor protein p53 [Source:HGNC Symbol;Acc:HGNC:11998]\tENSP00000410739\t7157\tTP53\t"
    "HGNC Symbol\tLFS1\tprotein_coding\tensembl_havana\n"
    "ENSG00000141510\t17\ttumor protein p53 [Source:HGNC Symbol;Acc:HGNC:11998]\tENSP00000410739\t7157\tTP53\t"
    "HGNC Symbol\tP53\tprotein_coding\tensembl_havana\n"
)


@pytest.fixture
def ensembl_dir(tmp_path):
    """An ENSEMBL download directory with a yeast dataset, a human dataset and an empty dataset directory."""
    root = tmp_path / "ENSEMBL"
    (root / "scerevisiae_gene_ensembl").mkdir(parents=True)
    (root / "scerevisiae_gene_ensembl" / "BioMart.tsv").write_text(YEAST_BIOMART)
    (root / "hsapiens_gene_ensembl").mkdir()
    (root / "hsapiens_gene_ensembl" / "BioMart.tsv").write_text(HUMAN_BIOMART)
    # A dataset whose download never completed has a directory but no BioMart.tsv.
    (root / "choffmanni_gene_ensembl").mkdir()
    return root


def _ids(path):
    return [line.rstrip("\n") for line in open(path) if line.strip()]


# ENSEMBL PROTEIN IDS


@pytest.mark.unit
def test_write_ensembl_protein_ids_skips_ids_that_are_gene_ids(ensembl_dir, tmp_path):
    """Yeast protein stable IDs equal their gene stable ID and should not be written as protein ids;
    the human ENSP id should be written once despite appearing on two synonym rows."""
    outfile = tmp_path / "ids" / "ENSEMBL"
    outfile.parent.mkdir()
    protein.write_ensembl_protein_ids(str(ensembl_dir), str(outfile))
    assert _ids(outfile) == ["ENSEMBL:ENSP00000410739"]


@pytest.mark.unit
def test_write_ensembl_gene_ids_keeps_yeast_ids(ensembl_dir, tmp_path):
    """The gene pipeline should still claim the yeast systematic names, so the ID lives in exactly one
    compendium (the gene claim wins; see write_ensembl_protein_ids)."""
    outfile = tmp_path / "ids" / "ENSEMBL"
    outfile.parent.mkdir()
    gene.write_ensembl_gene_ids(str(ensembl_dir), str(outfile))
    # Datasets are visited in os.listdir() order, so compare as a set.
    assert set(_ids(outfile)) == {"ENSEMBL:ENSG00000141510", "ENSEMBL:ETS1-1", "ENSEMBL:YOR125C", "ENSEMBL:YAL068C"}


# NCIT-SWISSPROT MAPPING

# Rows copied verbatim (CRLF line endings included) from NCIt-SwissProt_Mapping.txt, downloaded
# 2026-09-25 from https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Mappings/. The row's NCIt code is its ID.
NCIT_SWISSPROT_ROWS = (
    "NCIt Code\tSwissProt ID\tNCIt Preferred Name\r\n"
    "C150023\tQ9C0C9\t(E3-Independent) E2 Ubiquitin-Conjugating Enzyme\r\n"
    "C16375\tP0DP24|P0DP23|P0DP25\tCalmodulin\r\n"
    "C184960\tP0DTC1|P0DTD1\tSARS-CoV-2 3C-Like Proteinase Nsp5\r\n"
    "C71447\tU65002\tZinc Finger Protein PLAG1\r\n"
)


@pytest.mark.unit
def test_ncit_uniprot_keeps_only_single_uniprot_accessions(tmp_path):
    """Should map only rows with one UniProt accession, skipping the header, "|"-joined accession
    lists (C16375, C184960) and GenBank accessions (C71447)."""
    infile = tmp_path / "NCIt-SwissProt_Mapping.txt"
    infile.write_bytes(NCIT_SWISSPROT_ROWS.encode())
    outfile = tmp_path / "NCIT_UniProtKB"
    build_ncit_uniprot_relationships(infile, outfile, tmp_path / "metadata.yaml")
    assert outfile.read_text() == "NCIT:C150023\teq\tUniProtKB:Q9C0C9\n"


@pytest.mark.unit
def test_ncit_uniprot_rejects_unexpected_header(tmp_path):
    """Should raise rather than silently treat the first row as a header if NCIt drops the header."""
    infile = tmp_path / "NCIt-SwissProt_Mapping.txt"
    infile.write_bytes(NCIT_SWISSPROT_ROWS.split("\r\n", 1)[1].encode())
    with pytest.raises(ValueError, match="Unexpected header"):
        build_ncit_uniprot_relationships(infile, tmp_path / "out", tmp_path / "metadata.yaml")

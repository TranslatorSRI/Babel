import pytest
from src.createcompendia.geneprotein import build_compendium
import os

def test_gp():
    here=os.path.abspath(os.path.dirname(__file__))
    gene_compendium = os.path.join(here,'testdata','gptest_Gene.txt')
    protein_compendium = os.path.join(here,'testdata','gptest_Protein.txt')
    geneprotein_concord = os.path.join(here,'testdata','gp_UniProtNCBI.txt')
    outfile = os.path.join(here,'testdata','gp_output.txt')
    build_compendium(gene_compendium, protein_compendium, geneprotein_concord, outfile)
    with open(outfile,'r') as inf:
        x = inf.read()
    assert len(x) > 0
    print(x)

import pytest
from src.createcompendia.geneprotein import build_compendium
import os

def test_gp():
    gene_compendium = os.path.join('tests','testdata','gptest_Gene.txt')
    protein_compendium = os.path.join('tests','testdata','gptest_Protein.txt')
    geneprotein_concord = os.path.join('tests','testdata','gp_UniProtNCBI.txt')
    outfile = os.path.join('tests','testdata','gp_output.txt')
    print('HI')
    build_compendium(gene_compendium, protein_compendium, geneprotein_concord, outfile)
    with open(outfile,'r') as inf:
        x = inf.read()
    assert len(x) > 0
    print(x)

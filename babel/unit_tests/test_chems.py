import pytest
from babel.chemicals import label_pubchem

def test_label_pubchem():
    label_pubchem([],refresh_pubchem=True)

from src.LabeledID import LabeledID

def test_LID():
    x = 'identifier'
    lid = LabeledID(identifier=x, label="label")
    assert not x == lid
    s = set([lid])
    assert x not in s

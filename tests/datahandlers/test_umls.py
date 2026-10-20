"""Unit tests for src/datahandlers/umls.py: how UMLS GO atoms become UMLS-GO concords and UMLS synonyms.

Every MRCONSO row below is copied verbatim from the 2026AA MRCONSO.RRF; each group names its CUI or GO term.
"""

import pytest

from src.datahandlers.umls import DEFAULT_ACCEPTABLE_TTYS, GO_PREFERRED_TTYS, build_sets, pull_umls
from tests.conftest import assert_concordance_file_valid, assert_labels_file_valid, assert_synonyms_file_valid

# GO:0045943: the GO preferred term is in C1158785, but UMLS files two GO entry terms in CUIs of their own.
GO_0045943_ROWS = [
    "C1158785|ENG|P|L14795212|PF|S17987359|Y|A29357458|||GO:0045943|GO|PT|GO:0045943|positive regulation of transcription by RNA polymerase I|0|N||",
    "C1158785|ENG|S|L7626341|PF|S8824971|Y|A14275353|||GO:0045943|GO|SY|GO:0045943|up regulation of transcription from RNA polymerase I promoter|0|N||",
    "C2249862|ENG|P|L7623555|PF|S8799511|Y|A14238390|||GO:0045943|GO|ET|GO:0045943|activation of transcription from RNA polymerase I promoter|0|N||",
    "C2249863|ENG|P|L7626064|PF|S8822535|Y|A14254123|||GO:0045943|GO|ET|GO:0045943|stimulation of transcription from RNA polymerase I promoter|0|N||",
]

# C0039066 "Synaptonemal Complex": a MeSH heading, the GO preferred term, and NLM's MTH_PT for the same GO term.
C0039066_ROWS = [
    "C0039066|ENG|P|L0039066|PF|S0090147|Y|A0121905||M0020953|D013573|MSH|MH|D013573|Synaptonemal Complex|0|N||",
    "C0039066|ENG|P|L0039066|VC|S0529288|Y|A11625145|||GO:0000795|GO|PT|GO:0000795|synaptonemal complex|0|N||",
    "C0039066|ENG|S|L2486743|PF|S2937409|Y|A11600478|||GO:0000795|GO|MTH_PT|GO:0000795|synaptonemal complex location|0|N||",
]

# C0001811: an obsolete GO term, suppressed (SUPPRESS=O) in MRCONSO.
C0001811_ROWS = [
    "C0001811|ENG|S|L18742642|PF|S22463947|Y|A35572697|||GO:0007568|GO|OP|GO:0007568|obsolete aging|0|O||",
]

# C1152464 "cardiolipin synthase activity" (GO:0008808): UMLS also files entry terms of GO:0043337 and GO:0090483
# under it.
C1152464_ROWS = [
    "C1152464|ENG|P|L2429595|PF|S2865956|N|A11601962|||GO:0008808|GO|PT|GO:0008808|cardiolipin synthase activity|0|N||",
    "C1152464|ENG|P|L2429595|PF|S2865956|Y|A2499852||||MTH|PN|NOCODE|cardiolipin synthase activity|0|N||",
    "C1152464|ENG|S|L0054788|PF|S0141413|Y|A14253754|||GO:0043337|GO|ET|GO:0043337|cardiolipin synthetase|0|N||",
    "C1152464|ENG|S|L0289386|PF|S0416199|N|A21210960|||GO:0090483|GO|ET|GO:0090483|cardiolipin synthase|0|N||",
    "C1152464|ENG|S|L0289386|PF|S0416199|Y|A14243241|||GO:0043337|GO|ET|GO:0043337|cardiolipin synthase|0|N||",
    "C1152464|ENG|S|L2430485|PF|S2866460|Y|A14251799|||GO:0008808|GO|SY|GO:0008808|diphosphatidylglycerol synthase activity|0|N||",
    "C1152464|ENG|S|L5820893|PF|S6672146|Y|A14251798|||GO:0008808|GO|ET|GO:0008808|cardiolipin synthetase activity|0|N||",
]


def write_mrconso(tmp_path, rows):
    mrconso = tmp_path / "MRCONSO.RRF"
    mrconso.write_text("".join(row + "\n" for row in rows))
    return str(mrconso)


# BUILD_SETS


@pytest.mark.unit
def test_build_sets_go_term_type_restriction(tmp_path):
    """With GO restricted to GO_PREFERRED_TTYS, only the CUI holding a GO term's PT or MTH_PT should map to it; by
    default, every non-suppressed GO atom should map, and suppressed atoms should never map."""
    mrconso = write_mrconso(tmp_path, GO_0045943_ROWS + C0039066_ROWS + C0001811_ROWS)
    idfile = tmp_path / "ids"
    cuis = ["C1158785", "C2249862", "C2249863", "C0039066", "C0001811"]
    idfile.write_text("".join(f"UMLS:{cui}\tbiolink:BiologicalProcess\n" for cui in cuis))
    outfile = str(tmp_path / "concord")
    other_prefixes = {"GO": "GO", "MSH": "MESH"}

    build_sets(
        mrconso,
        str(idfile),
        outfile,
        other_prefixes,
        acceptable_ttys={**DEFAULT_ACCEPTABLE_TTYS, "GO": GO_PREFERRED_TTYS},
    )
    assert {tuple(row) for row in assert_concordance_file_valid(outfile)} == {
        ("UMLS:C1158785", "eq", "GO:0045943"),
        ("UMLS:C0039066", "eq", "GO:0000795"),
        ("UMLS:C0039066", "eq", "MESH:D013573"),
    }

    # The default (as used for anatomy) takes every GO atom. C0001811's OP atom is only dropped here by the
    # SUPPRESS check, since OP is not a restricted term type.
    build_sets(mrconso, str(idfile), outfile, other_prefixes)
    assert {tuple(row) for row in assert_concordance_file_valid(outfile)} == {
        ("UMLS:C1158785", "eq", "GO:0045943"),
        ("UMLS:C2249862", "eq", "GO:0045943"),
        ("UMLS:C2249863", "eq", "GO:0045943"),
        ("UMLS:C0039066", "eq", "GO:0000795"),
        ("UMLS:C0039066", "eq", "MESH:D013573"),
    }


# PULL_UMLS


@pytest.mark.unit
def test_pull_umls_skips_go_atoms_of_other_go_terms(tmp_path):
    """A CUI's GO atoms should become synonyms only when they carry one of the CUI's own GO preferred-term codes;
    a CUI with no GO preferred term (C2249862) should keep all of its GO atoms."""
    mrconso = write_mrconso(tmp_path, C1152464_ROWS + GO_0045943_ROWS[2:3])

    labels_file, synonyms_file = str(tmp_path / "UMLS_labels"), str(tmp_path / "UMLS_synonyms")
    pull_umls(
        mrconso, labels_file, synonyms_file, str(tmp_path / "SNOMEDCT_labels"), str(tmp_path / "SNOMEDCT_synonyms")
    )

    labels = {tuple(row) for row in assert_labels_file_valid(labels_file)}
    assert labels == {
        ("UMLS:C1152464", "cardiolipin synthase activity"),
        ("UMLS:C2249862", "activation of transcription from RNA polymerase I promoter"),
    }
    synonyms = {(curie, synonym) for curie, _, synonym in assert_synonyms_file_valid(synonyms_file)}
    assert synonyms == {
        ("UMLS:C1152464", "cardiolipin synthase activity"),
        ("UMLS:C1152464", "diphosphatidylglycerol synthase activity"),
        ("UMLS:C1152464", "cardiolipin synthetase activity"),
        ("UMLS:C2249862", "activation of transcription from RNA polymerase I promoter"),
    }

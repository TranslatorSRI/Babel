import pytest
from src.babel_utils import pull_via_ftp
import gzip

#FTP doesn't play nicely with travis-ci, so these are marked so they can be excluded.
#See: https://blog.travis-ci.com/2018-07-23-the-tale-of-ftp-at-travis-ci
#for FTP/Travis conundrum.:w

@pytest.mark.ftp
def test_pull_text_to_memory():
    """Pull a text file into memory so it will be usable"""
    data=pull_via_ftp('ftp.ncbi.nlm.nih.gov','gene/DATA/','stopwords_gene')
    lines = data.split('\n')
    assert len(lines) > 100
    assert lines[0] == 'a'

@pytest.mark.ftp
def test_pull_text_to_file():
    """Pull a text file into local file"""
    ofname = 'test_text'
    outname=pull_via_ftp('ftp.ncbi.nlm.nih.gov','gene/DATA/','stopwords_gene',outfilename = ofname)
    with open(outname,'r') as inf:
        lines = inf.read().split('\n')
    assert len(lines) > 100
    assert lines[0] == 'a'

@pytest.mark.ftp
def test_pull_gzip_to_memory():
    """Pull a gzipped file into memory, decompressed"""
    data=pull_via_ftp('ftp.ncbi.nlm.nih.gov','gene/DATA/','gene_group.gz',decompress_data=True)
    lines = data.split('\n')
    assert len(lines) > 1000
    assert lines[0].startswith('#tax_id')

@pytest.mark.ftp
def test_pull_gzip_to_uncompressed_file():
    """Pull a gzipped file into memory, decompressed"""
    ofname = 'test_gz_text'
    outname=pull_via_ftp('ftp.ncbi.nlm.nih.gov','gene/DATA/','gene_group.gz',decompress_data=True,outfilename=ofname)
    with open(outname,'r') as inf:
        lines = inf.read().split('\n')
    assert len(lines) > 1000
    assert lines[0].startswith('#tax_id')

@pytest.mark.ftp
def test_pull_gzip_to_compressed_file():
    """Pull a gzipped file into memory, decompressed"""
    ofname = 'test_gz.gz'
    outname=pull_via_ftp('ftp.ncbi.nlm.nih.gov','gene/DATA/','gene_group.gz',outfilename=ofname)
    with gzip.open(outname,'rt') as inf:
        lines = inf.read().split('\n')
    assert len(lines) > 1000
    assert lines[0].startswith('#tax_id')



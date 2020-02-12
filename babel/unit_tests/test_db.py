import pytest
import os,ast
from babel.babel_utils import StateDB,make_local_name

def test_blank_db():
    dbfile = 'test.db'
    dbfilename = make_local_name(dbfile)
    print(dbfilename)
    if os.path.exists(dbfilename):
        os.remove(dbfilename)
    db = StateDB(dbfile)
    assert db.get('x') is None

def test_round_trip():
    dbfile = 'test.db'
    dbfilename = make_local_name(dbfile)
    print(dbfilename)
    if os.path.exists(dbfilename):
        os.remove(dbfilename)
    db = StateDB(dbfile)
    value = "['junk']"
    db.put('y',value)
    assert db.get('y') ==  value

def test_round_multichar():
    dbfile = 'test.db'
    dbfilename = make_local_name(dbfile)
    print(dbfilename)
    if os.path.exists(dbfilename):
        os.remove(dbfilename)
    db = StateDB(dbfile)
    value = "['junk']"
    db.put('C000009',value)
    assert db.get('C000009') ==  value

def test_persistence():
    dbfile = 'test.db'
    dbfilename = make_local_name(dbfile)
    print(dbfilename)
    if os.path.exists(dbfilename):
        os.remove(dbfilename)
    db = StateDB(dbfile)
    value = "['junk']"
    db.put('y',value)
    db2 = StateDB(dbfile)
    assert db2.get('y') ==  value

def test_array():
    dbfile = 'test.db'
    dbfilename = make_local_name(dbfile)
    print(dbfilename)
    if os.path.exists(dbfilename):
        os.remove(dbfilename)
    db = StateDB(dbfile)
    value = ['junk']
    db.put('y',str(value))
    rtv = ast.literal_eval(db.get('y'))
    assert len(rtv) == 1
    assert rtv[0] == value[0]

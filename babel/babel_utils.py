from ftplib import FTP
from io import BytesIO
from gzip import decompress
import os

def pull_via_ftp(ftpsite, ftpdir, ftpfile, decompress_data=False, outfilename=None):
    """Retrieve data via ftp.
    Setting decompress=True will ungzip the data
    If outfilename is None (default) then the data will be returned.
    Otherwise it will be written to the downloads directory."""
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    with BytesIO() as data:
        ftp.retrbinary(f'RETR {ftpfile}', data.write)
        binary = data.getvalue()
    ftp.quit()
    if decompress_data:
        value = decompress(binary).decode()
    else:
        value = binary.decode()
    if outfilename is None:
        return value
    ofilename = os.path.join(os.path.dirname(os.path.abspath(__file__)),'downloads',outfilename)
    with open(ofilename,'w') as ofile:
        ofile.write(value)

def dump_dict(outdict,outfname):
    oname = os.path.join(os.path.dirname(__file__),'downloads', outfname)
    with open(oname,'w') as outf:
        for k,v in outdict.items():
            outf.write(f'{k}\t{v}\n')


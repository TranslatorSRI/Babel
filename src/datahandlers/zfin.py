from src.babel_utils import pull_via_urllib

def pull_zfin():
    pull_via_urllib('https://zfin.org/downloads/','identifiersForIntermine.txt',subpath='ZFIN')


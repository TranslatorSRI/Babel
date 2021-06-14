from src.babel_utils import pull_via_urllib, make_local_name

def pull_one_uniprotkb(which):
    pull_via_urllib('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/',f'uniprot_{which}.fasta.gz',subpath='UniProtKB')

def readlabels(which):
    swissname = make_local_name(f'UniProtKB/uniprot_{which}.fasta')
    swissprot_labels = {}
    with open(swissname,'r') as inf:
        for line in inf:
            if line.startswith('>'):
                #example fasta line:
                #>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1
                x = line.split('|')
                uniprotid = f'UniProtKB:{x[1]}'
                name = x[2].split(' OS=')[0]
                swissprot_labels[uniprotid] = f'{name} ({which})'
    return swissprot_labels

def pull_uniprotkb():
    pull_via_urllib('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/',f'idmapping.dat.gz',subpath='UniProtKB')
    for which in ['sprot','trembl']:
        pull_via_urllib('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/',f'uniprot_{which}.fasta.gz',subpath='UniProtKB')

def pull_uniprot_labels(sprotfile,tremblfile,fname):
    slabels = readlabels('sprot')
    tlabels = readlabels('trembl')
    with open(fname,'w') as labelfile:
        for k,v in slabels.items():
            labelfile.write(f'{k}\t{v}\n')
        for k,v in tlabels.items():
            labelfile.write(f'{k}\t{v}\n')

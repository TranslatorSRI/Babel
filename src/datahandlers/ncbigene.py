from src.babel_utils import pull_via_ftp,make_local_name
import gzip

def pull_ncbigene(filenames):
    remotedir='https://ftp.ncbi.nih.gov/gene/DATA/'
    for fn in filenames:
        pull_via_ftp('ftp.ncbi.nih.gov', '/gene/DATA', fn, decompress_data=False, outfilename=f'NCBIGene/{fn}')

def pull_ncbigene_labels_and_synonyms():
    #File format described here: https://ftp.ncbi.nih.gov/gene/DATA/README
    ifname = make_local_name('gene_info.gz', subpath='NCBIGene')
    labelname = make_local_name('labels', subpath='NCBIGene')
    synname = make_local_name('synonyms', subpath='NCBIGene')
    bad_gene_types = set(['biological-region','other','unknown'])
    with gzip.open(ifname,'r') as inf, open(labelname,'w') as labelfile, open(synname,'w') as synfile :
        h = inf.readline()
        for line in inf:
            sline = line.decode('utf-8')
            x = sline.strip().split('\t')
            gene_id = f'NCBIGene:{x[1]}'
            symbol = x[2]
            gene_type = x[9]
            if gene_type in bad_gene_types:
                continue
            labelfile.write(f'{gene_id}\t{symbol}\n')
            syns = set(x[4].split('|'))
            syns.add(symbol)
            description = x[8]
            syns.add(description)
            authoritative_symbol=x[10]
            syns.add(authoritative_symbol)
            authoritative_full_name = x[11]
            syns.add(authoritative_full_name)
            others = set(x[13].split('|'))
            syns.update(others)
            for syn in syns:
                synfile.write(f'{gene_id}\t{syn}\n')



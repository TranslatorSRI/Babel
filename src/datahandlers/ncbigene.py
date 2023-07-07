from src.babel_utils import pull_via_ftp, make_local_name
import gzip


def pull_ncbigene(filenames):
    remotedir = 'https://ftp.ncbi.nih.gov/gene/DATA/'
    for fn in filenames:
        pull_via_ftp('ftp.ncbi.nih.gov', '/gene/DATA', fn, decompress_data=False, outfilename=f'NCBIGene/{fn}')


def pull_ncbigene_labels_and_synonyms():
    # File format described here: https://ftp.ncbi.nih.gov/gene/DATA/README
    ifname = make_local_name('gene_info.gz', subpath='NCBIGene')
    labelname = make_local_name('labels', subpath='NCBIGene')
    synname = make_local_name('synonyms', subpath='NCBIGene')
    bad_gene_types = set(['biological-region', 'other', 'unknown'])
    with gzip.open(ifname, 'r') as inf, open(labelname, 'w') as labelfile, open(synname, 'w') as synfile:

        # Make sure the gene_info.gz columns haven't changed from under us.
        header = inf.readline()
        assert header.split("\t") == [
            "#tax_id",
            "GeneID",
            "Symbol",
            "LocusTag",
            "Synonyms",
            "dbXrefs",
            "chromosome",
            "map_location",
            "description",
            "type_of_gene",
            "Symbol_from_nomenclature_authority",
            "Full_name_from_nomenclature_authority",
            "Nomenclature_status",
            "Other_designations",
            "Modification_date",
            "Feature_type"]

        for line in inf:
            sline = line.decode('utf-8')
            row = sline.strip().split('\t')
            gene_id = f'NCBIGene:{row[1]}'
            symbol = row[header.index("Symbol")]
            gene_type = row[header.index("type_of_gene")]
            if gene_type in bad_gene_types:
                continue
            labelfile.write(f'{gene_id}\t{symbol}\n')
            syns = set(row[header.index("Synonyms")].split('|'))
            syns.add(symbol)
            description = row[header.index("description")]
            syns.add(description)
            authoritative_symbol = row[header.index("Symbol_from_nomenclature_authority")]
            syns.add(authoritative_symbol)
            authoritative_full_name = row[header.index("Full_name_from_nomenclature_authority")]
            syns.add(authoritative_full_name)
            others = set(row[header.index("Other_designations")].split('|'))
            syns.update(others)
            for syn in syns:
                synfile.write(f'{gene_id}\thttp://www.geneontology.org/formats/oboInOwl#hasSynonym\t{syn}\n')

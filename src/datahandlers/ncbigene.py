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
        header = inf.readline().decode('utf-8').strip().split("\t")
        assert header == [
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

        def get_field(row, field_name):
            """
            A helper function for returning the value of a field in gene_info.gz by name.
            The value is `-` if no value is present; we need to convert that into an empty string.

            :param row: A row from gene_info.gz.
            :param field_name: A field name in the header of gene_info.gz.
            :return: The value in this column in this row, otherwise the empty string ('').
            """
            index = header.index(field_name)
            value = row[index].strip()
            if value == '-':
                return ''
            return value

        for line in inf:
            sline = line.decode('utf-8')
            row = sline.strip().split('\t')
            gene_id = f'NCBIGene:{get_field(row, "GeneID")}'
            symbol = get_field(row, "Symbol")
            gene_type = get_field(row, "type_of_gene")
            if gene_type in bad_gene_types:
                continue
            labelfile.write(f'{gene_id}\t{symbol}\n')
            syns = set(get_field(row, "Synonyms").split('|'))
            syns.add(symbol)
            description = get_field(row, "description")
            syns.add(description)
            authoritative_symbol = get_field(row, "Symbol_from_nomenclature_authority")
            syns.add(authoritative_symbol)
            authoritative_full_name = get_field(row, "Full_name_from_nomenclature_authority")
            syns.add(authoritative_full_name)
            others = set(get_field(row, "Other_designations").split('|'))
            syns.update(others)
            for syn in syns:
                if syn == '-':
                    raise RuntimeError(f"Synonym '-' should not be present in NCBIGene output!")
                synfile.write(f'{gene_id}\thttp://www.geneontology.org/formats/oboInOwl#hasSynonym\t{syn}\n')

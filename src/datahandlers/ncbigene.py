from src.babel_utils import make_local_name, pull_via_urllib
import gzip


def pull_ncbigene(filenames):
    for fn in filenames:
        pull_via_urllib('https://ftp.ncbi.nlm.nih.gov/gene/DATA/', fn, decompress=False, subpath='NCBIGene')

def get_ncbigene_field(row, header, field_name):
    """
    A helper function for returning the value of a field in gene_info.gz by name.
    The value is `-` if no value is present, so we need to convert that into an empty string.

    :param r: A row from gene_info.gz.
    :param field_name: A field name in the header of gene_info.gz.
    :return: The value in this column in this row, otherwise the empty string ('').
    """
    index = header.index(field_name)
    value = row[index].strip()
    if value == '-':
        return ''
    return value

def pull_ncbigene_labels_synonyms_and_taxa(gene_info_filename, labels_filename, synonyms_filename, taxa_filename, descriptions_filename):
    """
    Extract labels, synonyms, and taxonomic data for genes from the NCBIGene "gene_info.gz" file
    and write them into separate files. The function processes the input file by skipping rows
    with certain unwanted gene types and writing relevant data to output files for gene labels,
    synonyms, and taxonomy associations. Only rows conforming to the required gene type are processed.

    The output files include:
    1. Label file: Gene IDs mapped to their canonical labels (symbols).
    2. Synonym file: Gene IDs mapped to associated synonyms.
    3. Taxa file: Gene IDs mapped to their corresponding taxonomy identifiers.

    :raises AssertionError: If the file headers in "gene_info.gz" do not match the expected format.
    :raises RuntimeError: If a synonym value of `-` is encountered in the output, indicating unexpected
        processing behavior in the input file.

    :return: None
    """

    # File format described here: https://ftp.ncbi.nlm.nih.gov/gene/DATA/README
    bad_gene_types = {'biological-region', 'other', 'unknown'}
    with gzip.open(gene_info_filename, 'r') as inf, \
          open(labels_filename, 'w') as labelfile, \
          open(synonyms_filename, 'w') as synfile, \
          open(taxa_filename, 'w') as taxafile, \
          open(descriptions_filename, 'w') as descriptionfile:

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

        for line in inf:
            sline = line.decode('utf-8')
            row = sline.strip().split('\t')
            gene_id = f'NCBIGene:{get_ncbigene_field(row, header, "GeneID")}'
            gene_type = get_ncbigene_field(row, header, "type_of_gene")
            if gene_type in bad_gene_types:
                continue
            taxafile.write(f'{gene_id}\tNCBITaxon:{get_ncbigene_field(row, header, "#tax_id")}\n')

            # Write out all the synonyms.
            syns = set(get_ncbigene_field(row, header, "Full_name_from_nomenclature_authority").split('|'))
            syns.update(get_ncbigene_field(row, header, "Synonyms").split('|'))
            syns.update(get_ncbigene_field(row, header, "Other_designations").split('|'))
            # syns.add(get_ncbigene_field(row, header, "description"))
            syns.add(get_ncbigene_field(row, header, "Symbol_from_nomenclature_authority"))
            syns.add(get_ncbigene_field(row, header, "Symbol"))
            for syn in syns:
                # Skip empty synonym.
                if syn.strip() == '' or syn.strip() == '-':
                    continue

                # gene_info.gz uses `-` to indicate a blank field -- if we're seeing that here, that means
                # we've misread the file somehow!
                if syn == '-':
                    raise RuntimeError("Synonym '-' should not be present in NCBIGene output!")

                synfile.write(f'{gene_id}\thttp://www.geneontology.org/formats/oboInOwl#hasSynonym\t{syn}\n')

            # Figure out the label. We would ideally go with:
            #   {Symbol_from_nomenclature_authority || Symbol}: {Full_name_from_nomenclature_authority}
            # But falling back cleanly. As per https://github.com/TranslatorSRI/Babel/issues/429
            best_symbol = get_ncbigene_field(row, header, "Symbol_from_nomenclature_authority")
            if not best_symbol:
                # Fallback to the "Symbol" field.
                best_symbol = get_ncbigene_field(row, header, "Symbol")
            if not best_symbol and len(syns) > 0:
                # Fallback to the first synonym.
                best_symbol = syns[0]

            labelfile.write(f'{gene_id}\t{best_symbol}\n')

            # Write a description file for this description record.
            description = get_ncbigene_field(row, header, "description")
            descriptionfile.write(f'{gene_id}\t{description}\n')


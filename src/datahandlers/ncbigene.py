from src.babel_utils import make_local_name, pull_via_urllib
import gzip


def pull_ncbigene(filenames):
    for fn in filenames:
        pull_via_urllib('https://ftp.ncbi.nlm.nih.gov/gene/DATA/', fn, decompress=False, subpath='NCBIGene')


def pull_ncbigene_labels_synonyms_and_taxa():
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
    ifname = make_local_name('gene_info.gz', subpath='NCBIGene')
    labelname = make_local_name('labels', subpath='NCBIGene')
    synname = make_local_name('synonyms', subpath='NCBIGene')
    taxaname = make_local_name('taxa', subpath='NCBIGene')
    bad_gene_types = {'biological-region', 'other', 'unknown'}
    with gzip.open(ifname, 'r') as inf, \
          open(labelname, 'w') as labelfile, \
          open(synname, 'w') as synfile, \
          open(taxaname, 'w') as taxafile:

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

        def get_field(r, field_name):
            """
            A helper function for returning the value of a field in gene_info.gz by name.
            The value is `-` if no value is present; we need to convert that into an empty string.

            :param r: A row from gene_info.gz.
            :param field_name: A field name in the header of gene_info.gz.
            :return: The value in this column in this row, otherwise the empty string ('').
            """
            index = header.index(field_name)
            value = r[index].strip()
            if value == '-':
                return ''
            return value

        for line in inf:
            sline = line.decode('utf-8')
            row = sline.strip().split('\t')
            gene_id = f'NCBIGene:{get_field(row, "GeneID")}'
            gene_type = get_field(row, "type_of_gene")
            if gene_type in bad_gene_types:
                continue
            taxafile.write(f'{gene_id}\tNCBITaxon:{get_field(row, "#tax_id")}\n')

            # Write out all the synonyms.
            syns = set(get_field(row, "Synonyms").split('|'))
            syns.add(get_field(row, "description"))
            syns.add(get_field(row, "Symbol"))
            syns.add(get_field(row, "Symbol_from_nomenclature_authority"))
            syns.add(get_field(row, "Full_name_from_nomenclature_authority"))
            syns.update(get_field(row, "Other_designations").split('|'))
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
            best_symbol = get_field(row, "Symbol_from_nomenclature_authority")
            if not best_symbol:
                # Fallback to the "Symbol" field.
                best_symbol = get_field(row, "Symbol")
            if not best_symbol and len(syns) > 0:
                # Fallback to the first synonym.
                best_symbol = syns[0]
            best_description = get_field(row, "Full_name_from_nomenclature_authority")
            if not best_description:
                best_description = get_field(row, "description")
            if best_symbol:
                if best_description:
                    label = f'{best_symbol}: {best_description}'
                else:
                    label = best_symbol
                labelfile.write(f'{gene_id}\t{label}\n')

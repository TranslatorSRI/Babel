#
# properties.py: handle node- and clique-level properties for Babel.
#
# This file contains classes and functions for handling node- and clique-level properties for Babel.
# So far, these have been handled in individual tab-delimited files:
#   babel_downloads/{PREFIX}/labels: "{curie}\t{label}"
#   babel_downloads/{PREFIX}/synonyms: "{curie}\t{property}\t{label}"
#   babel_downloads/{PREFIX}/descriptions: "{curie}\t{description}"
#
# But we want to make it easier to:
#   1. Add and use new node-level properties (https://github.com/TranslatorSRI/Babel/issues/237).
#   2. Allow different parts of the source code to modify the labels for a single prefix
#      (https://github.com/TranslatorSRI/Babel/issues/428) but without overwriting the same
#      file (https://github.com/TranslatorSRI/Babel/issues/398).
#   3. Continue using `labels`, `descriptions` etc. files to signal that those data have been loaded.
#
# SQLite would be a better format for these property files than DuckDB files. But since these files should be very small
# and the rest of Babel uses DuckDB quite a bit, we'll store the property tables in DuckDB files.
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass

import duckdb

from src.babel_utils import make_local_name

# Properties currently supported in the property store:
supported_properties = {
    'label': 'http://www.w3.org/2000/01/rdf-schema#label',
    'hasExactSynonym': 'http://www.geneontology.org/formats/oboInOwl#hasExactSynonym',
    'hasRelatedSynonym': 'http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym',
}

LABEL = ['label']
EXACT_SYNONYM = ['hasExactSynonym']
RELATED_SYNONYM = ['hasRelatedSynonym']
SYNONYMS = EXACT_SYNONYM + RELATED_SYNONYM

# A single property value.
@dataclass
class PropertyValue:
    curie: str
    property: str
    value: str
    source: str


# A property store for a single prefix.
class PrefixPropertyStore(AbstractContextManager):
    def __init__(self, prefix_path=None, prefix=None, validate_properties=True, readonly=False):
        self.validate_properties = validate_properties
        self.readonly = readonly

        prefix_directory = None
        if prefix_path is not None:
            prefix_directory = prefix_path
        if prefix is not None:
            prefix_directory = make_local_name(prefix)
        if (not prefix_directory) or (prefix_path is not None and prefix is not None):
            raise ValueError("A PrefixPropertyStore must be created with either a `prefix` or a `prefix_path`, but not both.")

        # Make the prefix directory if it doesn't exist.
        os.makedirs(prefix_directory, exist_ok=True)

        # Normally we would open the database here, but databases in general are slow on inserts and DuckDB is
        # particularly so. Besides, PrefixPropertyStores aren't intended to work like full databases -- we only add
        # to the existing knowledge, and we don't add duplicates. So what we should do instead is to break this up into
        # a Reader and a Writer:
        #   - The Reader opens the database in read-only mode, and can be used to query it.
        #   - The Writer doesn't open the database as well -- it collects new properties into a NumPy table, then
        #     writes it all to the database in one go at commit. (This might be more efficient to do with an in-memory
        #     database that we later attach the file database to.)
        self.connection = duckdb.connect(os.path.join(prefix_directory, 'properties.duckdb'), read_only=self.readonly)
        self.connection.sql("CREATE TABLE IF NOT EXISTS properties (curie TEXT, property TEXT, value TEXT, source TEXT) ;")
        # Create a UNIQUE index on the property values -- this means that if someone tries to set a value for a property
        # either duplicatively or from another source, we simply ignore it.
        self.connection.sql("CREATE UNIQUE INDEX IF NOT EXISTS properties_propvalues ON properties (curie, property, value);")
        self.connection.commit()


    def get_properties(self, curie) -> list[str]:
        return list(map(lambda x: x[0], self.connection.sql("SELECT DISTINCT properties FROM properties WHERE curie=$curie", params={
            "curie": curie,
        }).fetchall()))

    def get(self, curie, prop) -> list[PropertyValue]:
        if self.validate_properties and prop not in supported_properties:
            raise ValueError(f"Unable to get({curie}, {prop}): unsupported property {prop}.")
        results = self.connection.sql("SELECT curie, property, value, source FROM properties WHERE curie=$curie AND property=$property", params={
            "curie": curie,
            "property": supported_properties[prop],
        }).fetchall()
        return [PropertyValue(result[0], result[1], result[2], result[3]) for result in results]

    def get_all(self) -> list[PropertyValue]:
        results = self.connection.sql("SELECT curie, property, value, source FROM properties").fetchall()
        return [PropertyValue(result[0], result[1], result[2], result[3]) for result in results]

    def get_all_by_properties(self, props) -> list[PropertyValue]:
        if self.validate_properties:
            unsupported_properties = list(filter(lambda pr: pr not in supported_properties, props))
            if len(unsupported_properties) > 0:
                raise ValueError(f"Unable to get_all_by_properties({props}): unsupported properties {unsupported_properties}.")
        prop_iris = list(map(lambda pr: supported_properties[pr], props))
        results = self.connection.sql("SELECT curie, property, value, source FROM properties WHERE property IN $props", params={
            'props': prop_iris,
        }).fetchall()
        return [PropertyValue(result[0], result[1], result[2], result[3]) for result in results]


    def get_all_for_curie(self, curie) -> list[PropertyValue]:
        all_props = self.get_properties(curie)
        return [pv for prop in all_props for pv in self.get(curie, prop)]

    def get_values(self, curie) -> list[str]:
        return list(map(lambda pv: pv.value, self.get_all(curie)))

    def insert_all(self, pvs):
        if self.autocommit:
            self.connection.begin()
        for pv in pvs:
            if self.validate_properties and pv.property not in supported_properties:
                raise ValueError(f"Unable to insert_all({pvs}): unsupported property {pv.property} in {pv}.")
            self.connection.sql("INSERT OR IGNORE INTO properties VALUES ($curie, $property, $value, $source)", params={
                "curie": pv.curie,
                "property": supported_properties[pv.property],
                "value": pv.value,
                "source": pv.source,
            })
        if self.autocommit:
            self.connection.commit()

    def insert_values(self, curie, prop, values, source):
        if self.validate_properties and prop not in supported_properties:
            raise ValueError(f"Unable to insert_values({curie}, {prop}, {values}, {source}): unsupported property {prop}.")
        if self.autocommit:
            self.connection.begin()
        for value in values:
            self.connection.sql("INSERT OR IGNORE INTO properties VALUES ($curie, $property, $value, $source)", params={
                "curie": curie,
                "property": supported_properties[prop],
                "value": value,
                "source": source,
            })
        if self.autocommit:
            self.connection.commit()

    def begin_transaction(self):
        self.connection.begin()

    def commit_transaction(self):
        self.connection.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def to_tsv(self, file, properties, include_properties=False, include_sources=False):
        for pv in self.get_all_by_properties(properties):
            output = [pv.curie, pv.value]
            if include_properties:
                output.insert(1, pv.property)
            if include_sources:
                output.append(pv.source)
            file.write('\t'.join(output) + '\n')

    def to_labels_tsv(self, file):
        self.to_tsv(file, LABEL, include_properties=False, include_sources=False)

    def to_synonyms_tsv(self, file):
        self.to_tsv(file, SYNONYMS, include_properties=True, include_sources=False)

    def add_label(self, curie, label, source):
        self.insert_values(curie, 'label', [label], source)

    def add_exact_synonym(self, curie, synonym, source):
        self.insert_values(curie, 'hasExactSynonym', [synonym], source)

    def add_related_synonym(self, curie, synonym, source):
        self.insert_values(curie, 'hasRelatedSynonym', [synonym], source)

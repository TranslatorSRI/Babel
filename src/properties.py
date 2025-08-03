#
# properties.py: handle node- and clique-level properties for Babel.
#
# It would be great if we could get all Babel properties (labels, synonyms, etc.) stored in the same database
# store, but that appears to be impractical given how long it takes to write into a database. So we'll leave
# labels, synonyms and descriptions working in the current system, and start putting new properties (starting with
# hasAdditionalId) into this database. I'd love to get descriptions moved in here as well.
#
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass

import sqlite3

from src.babel_utils import make_local_name

# Properties currently supported in the property store:
supported_properties = {
    'hasAdditionalId': 'http://www.geneontology.org/formats/oboInOwl#hasAlternativeId',
}

HAS_ADDITIONAL_ID = 'hasAdditionalId'

# A single property value.
@dataclass
class PropertyValue:
    curie: str
    property: str
    value: str
    description: str


# A property store for a properties file.
class PropertyStore(AbstractContextManager):
    def __init__(self, db3file_path, validate_properties=True, autocommit=True):
        self.validate_properties = validate_properties
        self.autocommit = autocommit

        # Make the prefix directory if it doesn't exist.
        os.makedirs(os.path.dirname(db3file_path), exist_ok=True)

        self.connection = sqlite3.connect(db3file_path)
        cur = self.connection.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS properties (curie TEXT, property TEXT, value TEXT, description TEXT) ;")
        # Create a UNIQUE index on the property values -- this means that if someone tries to set the same value for
        # a property either duplicatively or from another source, we simply ignore it.
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS properties_propvalues ON properties (curie, property, value);")
        self.connection.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def query(self, sql, params=None):
        cursor = self.connection.cursor()
        return cursor.execute(sql, params)

    def get_by_curie(self, curie) -> list[PropertyValue]:
        results = self.query("SELECT curie, property, value, source FROM properties WHERE curie=:curie", params={
            "curie": curie,
        })
        return [PropertyValue(result[0], result[1], result[2], result[3]) for result in results]

    def get_all(self) -> list[PropertyValue]:
        results = self.query("SELECT curie, property, value, source FROM properties")
        return [PropertyValue(result[0], result[1], result[2], result[3]) for result in results]

    def insert_all(self, pvs):
        cursor = self.connection.cursor()
        data = []
        for pv in pvs:
            if self.validate_properties and pv.property not in supported_properties:
                raise ValueError(f"Unable to insert_all({pvs}): unsupported property {pv.property} in {pv}.")
            data.append({
                "curie": pv.curie,
                "property": supported_properties[pv.property],
                "value": pv.value,
                "source": pv.source,
            })
        cursor.executemany("INSERT OR IGNORE INTO properties VALUES (:curie, :property, :value, :source)", data)
        self.connection.commit()

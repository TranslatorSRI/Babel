#
# properties.py: handle node- and clique-level properties for Babel.
#
# Property files are JSONL files that can be read into and out of the Property dataclass.
# So writing them is easy: you just add each property on its own line, and if you go through
# Property(...).to_json_line() we can even validate it for you (eventually).
#
# We generally need to read multiple properties files so you can run queries over all of them, which you can do by
# using the PropertyList class.
#
import gzip
import json
from collections import defaultdict
from dataclasses import dataclass

#
# SUPPORTED PROPERTIES
#

# HAS_ADDITIONAL_ID indicates
#   - Used by write_compendia() to
HAS_ADDITIONAL_ID = 'http://www.geneontology.org/formats/oboInOwl#hasAlternativeId'

# Properties currently supported in the property store in one set for validation.
supported_properties = {
    HAS_ADDITIONAL_ID,
}

#
# The Property dataclass can be used to encapsulate a property for a CURIE. It has helper code to read
# and write these properties.
#

@dataclass
class Property:
    """
    A property value for a CURIE.
    """

    curie: str
    property: str
    value: str
    source: str

    @staticmethod
    def valid_keys():
        return ['curie', 'property', 'value', 'source']

    def __post_init__(self):
        """
        Make sure this Property makes sense.
        """
        if self.property not in supported_properties:
            raise ValueError(f'Property {self.property} is not supported (supported properties: {supported_properties})')

    @staticmethod
    def from_dict(prop):
        """
        Read this dictionary into a Property.

        :return: A Property version of this JSON line.
        """

        # Check if this dictionary includes keys that aren't valid in a Property.
        unexpected_keys = prop.keys() - Property.valid_keys()
        if len(unexpected_keys) > 0:
            raise ValueError(f'Unexpected keys in dictionary to be converted to Property ({unexpected_keys}): {json.dumps(prop, sort_keys=True, indent=2)}')

        return Property(**prop)

    # TODO: we should have some validation code in here so people don't make nonsense properties, which means
    # validating both the property and the value.

    def to_json_line(self):
        """
        Returns this property as a JSONL line, including the final newline (so you can write it directly to a file).

        :return: A string containing the JSONL line of this property.
        """
        return json.dumps({
            'curie': self.curie,
            'property': self.property,
            'value': self.value,
            'source': self.source,
        }) + '\n'

#
# The PropertyList object can be used to load and query properties from multiple sources.
#
# We could write them into a DuckDB file as we load them so they can overflow onto disk as needed, but that's overkill
# for right now, so we'll just load them all into memory.
#

class PropertyList:
    """
    This class can be used to load multiple property files for simultaneous querying.

    In order to support the existing property files, we will additionally support the two main alternate formats we use:
    - A three column TSV file, with columns: CURIE, property, value
    - A four column TSV file, with columns: CURIE, property, value, source

    But eventually all of those files will be subsumed into JSONL files.
    """

    def __init__(self):
        """
        Create a new PropertyList object.

        Since most of our queries will be CURIE-based, we'll index properties by CURIE, but we'll also keep
        a set of all properties.
        """
        self._properties = set[Property]()
        self._properties_by_curie = defaultdict(set[Property])

    @property
    def properties(self) -> set[Property]:
        return self._properties

    def __getitem__(self, curie: str) -> set[Property]:
        """
        Get all properties for a given CURIE.

        :param curie: The CURIE to look up properties.
        :return: The set of properties for this CURIE.
        """
        return self._properties_by_curie[curie]

    def add_properties(self, props: set[Property]):
        """
        Add a set of Property values to the list.

        :param props: A set of Property values.
        :return: The number of unique properties added.
        """

        props_to_be_added = (props - self._properties)

        self._properties.update(props)
        for prop in props:
            self._properties_by_curie[prop.curie].add(prop)

        return len(props_to_be_added)

    def add_properties_jsonl_gz(self, filename_gz: str):
        """
        Add all the properties in a JSONL Gzipped file.

        :param filename_gz: The properties JSONL Gzipped filename to load.
        :return: The number of unique properties loaded.
        """

        props_to_add = set[Property]()
        with gzip.open(filename_gz, 'rt') as f:
            for line in f:
                props_to_add.add(Property.from_dict(json.loads(line)))

        return self.add_properties(props_to_add)

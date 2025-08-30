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
from dataclasses import dataclass, field

#
# SUPPORTED PROPERTIES
#

# HAS_ALTERNATIVE_ID indicates that CURIE has an alternative ID that should be included in the clique, but NOT
# treated as part of the clique for the purposes of choosing the clique leader. This is used for e.g. ChEBI secondary
# IDs or other deprecated identifiers.
HAS_ALTERNATIVE_ID = 'http://www.geneontology.org/formats/oboInOwl#hasAlternativeId'

# Properties currently supported in the property store in one set for validation.
supported_predicates = {
    HAS_ALTERNATIVE_ID,
}

#
# The Property dataclass can be used to encapsulate a property for a CURIE. It has helper code to read
# and write these properties.
#

@dataclass(frozen=True)
class Property:
    """
    A property value for a CURIE.
    """

    curie: str
    predicate: str
    value: str
    source: str = ""    # TODO: making this a list would be better, but that would make a Property non-frozen, which
                        # would make it harder to uniquify.

    @staticmethod
    def valid_keys():
        return ['curie', 'predicate', 'value', 'source']

    def __post_init__(self):
        """
        Make sure this Property makes sense.
        """
        if self.predicate not in supported_predicates:
            raise ValueError(f'Predicate {self.predicate} is not supported (supported predicates: {supported_predicates})')

    @staticmethod
    def from_dict(prop_dict, source=None):
        """
        Read this dictionary into a Property.

        :param prop_dict: A dictionary containing the property values.
        :param source: The source of this property, if any.
        :return: A Property version of this JSON line.
        """

        # Check if this dictionary includes keys that aren't valid in a Property.
        unexpected_keys = prop_dict.keys() - Property.valid_keys()
        if len(unexpected_keys) > 0:
            raise ValueError(f'Unexpected keys in dictionary to be converted to Property ({unexpected_keys}): {json.dumps(prop_dict, sort_keys=True, indent=2)}')

        prop = Property(**prop_dict)
        return prop

    # TODO: we should have some validation code in here so people don't make nonsense properties, which means
    # validating both the property and the value.

    def to_json_line(self):
        """
        Returns this property as a JSONL line, including the final newline (so you can write it directly to a file).

        :return: A string containing the JSONL line of this property.
        """
        return json.dumps({
            'curie': self.curie,
            'predicate': self.predicate,
            'value': self.value,
            'source': self.source,
        }) + '\n'

#
# The PropertyList object can be used to load and query properties from a particular source.
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

    def count_unique(self):
        """
        Return the number of unique Properties in this PropertyList.

        :return: The number of unique Properties in this PropertyList.
        """
        return len(self._properties)

    @property
    def properties(self) -> set[Property]:
        return self._properties

    def get_all(self, curie: str, predicate: str = None) -> set[Property]:
        """
        Get all properties for a given CURIE.

        :param curie: The CURIE to look up properties.
        :param predicate: If specified, only return properties with this predicate.
        :return: The set of properties for this CURIE.
        """
        props = self._properties_by_curie[curie]

        if predicate is None:
            return props

        if predicate not in supported_predicates:
            raise ValueError(f'Predicate {predicate} is not supported (supported predicates: {supported_predicates})')

        return set(filter(lambda p: p.predicate == predicate, props))

    def add_properties(self, props: set[Property]):
        """
        Add a set of Property values to the list.

        :param props: A set of Property values.
        :param source: The source of these properties, if any.
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
                props_to_add.add(Property.from_dict(json.loads(line), source=filename_gz))

        return self.add_properties(props_to_add)

if __name__ == '__main__':
    pl = PropertyList()
    ps = set[Property]()
    ps.add(Property('A', HAS_ALTERNATIVE_ID, 'B', source='E and F'))
    ps.add(Property('A', HAS_ALTERNATIVE_ID, 'C'))
    ps.add(Property('A', HAS_ALTERNATIVE_ID, 'D'))
    ps.add(Property('A', HAS_ALTERNATIVE_ID, 'C'))
    pl.add_properties(ps)
    print(pl.properties)
    assert len(pl.properties) == 3

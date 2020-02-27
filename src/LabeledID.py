# standard modules
from typing import NamedTuple

#logger = LoggingUtil.init_logging(__name__, logging.ERROR)

class LabeledID(NamedTuple):
    """
    Labeled Thing Object
    ---
    schema:
        id: LabeledID
        required:
            - identifier
        properties:
            identifer:
                type: string
            label:
                type: string
    """
    identifier: str
    label: str = ''

    def __repr__(self):
        return f'({self.identifier},{self.label})'

    def __gt__(self, other):
        return self.identifier > other.identifier

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self,other):
        if not isinstance(other,LabeledID):
            return False
        return self.identifier == other.identifier


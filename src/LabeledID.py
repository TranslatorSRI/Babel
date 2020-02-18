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


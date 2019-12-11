# standard modules
import logging
from typing import NamedTuple

from api.setup import swagger
from src.util import LoggingUtil, Text

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)


@swagger.definition('LabeledID')
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


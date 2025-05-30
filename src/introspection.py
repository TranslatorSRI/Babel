#
# Introspection methods
#
# To help with understanding the various pieces of Babel pipeliees and finding code that is redundant or incorrect,
# this module provides introspection and tagging methods.
#

from collections import defaultdict

tagged_code = defaultdict(list)

def tagged(*tags):
    """
    Decorator to tag a function with a set of tags.

    :param tags: A list of tags to apply to this function.
    """
    def _decorator(func):
        # Add this function to each provided tag.
        for tag in tags:
            tagged_code[tag].extend(func)
        return func
    return _decorator

def get_tagged_code(tag):
    """
    Get a list of tagged functions.
    """
    return tagged_code.get(tag, [])

# Predefined tags to help with code introspection.
def downloader(fn): return tagged('downloader')(fn)
def filterer(fn): return tagged('filterer')(fn)
def transformer(fn): return tagged('transformer')(fn)
def obo_writer(fn): return tagged('obo_writer', 'filterer')(fn)
def efo_writer(fn): return tagged('efo_writer', 'filterer')(fn)
def doid_writer(fn): return tagged('doid_writer', 'filterer')(fn)
def mesh_writer(fn): return tagged('mesh_writer', 'filterer')(fn)
def umls_writer(fn): return tagged('umls_writer', 'filterer')(fn)
def rxnorm_writer(fn): return tagged('rxnorm_writer', 'filterer')(fn)
def ubergraph_writer(fn): return tagged('ubergraph_writer', 'filterer')(fn)
def frink_writer(fn): return tagged('frink_writer', 'filterer')(fn)
def wikidata_writer(fn): return tagged('wikidata_writer', 'filterer')(fn)
def type_determination(fn): return tagged('type_determination')(fn)
def concord_generator(fn): return tagged('concord_generator')(fn)
def compendium_builder(fn): return tagged('compendium_builder')(fn)
def conflation_builder(fn): return tagged('conflation_builder')(fn)
def utility(fn): return tagged('utility')(fn)
def no_longer_needed(fn): return tagged('no_longer_needed')(fn)
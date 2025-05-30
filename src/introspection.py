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
def obo_writer(fn): return tagged('obo_writer')(fn)
def mesh_writer(fn): return tagged('mesh_writer')(fn)
def umls_writer(fn): return tagged('umls_writer')(fn)
def ubergraph_writer(fn): return tagged('ubergraph_writer')(fn)
def frink_writer(fn): return tagged('frink_writer')(fn)
def compendium_builder(fn): return tagged('compendium_builder')(fn)
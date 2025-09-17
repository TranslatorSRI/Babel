import logging
import json
import os
import sys
from time import gmtime

import curies
import yaml
import psutil
from collections import namedtuple
import copy
from logging.handlers import RotatingFileHandler

from bmt import Toolkit
from humanfriendly import format_size

from src.LabeledID import LabeledID
from src.prefixes import OMIM, OMIMPS, UMLS, SNOMEDCT, KEGGPATHWAY, KEGGREACTION, NCIT, ICD10, ICD10CM, ICD11FOUNDATION
import src.prefixes as prefixes

def get_logger(name, loglevel=logging.INFO):
    """
    Get a logger with the specified name.

    The LoggingUtil is inconsistently used, and we don't want rolling logs anyway -- just logging everything to STDERR
    so that Snakemake can capture it is fine. However, we do want every logger to be configured identically and without
    duplicated handlers.
    """

    # Set up the root handler for a logger. Ideally we would call this in one central location, but I'm not sure
    # what they would be for Snakemake. basicConfig() should be safe to call from multiple threads after Python 3.2, but
    # we might as well check.
    if not logging.getLogger().hasHandlers():
        formatter = logging.Formatter('%(levelname)s %(name)s [%(asctime)s]: %(message)s', "%Y-%m-%dT%H:%M:%S%z")
        formatter.converter = gmtime

        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        logging.basicConfig(level=logging.INFO, handlers=[stream_handler])

    # Set up a logger for the specified loglevel and return it.
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)
    return logger

#loggers = {}
class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name, level=logging.INFO, format='short', logFilePath=None, logFileLevel=None):
        logger = logging.getLogger(__name__)
        if not logger.parent.name == 'root':
            return logger

        FORMAT = {
            "short" : '%(funcName)s: %(message)s',
            "medium" : '%(funcName)s: %(asctime)-15s %(message)s',
            "long"  : '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        }[format]

        # create a stream handler (default to console)
        stream_handler = logging.StreamHandler()

        # create a formatter
        formatter = logging.Formatter(FORMAT)

        # set the formatter on the console stream
        stream_handler.setFormatter(formatter)

        # get the name of this logger
        logger = logging.getLogger(name)

        # set the logging level
        logger.setLevel(level)

        # if there was a file path passed in use it
        if logFilePath is not None:
            # create a rotating file handler, 100mb max per file with a max number of 10 files
            file_handler = RotatingFileHandler(filename=logFilePath + name + '.log', maxBytes=1000000, backupCount=10)

            # set the formatter
            file_handler.setFormatter(formatter)

            # if a log level for the file was passed in use it
            if logFileLevel is not None:
                level = logFileLevel

            # set the log level
            file_handler.setLevel(level)

            # add the handler to the logger
            logger.addHandler(file_handler)

        # add the console handler to the logger
        logger.addHandler(stream_handler)

        # return to the caller
        return logger

class Munge(object):
    @staticmethod
    def gene (gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene

class Text:
    """ Utilities for processing text. """
    prefixmap = { x.lower(): x for k,x in vars(prefixes).items() if not k.startswith("__")}

    @staticmethod
    def get_curie (text):
        """
        Return a CURIE from either a text string or a LabeledID.
        Intended to be used with the output from NodeFactory.apply_labels().

        :param text: A LabeledID or a string.
        :return: The CURIE as a string, without changing its case.
        """
        if isinstance(text, LabeledID):
            return text.identifier
        if isinstance(text, str):
            return text
        raise ValueError(f"Unable to get_curie({text}) with type {type(text)}")

    @staticmethod
    def get_prefix_or_none (text):
        if isinstance(text,LabeledID):
            text = text.identifier
        return text.upper().split(':', 1)[0] if ':' in text else None

    @staticmethod
    def get_prefix (id):
        if isinstance(id,LabeledID):
            text = id.identifier
        else:
            text = id
        if ':' in text:
            return text.split(':', 1)[0]
        raise ValueError(f"Unable to get_prefix({id}) with text '{text}': no colons found in identifier.")


    @classmethod
    def recurie(cls,text,new_prefix=None):
        """Given input CURIE and a new prefix, replace the old prefix with the new"""
        if new_prefix is None:
            p = Text.get_prefix(text)
            try:
                new_prefix = cls.prefixmap[p.lower()]
            except KeyError:
                new_prefix = p
            if new_prefix == "ORPHANET":
                print("?")
                print(text)
        if isinstance(text, LabeledID):
            newident = f'{new_prefix}:{Text.un_curie(text.identifier)}'
            return LabeledID(newident,text.label)
        return f'{new_prefix}:{Text.un_curie(text)}'

    @staticmethod
    def un_curie (text):
        return ':'.join(text.split (':', 1)[1:]) if ':' in text else text

    @staticmethod
    def short (obj, limit=80):
        text = str(obj) if obj else None
        return (text[:min(len(text),limit)] + ('...' if len(text)>limit else '')) if text else None

    @staticmethod
    def path_last (text):
        return text.split ('/')[-1:][0] if '/' in text else text

    @staticmethod
    def obo_to_curie (text):
        """Converts /somthing/something/CL_0000001 to CL:0000001"""
        return ':'.join( text.split('/')[-1].split('_') )

    @staticmethod
    def opt_to_curie (text):
        if text is None:
            return None
        #grumble, I should be better about handling prefixes
        if text.startswith('http://purl.obolibrary.org/obo/mondo/sources/icd11foundation/'):
            # This has to go on top because it's a 'purl.obolibrary.org' which doesn't follow the same pattern as the others.
            r = f'{ICD11FOUNDATION}:{text[61:]}'
        elif text.startswith('http://purl.obolibrary.org') or text.startswith('http://www.orpha.net') or text.startswith('http://www.ebi.ac.uk/efo'):
            p = text.split('/')[-1].split('_')
            r = ':'.join( p )
        elif text.startswith('https://omim.org/'):
            ident = text.split("/")[-1]
            if ident.startswith('PS'):
                return f'{OMIMPS}:{ident[2:]}'
            r = f'{OMIM}:{ident}'
        elif text.startswith('http://linkedlifedata.com/resource/umls'):
            r = f'{UMLS}:{text.split("/")[-1]}'
        elif text.startswith('http://identifiers.org/'):
            p = text.split("/")
            r = f'{p[-2].upper()}:{p[-1]}'
        elif text.startswith('http://en.wikipedia.org/wiki'):
            r = f'wikipedia.en:{text.split("/")[-1]}'
        elif text.startswith('http://apps.who.int/classifications/icd10'):
            r = f'{ICD10}:{text.split("/")[-1]}'
        elif text.startswith('http://purl.bioontology.org/ontology/ICD10CM'):
            r = f'{ICD10CM}:{text.split("/")[-1]}'
        elif text.startswith('http://www.snomedbrowser.com/'):
            r = f'{SNOMEDCT}:{text.split("/")[-1]}'
        elif text.startswith('KEGG_PATHWAY'):
            r = Text.recurie(text,KEGGPATHWAY)
        elif text.startswith('NCIt'):
            r = Text.recurie(text,NCIT)
        elif text.startswith('KEGG_REACTION'):
            r = Text.recurie(text,KEGGREACTION)
        else:
            r = text

        if ':' in r:
            return Text.recurie(r)
        else:
            raise ValueError(f"Unable to opt_to_curie({text}): output calculated as {r}, which has no colon.")

        return r

    @staticmethod
    def curie_to_obo (text):
        x = text.split(':')
        return f'<http://purl.obolibrary.org/obo/{x[0]}_{x[1]}>'


    @staticmethod
    def snakify(text):
        decomma = '_'.join( text.split(','))
        dedash = '_'.join( decomma.split('-'))
        resu =  '_'.join( dedash.split() )
        return resu

    @staticmethod
    def upper_curie(text):
        if ':' not in text:
            return text
        p = text.split(':', 1)
        return f'{p[0].upper()}:{p[1]}'



class Resource:
    @staticmethod
    def get_resource_path(resource_name):
        """ Given a string resolve it to a module relative file path unless it is already an absolute path. """
        resource_path = resource_name
        if not resource_path.startswith (os.sep):
            resource_path = os.path.join (os.path.dirname (__file__), resource_path)
        return resource_path
    @staticmethod
    def load_json (path):
        result = None
        with open (path, 'r') as stream:
            result = json.loads (stream.read ())
        return result

    @staticmethod
    def load_yaml (path):
        result = None
        with open (path, 'r') as stream:
            result = yaml.load (stream.read ())
        return result

    def get_resource_obj (resource_name, format='json'):
        result = None
        path = Resource.get_resource_path (resource_name)
        if os.path.exists (path):
            m = {
                'json' : Resource.load_json,
                'yaml' : Resource.load_yaml
            }
            if format in m:
                result = m[format](path)
        return result

    @staticmethod
    # Modified from:
    # Copyright Ferry Boender, released under the MIT license.
    def deepupdate(target, src, overwrite_keys = []):
        """Deep update target dict with src
        For each k,v in src: if k doesn't exist in target, it is deep copied from
        src to target. Otherwise, if v is a list, target[k] is extended with
        src[k]. If v is a set, target[k] is updated with v, If v is a dict,
        recursively deep-update it.

        Updated to deal with yaml structure: if you have a list of yaml dicts,
        want to merge them by "name"

        If there are particular keys you want to overwrite instead of merge, send in overwrite_keys
        """
        if type(src) == dict:
            for k, v in src.items():
                if k in overwrite_keys:
                    target[k] = copy.deepcopy(v)
                elif type(v) == list:
                    if not k in target:
                        target[k] = copy.deepcopy(v)
                    elif type(v[0]) == dict:
                        Resource.deepupdate(target[k],v,overwrite_keys)
                    else:
                        target[k].extend(v)
                elif type(v) == dict:
                    if not k in target:
                        target[k] = copy.deepcopy(v)
                    else:
                        Resource.deepupdate(target[k], v,overwrite_keys)
                elif type(v) == set:
                    if not k in target:
                        target[k] = v.copy()
                    else:
                        target[k].update(v.copy())
                else:
                    target[k] = copy.copy(v)
        else:
            #src is a list of dicts, target is a list of dicts, want to merge by name (yikes)
            src_elements = { x['name']: x for x in src }
            target_elements = { x['name']: x for x in target }
            for name in src_elements:
                if name in target_elements:
                    Resource.deepupdate(target_elements[name], src_elements[name],overwrite_keys)
                else:
                    target.append( src_elements[name] )


class DataStructure:
    @staticmethod
    def to_named_tuple (type_name, d):
        return namedtuple(type_name, d.keys())(**d)


# Cache the config.yaml so we don't need to load it every time get_config() is called.
config_yaml = None
def get_config():
    """
    Retrieve the configuration data from the 'config.yaml' file.

    :return: The configuration data loaded from the 'config.yaml' file.
    """
    global config_yaml
    if config_yaml is not None:
        return config_yaml

    cname = os.path.join(os.path.dirname(__file__),'..', 'config.yaml')
    with open(cname,'r') as yaml_file:
        config_yaml = yaml.safe_load(yaml_file)
    return config_yaml


def get_biolink_model_toolkit(biolink_version):
    """
    Return a BMT Toolkit object for the specified Biolink Model version.

    :param biolink_version: The Biolink Model version to use (e.g. "v4.2.6-rc5").
    :return: A Toolkit instance from the bmt library using the specified Biolink version.
    """
    return Toolkit(f'https://raw.githubusercontent.com/biolink/biolink-model/v{biolink_version}/biolink-model.yaml')


def get_biolink_prefix_map():
    """
    Get the prefix map for the BioLink Model.

    :return: The prefix map for the BioLink Model.
    :raises RuntimeError: If the BioLink version is not supported.
    """
    config = get_config()
    biolink_version = config['biolink_version']
    if biolink_version.startswith('1.') or biolink_version.startswith('2.'):
        raise RuntimeError(f"Biolink version {biolink_version} is not supported.")
    elif biolink_version.startswith('3.'):
        # biolink-model v3.* releases keeps the prefix map in a different place.
        return curies.Converter.from_prefix_map(
            'https://raw.githubusercontent.com/biolink/biolink-model/v' + biolink_version +
            '/prefix-map/biolink-model-prefix-map.json'
        )
    else:
        # biolink-model v4.0.0 and beyond is in the /project directory.
        return curies.Converter.from_prefix_map(
            f'https://raw.githubusercontent.com/biolink/biolink-model/v' + biolink_version +
            '/project/prefixmap/biolink_model_prefix_map.json'
        )

def get_memory_usage_summary():
    """
    Provide a short summary of current memory usage to write into logs.

    :return: A string summarizing current memory usage.
    """
    process = psutil.Process()
    process.memory_percent()
    mem_info = process.memory_info()

    return f"Using {process.memory_percent():.2f}% of available memory (RSS: {format_size(mem_info.rss, binary=True)}, VMS: {format_size(mem_info.vms, binary=True)})"

#!/usr/bin/env python3

#
# comparator.py - A script for comparing Babel files from different runs
#

import click

@click.command()
@click.option('--file-type', type=click.Choice(['compendium']), default='compendium')
@click.argument('file1', type=click.File('r'), required=True)
@click.argument('file2', type=click.File('r'), required=True)
def comparator(file_type, file1, file2):
    """
    Compares two compendium or synonym files.

    :param file_type: Specifies the type of the files to compare.
        Options are 'compendium' or 'synonyms' (not yet supported).
        Defaults to 'compendium'.
    :param file1: First file to compare.
    :param file2: Second file to compare.
    """
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        for line1, line2 in zip(f1, f2):
            # We can't really process them by-line, alas.
            pass
    return True

if __name__ == "__main__":
    comparator()
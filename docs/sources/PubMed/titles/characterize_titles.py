"""Characterize the characters in every ``article_title`` of a pubmed2db export.

Babel writes each title twice: as the PMID's label in the Publication compendium (exactly as exported), and
as a row of ``titles.tsv`` (escaped with ``escape_tsv_field()``). Both decisions depend on which awkward
characters titles actually contain -- newlines and tabs break a TSV row, surrounding whitespace changes a
label -- so this script streams *every* record of the export and counts, per shape:

- ``empty`` -- ``""`` (pubmed2db's spelling of "no title"; never written as a label)
- ``whitespace_only`` -- non-empty but nothing except whitespace
- ``leading_whitespace`` / ``trailing_whitespace``
- ``newline``, ``carriage_return``, ``tab``, ``backslash``
- ``other_control`` -- any other Unicode ``Cc`` character

with up to five example PMIDs each. It reads the shards through the production ``_read_shard()``, so it sees
exactly what the build sees.

Regenerate with (``--export-dir`` defaults to the download of the export pinned in config.yaml):

    uv run python docs/sources/PubMed/titles/characterize_titles.py [--export-dir DIR] [--workers N]

and see "Titles" in docs/sources/PubMed/README.md for what the counts mean for Babel.
"""

import argparse
import json
import os
import unicodedata
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.createcompendia.publications import _read_shard, expected_shard_counts
from src.util import get_config

EXAMPLES_PER_SHAPE = 5
DEFAULT_OUTPUT = Path(__file__).with_name("counts.json")


def title_shapes(title):
    """The shapes (see the module docstring) that one title has; a title can have several."""
    if title == "":
        return ["empty"]
    shapes = []
    if not title.strip():
        shapes.append("whitespace_only")
    else:
        if title[0].isspace():
            shapes.append("leading_whitespace")
        if title[-1].isspace():
            shapes.append("trailing_whitespace")
    for char, shape in (("\n", "newline"), ("\r", "carriage_return"), ("\t", "tab"), ("\\", "backslash")):
        if char in title:
            shapes.append(shape)
    if any(unicodedata.category(c) == "Cc" and c not in "\n\r\t" for c in title):
        shapes.append("other_control")
    return shapes


def characterize_shard(shard_path):
    """Count each shape in one shard; returns (record count, Counter of shapes, {shape: [example PMIDs]})."""
    records = 0
    counts = Counter()
    examples = {}
    for pmid, title, _ in _read_shard(shard_path):
        records += 1
        for shape in title_shapes(title):
            counts[shape] += 1
            shape_examples = examples.setdefault(shape, [])
            if len(shape_examples) < EXAMPLES_PER_SHAPE:
                shape_examples.append({"pmid": pmid, "title": title})
    return records, counts, examples


def main():
    config = get_config()
    default_export_dir = os.path.join(config["download_directory"], "PubMed2DB", config["pubmed2db_version"])
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--export-dir", default=default_export_dir, help="A downloaded pubmed2db export.")
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    shards = list(expected_shard_counts(args.export_dir))
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(characterize_shard, [os.path.join(args.export_dir, s) for s in shards]))

    total = Counter()
    examples = {}
    for _, counts, shard_examples in results:
        total.update(counts)
        for shape, shape_examples in shard_examples.items():
            examples.setdefault(shape, []).extend(shape_examples)
    report = {
        # Shard names, not the local path: this output is committed, and may cover only part of an export.
        "shards": shards,
        "records": sum(records for records, _, _ in results),
        "shapes": {
            shape: {"count": total[shape], "examples": examples[shape][:EXAMPLES_PER_SHAPE]} for shape in sorted(total)
        },
    }
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(json.dumps({shape: info["count"] for shape, info in report["shapes"].items()}, indent=2))


if __name__ == "__main__":
    main()

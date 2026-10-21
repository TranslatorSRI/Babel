"""Refresh the local Biolink Model fixture used by tests/compendium/.

Reads `biolink_version` from `config.yaml`, downloads the matching
`biolink-model.yaml`, `predicate_mapping.yaml`, and
`biolink_model_prefix_map.json` from GitHub, and writes them alongside
this script. Updates the `VERSION` file to record what was downloaded.

Run after bumping `biolink_version` in `config.yaml`. The
`tests/test_biolink_model_freshness.py` test (marked `network`) fails
loudly when this fixture is out of date.

    uv run python tests/fixtures/biolink-model/refresh.py
"""

import os
import sys
import urllib.request

import yaml

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CONFIG_PATH = os.path.join(REPO_ROOT, "config.yaml")
FIXTURE_DIR = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    with open(CONFIG_PATH) as f:
        version = yaml.safe_load(f)["biolink_version"]

    base = f"https://raw.githubusercontent.com/biolink/biolink-model/v{version}"
    files = {
        "biolink-model.yaml": f"{base}/biolink-model.yaml",
        "attributes.yaml": f"{base}/attributes.yaml",
        "predicate_mapping.yaml": f"{base}/predicate_mapping.yaml",
        "biolink_model_prefix_map.json": f"{base}/project/prefixmap/biolink_model_prefix_map.json",
    }
    for name, url in files.items():
        dest = os.path.join(FIXTURE_DIR, name)
        print(f"Downloading {url} -> {dest}")
        urllib.request.urlretrieve(url, dest)

    with open(os.path.join(FIXTURE_DIR, "VERSION"), "w") as f:
        f.write(version + "\n")

    print(f"Refreshed Biolink Model fixture to v{version}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

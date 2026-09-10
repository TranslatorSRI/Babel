"""Unit tests for CURIE-specific label overrides."""

import logging

import pytest
import yaml

from src.label_overrides import LabelOverrideFactory


def make_override_factory(tmp_path, overrides):
    """Write a minimal label_overrides.yaml and return a LabelOverrideFactory for it."""
    yaml_file = tmp_path / "label_overrides.yaml"
    yaml_file.write_text(yaml.dump({"label_overrides": overrides}))
    return LabelOverrideFactory(yaml_file)


@pytest.mark.unit
def test_label_override_replaces_expected_source_label(tmp_path):
    factory = make_override_factory(
        tmp_path,
        {
            "DRUGBANK:DB10626": {
                "replacement_label": "Trout allergenic extract",
                "expected_source_label": "Trout",
                "reason": "Too broad",
            }
        },
    )

    assert factory.apply("DRUGBANK:DB10626", "Trout") == "Trout allergenic extract"


@pytest.mark.unit
def test_label_override_ignores_other_curies(tmp_path):
    factory = make_override_factory(
        tmp_path,
        {
            "DRUGBANK:DB10626": {
                "replacement_label": "Trout allergenic extract",
                "expected_source_label": "Trout",
                "reason": "Too broad",
            }
        },
    )

    assert factory.apply("DRUGBANK:DB99999", "Trout") == "Trout"


@pytest.mark.unit
def test_label_override_allows_redundant_upstream_fix(tmp_path, caplog):
    factory = make_override_factory(
        tmp_path,
        {
            "DRUGBANK:DB10626": {
                "replacement_label": "Trout allergenic extract",
                "expected_source_label": "Trout",
                "reason": "Too broad",
            }
        },
    )

    with caplog.at_level(logging.WARNING, logger="src.label_overrides"):
        label = factory.apply("DRUGBANK:DB10626", "Trout allergenic extract")

    assert label == "Trout allergenic extract"
    assert any("redundant" in record.message for record in caplog.records)


@pytest.mark.unit
def test_label_override_fails_on_unexpected_source_label(tmp_path):
    factory = make_override_factory(
        tmp_path,
        {
            "DRUGBANK:DB10626": {
                "replacement_label": "Trout allergenic extract",
                "expected_source_label": "Trout",
                "reason": "Too broad",
            }
        },
    )

    with pytest.raises(RuntimeError, match="expected source label"):
        factory.apply("DRUGBANK:DB10626", "Rainbow trout")

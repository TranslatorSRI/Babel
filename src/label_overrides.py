"""Manual CURIE-specific label corrections for upstream source labels."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from src.util import get_config, get_logger

logger = get_logger(__name__)

_instance: Optional["LabelOverrideFactory"] = None


@dataclass(frozen=True)
class LabelOverride:
    """A replacement label for one CURIE, with an expected source-label drift check."""

    replacement_label: str
    expected_source_label: str | None
    reason: str


class LabelOverrideFactory:
    """Load and apply manual label overrides from ``input_data/label_overrides.yaml``."""

    def __init__(self, override_file: Path):
        self.override_file = override_file
        self.overrides: dict[str, LabelOverride] = {}
        self._load(override_file)

    def _load(self, path: Path) -> None:
        if not path.exists():
            logger.info(f"LabelOverrideFactory: override file not found at {path}; no labels will be overridden.")
            return

        with open(path) as f:
            data = yaml.safe_load(f) or {}

        raw_overrides = data.get("label_overrides", {})
        if not isinstance(raw_overrides, dict):
            raise ValueError(f"label_overrides must be a CURIE-keyed mapping in {path}")

        for curie, raw in raw_overrides.items():
            if not isinstance(raw, dict):
                raise ValueError(f"Label override for {curie} in {path} must be a mapping")
            replacement_label = raw.get("replacement_label")
            if not replacement_label:
                raise ValueError(f"Label override for {curie} in {path} must include replacement_label")
            self.overrides[curie] = LabelOverride(
                replacement_label=replacement_label,
                expected_source_label=raw.get("expected_source_label"),
                reason=raw.get("reason", "(no reason given)"),
            )

        logger.info(f"LabelOverrideFactory: loaded {len(self.overrides)} label override(s) from {path}")

    def apply(self, curie: str, label: str) -> str:
        """Return the corrected label for ``curie`` when an override exists.

        If ``expected_source_label`` is configured and the observed label no longer
        matches it, fail loudly unless the observed label already equals the replacement.
        This keeps stale manual overrides from silently masking upstream changes.
        """

        if not label or curie not in self.overrides:
            return label

        override = self.overrides[curie]
        source = _source_from_curie(curie)
        if label == override.replacement_label:
            logger.warning(
                f"Label override for {curie} in {self.override_file} is redundant: {source} already emits "
                f"{override.replacement_label!r}. Reason: {override.reason}"
            )
            return label

        if override.expected_source_label is not None and label != override.expected_source_label:
            raise RuntimeError(
                f"Label override for {curie} in {self.override_file} expected source label "
                f"{override.expected_source_label!r}, but {source} emitted {label!r}. Re-review this override. "
                f"Reason: {override.reason}"
            )

        logger.warning(
            f"Overriding label for {curie} from {label!r} to {override.replacement_label!r} in {source}. "
            f"Reason: {override.reason}"
        )
        return override.replacement_label


def _source_from_curie(curie: str) -> str:
    """Return the CURIE prefix for override diagnostics."""

    return curie.split(":", 1)[0] if ":" in curie else "unknown source"


def _get_label_override_factory() -> LabelOverrideFactory:
    """Return the process-wide label override factory."""

    global _instance
    if _instance is None:
        config = get_config()
        override_file = Path(config.get("input_directory", "input_data")) / "label_overrides.yaml"
        _instance = LabelOverrideFactory(override_file)
    return _instance


def apply_label_override(curie: str, label: str) -> str:
    """Apply any configured manual label override for ``curie``."""

    return _get_label_override_factory().apply(curie, label)

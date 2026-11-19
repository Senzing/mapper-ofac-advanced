"""The payload attributes collected from features and identifiers must never share a key with the ones
``_add_payload_attributes`` fills in afterwards.

``_join_payload_values`` finishes the collected feature/identifier payloads first, and
``_add_payload_attributes`` then fills its own keys with ``setdefault``. If a key could appear in both,
the earlier (feature-derived) value would silently win. The two key sets are disjoint today; this test
fails loudly if a mapping change ever makes them overlap.
"""

# The mapper's private payload helper is the unit under test here: its source is read to find its keys.
# pylint: disable=protected-access

import inspect
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# pylint: disable=wrong-import-position,import-error
import ofac_advanced_mapper  # noqa: E402
from config.feature_mappings import FEATURE_MAPPINGS  # noqa: E402
from config.id_doc_mappings import ID_DOC_MAPPINGS  # noqa: E402

KEY = re.compile(r"'([A-Z][A-Z0-9_]{3,})'")


def _sanctions_payload_keys() -> set:
    """Literal keys ``_add_payload_attributes`` writes, read from its source so the test cannot drift."""
    source = inspect.getsource(ofac_advanced_mapper.StrictOFACTransformer._add_payload_attributes)
    return set(re.findall(r'record(?:\.setdefault\(|\[)\s*"([A-Z0-9_]+)"', source))


def _feature_payload_keys() -> set:
    keys: set = set()
    for mapping in FEATURE_MAPPINGS.values():
        section = mapping["section"]
        if "PAYLOAD" in section.upper() or section.startswith("DESCRIPTIVE"):
            keys |= set(KEY.findall(repr(mapping["instructions"])))
    return keys


def _identifier_payload_keys() -> set:
    keys: set = set()
    for mapping in ID_DOC_MAPPINGS.values():
        if mapping.get("payload"):
            keys |= set(KEY.findall(repr(mapping["instructions"])))
    return keys


def test_the_key_extraction_is_not_vacuous():
    """A collision check that found no keys would pass for the wrong reason."""
    assert len(_sanctions_payload_keys()) >= 6
    assert len(_feature_payload_keys()) >= 10
    assert {"GOVERNMENT_GAZETTE_NUMBER", "CHINESE_COMMERCIAL_CODE"} <= _identifier_payload_keys()


def test_feature_payload_keys_never_collide_with_the_sanctions_payload_keys():
    assert not _feature_payload_keys() & _sanctions_payload_keys()


def test_identifier_payload_keys_never_collide_with_the_sanctions_payload_keys():
    assert not _identifier_payload_keys() & _sanctions_payload_keys()

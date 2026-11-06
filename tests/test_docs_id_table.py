"""docs/id-type-normalization.md carries a table generated from ID_DOC_MAPPINGS; it must match the code.

Regenerate after changing the mapping:  python tests/test_docs_id_table.py
"""

import importlib.util
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DOC = _ROOT / "docs" / "id-type-normalization.md"
_BEGIN = "<!-- BEGIN GENERATED ID TABLE (python tests/test_docs_id_table.py) -->"
_END = "<!-- END GENERATED ID TABLE -->"

_spec = importlib.util.spec_from_file_location("id_doc_mappings", _ROOT / "src" / "config" / "id_doc_mappings.py")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
ID_DOC_MAPPINGS = _mod.ID_DOC_MAPPINGS

_FEATURE_PREFIXES = (
    "NATIONAL_ID",
    "TAX_ID",
    "OTHER_ID",
    "PASSPORT",
    "SSN",
    "DRIVERS_LICENSE",
    "LEI",
    "IMO_NUMBER",
    "MMSI_NUMBER",
)


def _row(id_, entry):
    attrs = dict(entry["instructions"])
    if entry.get("payload") is True:
        feature = "payload " + ", ".join(f"`{a}`" for a in attrs)
    else:
        feature = next(f"`{p}`" for p in _FEATURE_PREFIXES if any(a == p or a.startswith(p + "_") for a in attrs))
    typ = next((f"`{v}`" for a, v in attrs.items() if a.endswith("_TYPE") and v is not None), "-")
    by_country = entry.get("type_by_country", {})
    if by_country:
        typ = ", ".join(f"`{v}` if {k}" for k, v in sorted(by_country.items())) + ("" if typ == "-" else f"; {typ}")
    subtype = next((f"`{v}`" for a, v in attrs.items() if a.endswith("_SUBTYPE") and v is not None), "-")
    country = next((f"`{v}`" for a, v in attrs.items() if a.endswith("_COUNTRY") and v is not None), "-")
    return f"| {id_} | {entry['name']} | {feature} | {typ} | {subtype} | {country} |"


def render_table():
    """The markdown table, one row per mapping entry, in ID order."""
    lines = [
        "| OFAC type | OFAC label | Senzing feature | Type value | Subtype | Fixed country |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    lines += [_row(id_, ID_DOC_MAPPINGS[id_]) for id_ in sorted(ID_DOC_MAPPINGS)]
    return "\n".join(lines)


def _replace_section(text):
    head, rest = text.split(_BEGIN, 1)
    _, tail = rest.split(_END, 1)
    return f"{head}{_BEGIN}\n\n{render_table()}\n\n{_END}{tail}"


def test_doc_table_matches_mapping():
    """The generated section of the doc equals a fresh render of the mapping."""
    text = _DOC.read_text(encoding="utf-8")
    assert _BEGIN in text and _END in text
    assert _replace_section(text) == text, "docs table is stale: run python tests/test_docs_id_table.py"


if __name__ == "__main__":
    _DOC.write_text(_replace_section(_DOC.read_text(encoding="utf-8")), encoding="utf-8")
    sys.exit(0)

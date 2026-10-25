"""A person's name is NAME_FULL; an organization, vessel or aircraft name is NAME_ORG; an unknown type is NAME_FULL."""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ofac_advanced_mapper import (  # noqa: E402  pylint: disable=wrong-import-position,import-error
    NS,
    StrictOFACTransformer,
)

OFAC = NS["ofac"]
# The live feed's shape: a DocumentedNamePart has no type attribute; its NamePartValue points at a
# NamePartGroup, which the Identity declares with a NamePartTypeID (1520 Last Name, 1521 First Name, ...).
PART = "<DocumentedNamePart><NamePartValue NamePartGroupID='{group}'>{value}</NamePartValue></DocumentedNamePart>"
GROUP = "<MasterNamePartGroup><NamePartGroup ID='{group}' NamePartTypeID='{type_id}'/></MasterNamePartGroup>"


def _party(parts):
    """A DistinctParty with one primary documented name made of (NamePartTypeID, value) parts."""
    body = "".join(PART.format(group=i, value=v) for i, (_, v) in enumerate(parts))
    groups = "".join(GROUP.format(group=i, type_id=t) for i, (t, _) in enumerate(parts))
    return ET.fromstring(
        f'<DistinctParty xmlns="{OFAC}" FixedRef="1"><Profile ID="1"><Identity>'
        f'<Alias Primary="true" AliasTypeID="1403"><DocumentedName>{body}</DocumentedName></Alias>'
        f"<NamePartGroups>{groups}</NamePartGroups></Identity></Profile></DistinctParty>"
    )


NAME_PARTS = {"NAME_FIRST", "NAME_LAST", "NAME_MIDDLE", "NAME_SUFFIX", "NAME_PREFIX"}


def _names(is_person, parts):
    """The name features produced for one party."""
    mapper = StrictOFACTransformer.__new__(StrictOFACTransformer)  # the name step needs no loaded XML
    record = {"FEATURES": []}
    mapper._add_names(record, _party(parts), is_person=is_person)  # pylint: disable=protected-access
    return [f for f in record["FEATURES"] if any(k.startswith("NAME_") and k != "NAME_TYPE" for k in f)]


def test_person_name_is_name_full_without_parts():
    """A person is NAME_FULL only: typed parts (Last/First Name) are joined, never emitted as NAME_* parts."""
    (name,) = _names(True, [("1520", "ZUMAR"), ("1521", "ABOUD")])
    assert name == {"NAME_FULL": "ZUMAR ABOUD", "NAME_TYPE": "PRIMARY"}
    assert not NAME_PARTS & set(name)


@pytest.mark.parametrize("parts", [[("1525", "CIMEX, S.A.")], [("1526", "MAR"), ("1526", "AZUL")]])
def test_non_person_name_is_name_org_without_parts(parts):
    """An organization, vessel or aircraft name is NAME_ORG only: no NAME_FULL and no parsed parts."""
    (name,) = _names(False, parts)
    assert name["NAME_ORG"] == " ".join(v for _, v in parts)
    assert name["NAME_TYPE"] == "PRIMARY"
    assert not ({"NAME_FULL"} | NAME_PARTS) & set(name)


def _profile(subtype):
    """A Profile with the given PartySubTypeID (None -> attribute absent)."""
    attr = f' PartySubTypeID="{subtype}"' if subtype is not None else ""
    return ET.fromstring(f'<Profile xmlns="{OFAC}" ID="1"{attr}/>')


def _record_for(subtype):
    """The RECORD_TYPE step's output for one profile."""
    record = {"FEATURES": []}
    StrictOFACTransformer()._add_record_type(record, _profile(subtype))  # pylint: disable=protected-access
    return record


@pytest.mark.parametrize(
    ("subtype", "expected"), [("1", False), ("2", False), ("3", False), ("4", True), (None, None), ("9", None)]
)
def test_is_person_decision(subtype, expected):
    """PERSON -> True; vessel/aircraft/organization -> False; missing or unrecognized subtype -> None."""
    assert StrictOFACTransformer._is_person(_record_for(subtype)) is expected  # pylint: disable=protected-access


def test_unrecognized_subtype_emits_no_record_type_and_warns(caplog):
    """An unrecognized (or non-numeric) subtype adds no RECORD_TYPE and logs a warning."""
    for subtype in ("9", "x"):
        assert not _record_for(subtype)["FEATURES"]
    assert "PartySubTypeID=9" in caplog.text and "PartySubTypeID=x" in caplog.text


def test_name_without_record_type_is_name_full_without_parts():
    """Unknown type (e.g. a person whose profile has no subtype): NAME_FULL only, never NAME_ORG or parts."""
    (name,) = _names(None, [("1520", "ZUMAR"), ("1521", "ABOUD")])
    assert name == {"NAME_FULL": "ZUMAR ABOUD", "NAME_TYPE": "PRIMARY"}


def test_person_without_record_type_end_to_end():
    """A party whose Profile lacks PartySubTypeID gets NAME_FULL names (not NAME_ORG) from _transform_party."""
    party = _party([("1520", "ZUMAR"), ("1521", "ABOUD")])
    record = StrictOFACTransformer()._transform_party(party)  # pylint: disable=protected-access
    assert not any("RECORD_TYPE" in f for f in record["FEATURES"])
    names = [f for f in record["FEATURES"] if "NAME_TYPE" in f]
    assert names == [{"NAME_FULL": "ZUMAR ABOUD", "NAME_TYPE": "PRIMARY"}]

"""A person's name is NAME_FULL; every other record type's name is NAME_ORG."""

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
PART = "<DocumentedNamePart NamePartTypeID='{type_id}'><NamePartValue>{value}</NamePartValue></DocumentedNamePart>"


def _party(parts):
    """A DistinctParty with one primary documented name made of (NamePartTypeID, value) parts."""
    body = "".join(PART.format(type_id=t, value=v) for t, v in parts)
    return ET.fromstring(
        f'<DistinctParty xmlns="{OFAC}" FixedRef="1"><Profile ID="1"><Identity>'
        f'<Alias Primary="true" AliasTypeID="1403"><DocumentedName>{body}</DocumentedName></Alias>'
        "</Identity></Profile></DistinctParty>"
    )


def _names(is_person, parts):
    """The name features produced for one party."""
    mapper = StrictOFACTransformer.__new__(StrictOFACTransformer)  # the name step needs no loaded XML
    record = {"FEATURES": []}
    mapper._add_names(record, _party(parts), is_person=is_person)  # pylint: disable=protected-access
    return [f for f in record["FEATURES"] if any(k.startswith("NAME_") and k != "NAME_TYPE" for k in f)]


def test_person_name_is_name_full_with_parts():
    """A person keeps NAME_FULL and its parsed first/last parts."""
    (name,) = _names(True, [("1481", "ZUMAR"), ("1480", "ABOUD")])
    assert name["NAME_FULL"] == "ZUMAR ABOUD"
    assert name["NAME_LAST"] == "ZUMAR" and name["NAME_FIRST"] == "ABOUD"
    assert "NAME_ORG" not in name


@pytest.mark.parametrize("parts", [[("1481", "CIMEX, S.A.")], [("1481", "MAR"), ("1481", "AZUL")]])
def test_non_person_name_is_name_org_without_parts(parts):
    """An organization, vessel or aircraft name is NAME_ORG only: no NAME_FULL and no parsed parts."""
    (name,) = _names(False, parts)
    assert name["NAME_ORG"] == " ".join(v for _, v in parts)
    assert name["NAME_TYPE"] == "PRIMARY"
    assert not {"NAME_FULL", "NAME_FIRST", "NAME_LAST"} & set(name)

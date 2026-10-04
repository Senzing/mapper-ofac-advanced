"""A birth date is a DatePeriod/Start/From; a nationality is a Location reference whose text is the country."""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ofac_advanced_mapper import (  # noqa: E402  pylint: disable=wrong-import-position,import-error
    NS,
    StrictOFACTransformer,
)

OFAC = NS["ofac"]


def _mapper(locations=None):
    """A transformer with no loaded XML and the given Location lookup."""
    mapper = StrictOFACTransformer.__new__(StrictOFACTransformer)
    mapper.location_lookup = locations or {}
    mapper.country_lookup = {}
    return mapper


def _feature(inner):
    return ET.fromstring(
        f'<Feature xmlns="{OFAC}" ID="1" FeatureTypeID="8"><FeatureVersion ID="2">{inner}</FeatureVersion></Feature>'
    )


def _period(year, month=None, day=None):
    parts = f"<Year>{year}</Year>" + (f"<Month>{month}</Month>" if month else "") + (f"<Day>{day}</Day>" if day else "")
    return f'<DatePeriod CalendarTypeID="1"><Start Approximate="false"><From>{parts}</From></Start></DatePeriod>'


def test_birthdate_from_date_period():
    """A full date, a year and month, and a year alone."""
    mapper = _mapper()
    assert (
        mapper._extract_feature_date(_feature(_period(1948, 12, 10))) == "1948-12-10"
    )  # pylint: disable=protected-access
    assert mapper._extract_feature_date(_feature(_period(1948, 12))) == "1948-12"  # pylint: disable=protected-access
    assert mapper._extract_feature_date(_feature(_period(1948))) == "1948"  # pylint: disable=protected-access


def test_birthdate_end_to_end_attribute():
    """The DATE_OF_BIRTH attribute is emitted for a Birthdate feature."""
    mapper = _mapper()
    attrs = mapper._build_attribute_dict(
        _feature(_period(1948, 12, 10)), [("DATE_OF_BIRTH", None)]
    )  # pylint: disable=protected-access
    assert attrs == {"DATE_OF_BIRTH": "1948-12-10"}


def test_nationality_from_location_text():
    """A Nationality feature points at a Location whose text is the country."""
    location = ET.fromstring(
        f'<Location xmlns="{OFAC}" ID="186082"><LocationPart LocPartTypeID="1"><LocationPartValue Primary="true">'
        "<Value>Egypt</Value></LocationPartValue></LocationPart></Location>"
    )
    mapper = _mapper({"186082": location})
    feature = _feature('<VersionLocation LocationID="186082"/>')
    attrs = mapper._build_attribute_dict(feature, [("NATIONALITY", None)])  # pylint: disable=protected-access
    assert attrs == {"NATIONALITY": "Egypt"}

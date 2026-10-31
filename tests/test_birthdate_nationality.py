"""A date is a DatePeriod emitted at the precision its bounds share; a nationality is a Location reference."""

# The mapper's private feature helpers are the unit under test here, so the tests call them directly.
# pylint: disable=protected-access

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ofac_advanced_mapper import (  # noqa: E402  pylint: disable=wrong-import-position,import-error
    MAX_DATE_SPAN_YEARS,
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
    assert mapper._extract_feature_dates(_feature(_period(1948, 12, 10))) == ["1948-12-10"]
    assert mapper._extract_feature_dates(_feature(_period(1948, 12))) == ["1948-12"]
    assert mapper._extract_feature_dates(_feature(_period(1948))) == ["1948"]


def test_birthdate_end_to_end_attribute():
    """The DATE_OF_BIRTH attribute is emitted for a Birthdate feature."""
    mapper = _mapper()
    attrs = mapper._build_attribute_dict(_feature(_period(1948, 12, 10)), [("DATE_OF_BIRTH", None)])
    assert attrs == {"DATE_OF_BIRTH": "1948-12-10"}


def test_nationality_from_location_text():
    """A Nationality feature points at a Location whose text is the country."""
    location = ET.fromstring(
        f'<Location xmlns="{OFAC}" ID="186082"><LocationPart LocPartTypeID="1"><LocationPartValue Primary="true">'
        "<Value>Egypt</Value></LocationPartValue></LocationPart></Location>"
    )
    mapper = _mapper({"186082": location})
    feature = _feature('<VersionLocation LocationID="186082"/>')
    attrs = mapper._build_attribute_dict(feature, [("NATIONALITY", None)])
    assert attrs == {"NATIONALITY": "Egypt"}


def _span(start_from, end_to, start_to=None, end_from=None):
    """A DatePeriod from Start(From..To) to End(From..To); each bound is (year, month, day)."""

    def ymd(bound):
        return "".join(f"<{tag}>{value}</{tag}>" for tag, value in zip(("Year", "Month", "Day"), bound) if value)

    start_to, end_from = start_to or start_from, end_from or end_to
    return (
        f'<DatePeriod CalendarTypeID="1"><Start Approximate="false"><From>{ymd(start_from)}</From>'
        f'<To>{ymd(start_to)}</To></Start><End Approximate="false"><From>{ymd(end_from)}</From>'
        f"<To>{ymd(end_to)}</To></End></DatePeriod>"
    )


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (_span((1948, 12, 10), (1948, 12, 10)), ["1948-12-10"]),  # exact date: all bounds equal
        (_span((1938, 1, 1), (1938, 12, 31)), ["1938"]),  # year-only, not a fabricated 1938-01-01
        (_span((1946, 8, 1), (1946, 8, 31)), ["1946-08"]),  # month-only
        (_span((1946, 9, 26), (1946, 12, 7)), ["1946"]),  # two dates in one year -> the year they share
        (_span((1961, 1, 1), (1962, 12, 31)), ["1961", "1962"]),  # 2-year span -> both years
        # windowed bounds (Start From..To, End From..To), N = 3 years
        (_span((1955, 1, 1), (1957, 12, 31), (1955, 12, 31), (1957, 1, 1)), ["1955", "1956", "1957"]),
        # span crossing a precision boundary: day/month bounds in different years -> whole years only
        (_span((1962, 3, 15), (1963, 2, 10)), ["1962", "1963"]),
    ],
)
def test_date_period_precision(period, expected):
    """Within a year: the precision the bounds share. Across years: every year in the span, inclusive."""
    assert _mapper()._extract_feature_dates(_feature(period)) == expected


@pytest.mark.parametrize("years", [2, 5, 9, 20])
def test_span_of_n_years(years):
    """A span of N years (up to the cap; 9 is the longest on the real file) yields N consecutive years."""
    period = _span((1950, 1, 1), (1950 + years - 1, 12, 31))
    assert _mapper()._extract_feature_dates(_feature(period)) == [str(1950 + i) for i in range(years)]


def test_span_over_cap_emits_bound_years_and_warns(caplog):
    """A span longer than MAX_DATE_SPAN_YEARS is implausible: only its two bound years, plus a warning."""
    last = 1900 + MAX_DATE_SPAN_YEARS  # MAX + 1 years inclusive
    assert _mapper()._extract_feature_dates(_feature(_span((1900, 1, 1), (last, 12, 31)))) == ["1900", str(last)]
    assert "exceeds" in caplog.text


def test_span_reversed_or_non_numeric_bounds():
    """A reversed or non-numeric span degrades to its first bound year."""
    mapper = _mapper()
    assert mapper._span_years("1960", "1950") == ["1960"]
    assert mapper._span_years("19x0", "1955") == ["19x0"]


def test_date_period_open_ended():
    """A period with no End bound (open-ended) is its Start date, not a range."""
    assert _mapper()._extract_feature_dates(_feature(_period(1948, 12, 10))) == ["1948-12-10"]
    assert _mapper()._extract_feature_dates(_feature(_period(1948))) == ["1948"]


def test_multi_year_span_is_one_feature_per_year():
    """_build_attribute_dicts emits one DATE_OF_BIRTH dict per year; a payload date joins the years."""
    mapper = _mapper()
    feature = _feature(_span((1955, 1, 1), (1957, 12, 31)))
    dicts = mapper._build_attribute_dicts(feature, [("DATE_OF_BIRTH", None)])
    assert dicts == [{"DATE_OF_BIRTH": "1955"}, {"DATE_OF_BIRTH": "1956"}, {"DATE_OF_BIRTH": "1957"}]
    record = {"FEATURES": []}
    for values in mapper._build_attribute_dicts(feature, [("AIRCRAFT_MANUFACTURE_DATE", None)]):
        for key, value in values.items():
            mapper._append_payload_value(record, key, value)
    mapper._join_payload_values(record)
    assert record["AIRCRAFT_MANUFACTURE_DATE"] == "1955; 1956; 1957"


def test_day_without_month_is_dropped():
    """A day with no month cannot be placed: the date falls back to the year alone."""
    mapper = _mapper()
    assert mapper._extract_feature_dates(_feature(_span((1950, None, 15), (1950, None, 15)))) == ["1950"]
    assert StrictOFACTransformer._format_date_parts("1950", None, "15") == "1950"


def test_location_text_prefers_primary_value():
    """When a Location has several values, the one flagged Primary is used."""
    location = ET.fromstring(
        f'<Location xmlns="{OFAC}" ID="7"><LocationPart LocPartTypeID="1">'
        '<LocationPartValue Primary="false"><Value>Arab Republic of Egypt</Value></LocationPartValue>'
        '<LocationPartValue Primary="true"><Value>Egypt</Value></LocationPartValue>'
        "</LocationPart></Location>"
    )
    feature = _feature('<VersionLocation LocationID="7"/>')
    assert _mapper({"7": location})._extract_location_text(feature) == "Egypt"


def test_location_text_unknown_location_id():
    """A LocationID that is not in the Locations table yields no text and no NATIONALITY attribute."""
    feature = _feature('<VersionLocation LocationID="404"/>')
    mapper = _mapper()
    assert mapper._extract_location_text(feature) is None
    assert mapper._build_attribute_dict(feature, [("NATIONALITY", None)]) is None

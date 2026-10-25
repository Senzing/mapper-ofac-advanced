"""End to end: a small OFAC Advanced XML through load() and transform(), including identity documents.

The fixture mirrors the live feed's shapes: document types whose feed IDs differ from the mapping keys
(91508 / 92772 are matched by name), a party holding several documents of one payload type, a person,
an organization, a party with an unrecognized subtype, a relationship and a sanctions entry.
"""

# The identity-document step is exercised directly as well as through transform().
# pylint: disable=protected-access

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import ofac_advanced_mapper  # noqa: E402  pylint: disable=wrong-import-position,import-error
from ofac_advanced_mapper import (  # noqa: E402  pylint: disable=wrong-import-position,import-error
    StrictOFACTransformer,
    main,
)

OFAC = ofac_advanced_mapper.NS["ofac"]


def _doc(doc_id, type_id, identity_id, number, country_id=None):
    issued = f' IssuedBy-CountryID="{country_id}"' if country_id else ""
    return (
        f'<IDRegDocument ID="{doc_id}" IDRegDocTypeID="{type_id}" IdentityID="{identity_id}"{issued}>'
        f"<IDRegistrationNo>{number}</IDRegistrationNo></IDRegDocument>"
    )


def _feature(feature_id, type_id, inner, reliability=None):
    rel = f' ReliabilityID="{reliability}"' if reliability else ""
    return (
        f'<Feature ID="{feature_id}" FeatureTypeID="{type_id}">'
        f'<FeatureVersion ID="{feature_id}"{rel}>{inner}</FeatureVersion></Feature>'
    )


def _detail(text):
    return f'<VersionDetail DetailTypeID="1432">{text}</VersionDetail>'


def _loc_part(type_id, value):
    return (
        f'<LocationPart LocPartTypeID="{type_id}">'
        f'<LocationPartValue Primary="true"><Value>{value}</Value></LocationPartValue></LocationPart>'
    )


_MULTI_YEAR_PERIOD = (
    '<DatePeriod CalendarTypeID="1"><Start Approximate="false"><From><Year>1961</Year><Month>1</Month>'
    '<Day>1</Day></From></Start><End Approximate="false"><To><Year>1963</Year><Month>12</Month>'
    "<Day>31</Day></To></End></DatePeriod>"
)


def _alias(name):
    return (
        '<Alias FixedRef="1" AliasTypeID="1403" Primary="true"><DocumentedName ID="1">'
        f"<DocumentedNamePart><NamePartValue>{name}</NamePartValue></DocumentedNamePart>"
        "</DocumentedName></Alias>"
    )


FIXTURE = f"""<?xml version="1.0" encoding="utf-8"?>
<Sanctions xmlns="{OFAC}">
  <ReferenceValueSets>
    <CountryValues>
      <Country ID="11" ISO2="RU">Russia</Country>
      <Country ID="12" ISO2="EG">Egypt</Country>
    </CountryValues>
    <FeatureTypeValues>
      <FeatureType ID="8">Birthdate</FeatureType>
      <FeatureType ID="10">Nationality Country</FeatureType>
      <FeatureType ID="99001">Website Note</FeatureType>
    </FeatureTypeValues>
    <IDRegDocTypeValues>
      <IDRegDocType ID="1571">Passport</IDRegDocType>
      <IDRegDocType ID="1636">Government Gazette Number</IDRegDocType>
      <IDRegDocType ID="91508">Chinese Commercial Code</IDRegDocType>
      <IDRegDocType ID="92772">Russian State Individual Business Registration Number Pattern (OGRNIP)</IDRegDocType>
      <IDRegDocType ID="92883">Entity Code</IDRegDocType>
    </IDRegDocTypeValues>
    <ListValues><List ID="1550">SDN List</List></ListValues>
    <SanctionsTypeValues>
      <SanctionsType ID="1">Program</SanctionsType>
      <SanctionsType ID="2">Block</SanctionsType>
    </SanctionsTypeValues>
    <RelationTypeValues>
      <RelationType ID="15003">Owned or Controlled By</RelationType>
      <RelationType ID="99">Providing Shelter To</RelationType>
    </RelationTypeValues>
    <DetailReferenceValues><DetailReference ID="92084">Yes</DetailReference></DetailReferenceValues>
  </ReferenceValueSets>
  <Locations>
    <Location ID="500"><LocationPart LocPartTypeID="1">
      <LocationPartValue Primary="true"><Value>Egypt</Value></LocationPartValue>
    </LocationPart></Location>
    <Location ID="600">
      <LocationCountry CountryID="11" CountryRelevanceID="1413"/>
      {_loc_part(1451, "1 Main Street")}
      {_loc_part(1454, "Moscow")}
    </Location>
  </Locations>
  <IDRegDocuments>
    {_doc(1, 1636, 101, "GG-1")}
    {_doc(2, 1636, 101, "GG-2")}
    {_doc(3, 1636, 101, "GG-1")}
    {_doc(4, 91508, 101, "0001 0002")}
    {_doc(5, 1571, 101, "P123", country_id=12)}
    {_doc(6, 92772, 201, "304500116000157", country_id=11)}
    {_doc(7, 92883, 201, "E-1", country_id=11)}
  </IDRegDocuments>
  <DistinctParties>
    <DistinctParty FixedRef="1001"><Profile ID="1001" PartySubTypeID="4">
      <Identity ID="101" Primary="true">{_alias("ZUMAR ABOUD")}</Identity>
      <Feature ID="1" FeatureTypeID="8"><FeatureVersion ID="1" ReliabilityID="1">
        <DatePeriod CalendarTypeID="1">
          <Start Approximate="false"><From><Year>1938</Year><Month>1</Month><Day>1</Day></From>
            <To><Year>1938</Year><Month>1</Month><Day>1</Day></To></Start>
          <End Approximate="false"><From><Year>1938</Year><Month>12</Month><Day>31</Day></From>
            <To><Year>1938</Year><Month>12</Month><Day>31</Day></To></End>
        </DatePeriod>
      </FeatureVersion></Feature>
      {_feature(3, 8, _detail("10 Dec 1948"))}
      {_feature(4, 8, _MULTI_YEAR_PERIOD)}
      {_feature(2, 10, '<VersionLocation LocationID="500"/>')}
    </Profile></DistinctParty>
    <DistinctParty FixedRef="2001"><Profile ID="2001" PartySubTypeID="3">
      <Identity ID="201" Primary="true">{_alias("CIMEX, S.A.")}
        <Alias FixedRef="2" AliasTypeID="1401" Primary="false"><DocumentedName ID="2">
          <DocumentedNamePart><NamePartValue>CIMEX OLD</NamePartValue></DocumentedNamePart></DocumentedName></Alias>
        <Alias FixedRef="3" AliasTypeID="77" Primary="false"><DocumentedName ID="3">
          <DocumentedNamePart><NamePartValue>CIMEX ODD</NamePartValue></DocumentedNamePart></DocumentedName></Alias>
        <Alias FixedRef="4" Primary="false"><DocumentedName ID="4">
          <DocumentedNamePart><NamePartValue>CIMEX BARE</NamePartValue></DocumentedNamePart>
          <DocumentedNamePart><NamePartValue> </NamePartValue></DocumentedNamePart></DocumentedName></Alias>
      </Identity>
      {_feature(10, 25, '<VersionLocation LocationID="600"/>', reliability=2)}
      {_feature(11, 21, _detail("INFO@CIMEX.EXAMPLE"))}
      {_feature(12, 504, '<VersionDetail DetailTypeID="1431" DetailReferenceID="92084"/>')}
      {_feature(13, 99001, _detail("note-77"))}
      {_feature(14, 99002, "")}
      {_feature(15, 646, "<DatePart><Year>1991</Year><Month>11</Month><Day>19</Day></DatePart>")}
      {_feature(16, "x", "")}
    </Profile></DistinctParty>
    <DistinctParty FixedRef="3001"><Profile ID="3001" PartySubTypeID="9">
      <Identity ID="301" Primary="true">{_alias("UNKNOWN KIND")}</Identity>
    </Profile></DistinctParty>
  </DistinctParties>
  <ProfileRelationships>
    <ProfileRelationship ID="1" From-ProfileID="1001" To-ProfileID="2001" RelationTypeID="15003" Former="false"/>
    <ProfileRelationship ID="2" From-ProfileID="2001" To-ProfileID="1001" RelationTypeID="99" Former="true"/>
    <ProfileRelationship ID="3" From-ProfileID="2001" To-ProfileID="9999" RelationTypeID="15003"/>
  </ProfileRelationships>
  <SanctionsEntries>
    <SanctionsEntry ID="1" ProfileID="1001" ListID="1550">
      <EntryEvent ID="1"><Date><Year>2020</Year><Month>1</Month><Day>2</Day></Date></EntryEvent>
      <SanctionsMeasure ID="1" SanctionsTypeID="1"><Comment>SDGT</Comment></SanctionsMeasure>
      <SanctionsMeasure ID="2" SanctionsTypeID="2"><Comment/></SanctionsMeasure>
    </SanctionsEntry>
  </SanctionsEntries>
</Sanctions>
"""


@pytest.fixture(name="records")
def _records(tmp_path):
    """Load the fixture and transform it; returns {RECORD_ID: record}."""
    xml_path = tmp_path / "sdn_advanced.xml"
    xml_path.write_text(FIXTURE, encoding="utf-8")
    out = tmp_path / "out.jsonl"
    transformer = StrictOFACTransformer()
    transformer.load(xml_path)
    stats = transformer.transform(output_jsonl=out)
    assert stats["processed"] == stats["emitted"] == 3
    return {r["RECORD_ID"]: r for r in map(json.loads, out.read_text(encoding="utf-8").splitlines())}


def _features_with(record, key):
    return [f for f in record["FEATURES"] if key in f]


def test_multi_value_payload_keeps_every_distinct_value(records):
    """Three Government Gazette documents (two distinct numbers) -> one payload value holding both."""
    person = records["1001"]
    assert person["GOVERNMENT_GAZETTE_NUMBER"] == "GG-1; GG-2"
    assert not _features_with(person, "GOVERNMENT_GAZETTE_NUMBER")


def test_payload_matched_by_feed_name(records):
    """Feed type 91508 is not a mapping key; it is matched by name to 1508 and still routed to payload."""
    person = records["1001"]
    assert person["CHINESE_COMMERCIAL_CODE"] == "0001 0002"
    assert not _features_with(person, "CHINESE_COMMERCIAL_CODE")


def test_identifier_documents_are_features(records):
    """A passport is an identifier feature with its issuing country."""
    assert _features_with(records["1001"], "PASSPORT_NUMBER") == [{"PASSPORT_NUMBER": "P123", "PASSPORT_COUNTRY": "EG"}]


def test_ogrnip_emits_registration_number_and_subtype(records):
    """OGRNIP (feed 92772, matched by name) -> NATIONAL_ID REGISTRATION_NUMBER + NATIONAL_ID_SUBTYPE=OGRN."""
    (feature,) = _features_with(records["2001"], "NATIONAL_ID_NUMBER")
    assert feature == {
        "NATIONAL_ID_TYPE": "REGISTRATION_NUMBER",
        "NATIONAL_ID_SUBTYPE": "OGRN",
        "NATIONAL_ID_NUMBER": "304500116000157",
        "NATIONAL_ID_COUNTRY": "RU",
    }


def test_unmapped_document_type_falls_back_to_other_id(records):
    """A document type with no mapping by ID or name becomes OTHER_ID typed by its sanitized feed label."""
    assert {"OTHER_ID_TYPE": "ENTITY_CODE", "OTHER_ID_NUMBER": "E-1", "OTHER_ID_COUNTRY": "RU"} in records["2001"][
        "FEATURES"
    ]


def test_names_follow_record_type(records):
    """Person -> NAME_FULL, organization -> NAME_ORG, unrecognized subtype -> NAME_FULL."""
    assert _features_with(records["1001"], "NAME_FULL")[0]["NAME_FULL"] == "ZUMAR ABOUD"
    assert _features_with(records["2001"], "NAME_ORG")[0]["NAME_ORG"] == "CIMEX, S.A."
    unknown = records["3001"]
    assert not _features_with(unknown, "RECORD_TYPE") and not _features_with(unknown, "NAME_ORG")
    assert _features_with(unknown, "NAME_FULL")[0]["NAME_FULL"] == "UNKNOWN KIND"


def test_dates_nationality_relationships_and_sanctions(records):
    """Year-only birthdate, nationality text, relationship pointer and sanctions payload."""
    person = records["1001"]
    assert {"DATE_OF_BIRTH": "1938"} in person["FEATURES"]  # Jan 1 .. Dec 31 period -> year only
    assert _features_with(person, "NATIONALITY") == [{"NATIONALITY": "Egypt"}]
    assert _features_with(person, "REL_POINTER_KEY") == [
        {"REL_POINTER_DOMAIN": "OFAC_ADVANCED", "REL_POINTER_KEY": "2001", "REL_POINTER_ROLE": "CONTROLLED_BY"}
    ]
    assert person["SANCTIONS_LIST"] == "SDN"
    assert person["SANCTIONS_PROGRAMS"] == "SDGT"
    assert person["SANCTIONS_TYPE"] == "Block"
    assert person["SANCTIONS_DATE"] == "2020-01-02"
    assert person["SANCTIONS_REMARKS"] == "Linked To: CIMEX, S.A."
    assert person["DATA_QUALITY_SCORE"] == 1


def test_payload_routing_ignores_group_label(monkeypatch, tmp_path):
    """Routing is the "payload" key: relabelling a group (either way) does not change where a value goes."""
    gazette = dict(ofac_advanced_mapper.ID_DOC_MAPPINGS[1636], group="OTHER IDENTIFIERS")
    passport = dict(ofac_advanced_mapper.ID_DOC_MAPPINGS[1571], group="PAYLOAD (mislabelled)")
    monkeypatch.setitem(ofac_advanced_mapper.ID_DOC_MAPPINGS, 1636, gazette)
    monkeypatch.setitem(ofac_advanced_mapper.ID_DOC_MAPPINGS, 1571, passport)
    xml_path = tmp_path / "sdn_advanced.xml"
    xml_path.write_text(FIXTURE, encoding="utf-8")
    transformer = StrictOFACTransformer()
    transformer.load(xml_path)
    record = {"FEATURES": []}
    party = transformer.profile_elements["1001"]
    transformer._add_identity_documents(record, party)
    assert record["GOVERNMENT_GAZETTE_NUMBER"] == "GG-1; GG-2"
    assert {"PASSPORT_NUMBER": "P123", "PASSPORT_COUNTRY": "EG"} in record["FEATURES"]
    assert not any("GOVERNMENT_GAZETTE_NUMBER" in f for f in record["FEATURES"])


def test_append_payload_value():
    """Order-preserving, de-duplicated "; " join; empty values are ignored."""
    record = {}
    for value in ("A", "", None, "B", "A", "C"):
        StrictOFACTransformer._append_payload_value(record, "K", value)
    assert record == {"K": "A; B; C"}


def test_main_writes_jsonl(tmp_path):
    """The CLI entry point loads, transforms and writes one JSON line per party."""
    xml_path = tmp_path / "sdn_advanced.xml"
    xml_path.write_text(FIXTURE, encoding="utf-8")
    out = tmp_path / "cli.jsonl"
    assert main([str(xml_path), "--output-jsonl", str(out)]) == 0
    assert len(out.read_text(encoding="utf-8").splitlines()) == 3


def test_organization_features_payload_and_fallbacks(records):
    """Address, email, payload feature, unmapped-feature fallback, DatePart date, alias types, former role."""
    org = records["2001"]
    assert _features_with(org, "ADDR_FULL") == [{"ADDR_FULL": "1 Main Street, Moscow"}]
    assert _features_with(org, "EMAIL_ADDRESS") == [{"EMAIL_ADDRESS": "info@cimex.example"}]
    assert org["SECONDARY_SANCTIONS_RISK"] == "Yes"
    assert _features_with(org, "OTHER_ID_TYPE") == [
        {"OTHER_ID_TYPE": "WEBSITE_NOTE", "OTHER_ID_NUMBER": "note-77"},
        {"OTHER_ID_TYPE": "ENTITY_CODE", "OTHER_ID_NUMBER": "E-1", "OTHER_ID_COUNTRY": "RU"},
    ]
    assert _features_with(org, "REGISTRATION_DATE") == [{"REGISTRATION_DATE": "1991-11-19"}]
    names = {f["NAME_ORG"]: f["NAME_TYPE"] for f in _features_with(org, "NAME_ORG")}
    assert names == {"CIMEX, S.A.": "PRIMARY", "CIMEX OLD": "FKA", "CIMEX ODD": "AKA", "CIMEX BARE": "AKA"}
    assert _features_with(org, "REL_POINTER_ROLE") == [
        {
            "REL_POINTER_DOMAIN": "OFAC_ADVANCED",
            "REL_POINTER_KEY": "1001",
            "REL_POINTER_ROLE": "FORMER_PROVIDING_SHELTER_TO",
        }
    ]
    assert org["DATA_QUALITY_SCORE"] == 2


def test_birthdate_from_text(records):
    """Text date normalized to ISO; a 1961-1963 span is three separate DATE_OF_BIRTH features."""
    values = [f["DATE_OF_BIRTH"] for f in _features_with(records["1001"], "DATE_OF_BIRTH")]
    assert values == ["1938", "1948-12-10", "1961", "1962", "1963"]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1948-12-10", "1948-12-10"),
        ("10 Dec 1948", "1948-12-10"),
        ("10 December 1948", "1948-12-10"),
        ("1948/12/10", "1948-12-10"),
        ("12/10/1948", "1948-12-10"),
        ("1948.12.10", "1948-12-10"),
        ("1948", "1948"),
        ("1948-12", "1948-12"),
        ("1948-12-10T00:00", "1948-12-10"),
        ("circa 1948", None),
        ("  ", None),
    ],
)
def test_normalize_date_string(text, expected):
    """Free-text dates in the formats the feed has used, and rejection of anything else."""
    assert StrictOFACTransformer._normalize_date_string(text) == expected


def test_transform_requires_load(tmp_path):
    """transform() before load() is an error, not an empty file."""
    with pytest.raises(RuntimeError):
        StrictOFACTransformer().transform(output_jsonl=tmp_path / "x.jsonl")

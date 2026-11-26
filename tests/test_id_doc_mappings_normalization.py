"""Invariants for the identifier-type normalization of ID_DOC_MAPPINGS.

What the mapping emits (these tests assert each point):

* The business-registration family collapses to ``NATIONAL_ID_TYPE=REGISTRATION_NUMBER``.
* OGRNIP is ``NATIONAL_ID_TYPE=REGISTRATION_NUMBER`` plus ``NATIONAL_ID_SUBTYPE=OGRNIP`` (the subtype is
  registered by src/ofac_config_updates.gtc; see the note on entry 2772).
* Named person schemes carry an upper-case scheme token (``CURP``, ``DNI``, ``CEDULA``, ...); generic
  "national id" labels carry no ``NATIONAL_ID_TYPE`` (the feature already declares the class).
* Tax identifiers live on ``TAX_ID``: a generic tax number has no ``TAX_ID_TYPE``; VAT is
  ``TAX_ID_TYPE=VAT``; CUIT is ``TAX_ID_TYPE=CUIT`` (AR); a Russian "Tax ID No." is ``TAX_ID_TYPE=INN``.
* LEIs route to the ``LEI_NUMBER`` feature.
* IMO and MMSI route to the dedicated ``IMO_NUMBER`` / ``MMSI_NUMBER`` features.
* Many-per-entity or reissued documents (licenses, permits, electoral, ...) stay on the non-exclusive
  ``OTHER_ID`` feature with a canonical crosswalk token.
* Chinese Commercial Code, Government Gazette Number, Serial No., Trademark number, Branch Unit Number and
  Entity Code are marked ``"payload": True`` (not identifiers under a known scheme).
* ``group`` is descriptive only and must agree with where the instructions route.
"""

import importlib.util
import pathlib

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "config" / "id_doc_mappings.py"
_spec = importlib.util.spec_from_file_location("id_doc_mappings", _SRC)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
ID_DOC_MAPPINGS = _mod.ID_DOC_MAPPINGS


def _instr(id_):
    return dict(ID_DOC_MAPPINGS[id_]["instructions"])


def test_registration_family_collapses_to_registration_number():
    # Business-registration labels (incl. Registered Charity No. 1588 and C.I.N. 1594 folded in from
    # OTHER_ID) all land on the exclusive NATIONAL_ID feature with one canonical type.
    for id_ in (
        1412,
        1475,
        1581,
        1585,
        1602,
        1603,
        1614,
        1619,
        1620,
        1629,
        1631,
        1642,
        1643,
        1644,
        1720,
        1721,
        1747,
        1751,
        1753,
        1760,
        1761,
        2001,
        2067,
        2121,
        1588,
        1594,
        1835,  # File Number: a US state (e.g. Delaware) / Czech company file number is its registration
    ):
        instr = _instr(id_)
        assert instr.get("NATIONAL_ID_TYPE") == "REGISTRATION_NUMBER", id_
        assert "NATIONAL_ID_NUMBER" in instr, id_


def test_ogrnip_carries_ogrnip_subtype():
    # OGRNIP is one of several RU registration registries and a source may only know the broad
    # registration number, so it is TYPE=REGISTRATION_NUMBER + SUBTYPE=OGRNIP (NATIONAL_ID_SUBTYPE,
    # Senzing GDEV-4439) rather than a distinct top-level type that would fragment the namespace.
    instr = _instr(2772)
    assert instr.get("NATIONAL_ID_TYPE") == "REGISTRATION_NUMBER"
    assert instr.get("NATIONAL_ID_SUBTYPE") == "OGRNIP"
    assert instr.get("NATIONAL_ID_COUNTRY") == "RU"


def test_feature_class_unified_no_other_id_registration_leftovers():
    for id_, entry in ID_DOC_MAPPINGS.items():
        instr = dict(entry["instructions"])
        if instr.get("OTHER_ID_TYPE") == "REGISTRATION_NUMBER":
            raise AssertionError(f"{id_} left REGISTRATION_NUMBER on OTHER_ID")


def test_person_schemes_canonical_uppercase():
    # Named person/national schemes carry the UPPER-CASE canonical token (crosswalk normalization:
    # upper-case, trim punctuation) so every source shares one exclusivity namespace per scheme.
    named = {
        1600: "CURP",
        1574: "DNI",
        1570: "CEDULA",
        1625: "CUIP",
        1854: "CUI",
        1579: "NIE",
        1634: "CNP",
        1492: "TAZKIRA",
        1645: "TCKN",  # Turkish Identification Number -- a named scheme, not generic
        1740: "EID",  # UAE Identification (Emirates ID)
        1739: "CCN",  # Citizen's Card Number
    }
    for id_, typ in named.items():
        assert _instr(id_).get("NATIONAL_ID_TYPE") == typ, id_
    # Only a TRULY generic label (Numero de Identidad = "identity number" in Spanish, no distinct
    # scheme) carries NO type -- the NATIONAL_ID feature + country scope it and a blank type bridges.
    assert "NATIONAL_ID_TYPE" not in _instr(1482)


def test_generic_national_id_labels_stay_untyped():
    # National ID No. / Identification Number / Personal ID Card / Federal ID Card / Kenyan ID No. /
    # (Bosnian|Moroccan) Personal ID No. / National Foreign ID Number: the label only restates the
    # feature, so no NATIONAL_ID_TYPE (an explicit type would falsely conflict with specific schemes).
    for id_ in (1584, 1591, 1608, 1627, 1628, 1587, 1597, 1611):
        instr = _instr(id_)
        assert "NATIONAL_ID_TYPE" not in instr, id_
        assert "NATIONAL_ID_NUMBER" in instr, id_


def test_no_generic_idnumber_type_anywhere():
    # The generic FtM idNumber property would merge distinct person schemes -> never emit it as a type.
    for id_, entry in ID_DOC_MAPPINGS.items():
        for attr, value in entry["instructions"]:
            if attr.endswith("_TYPE"):
                assert value != "idNumber", id_


def test_legal_entity_numbers_route_to_lei_feature():
    # "Legal Entity Number" (1752) and "LE Number" (1586) are LEIs -> the self-typed global feature.
    for id_ in (1752, 1586):
        instr = _instr(id_)
        assert "LEI_NUMBER" in instr, id_
        assert "NATIONAL_ID_NUMBER" not in instr, id_


def test_tax_schemes_route_to_tax_id_blank_type():
    # Generic national tax numbers -> TAX_ID with a BLANK TAX_ID_TYPE (the un-scheme-specific
    # default), so a bare tax number bridges cross-source instead of conflicting on a token. Named
    # tax schemes (VAT -> TAX_ID_TYPE=VAT) keep their own type. Entity Spec puts EIN/TIN/VAT on TAX_ID.
    expected = (
        1573,
        1612,
        1478,
        1580,
        1481,
        1578,
        1582,
        1576,
        1592,
        1596,
        1609,
        1638,
        1646,
        1575,
        1633,
    )
    for id_ in expected:
        instr = _instr(id_)
        assert "TAX_ID_TYPE" not in instr, id_
        assert "TAX_ID_NUMBER" in instr, id_


def test_cuit_is_tax_id_type_cuit_argentina():
    # CUIT (Clave Unica de Identificacion Tributaria) is Argentina's tax id: the crosswalk vocabulary puts it
    # on TAX_ID with TAX_ID_TYPE=CUIT, country AR.
    assert _instr(1595) == {"TAX_ID_TYPE": "CUIT", "TAX_ID_NUMBER": None, "TAX_ID_COUNTRY": "AR"}


def test_russian_tax_id_no_is_inn_by_issuer():
    # The generic "Tax ID No." label names a scheme only together with its issuer: Russian -> INN.
    assert ID_DOC_MAPPINGS[1596]["type_by_country"] == {"RU": "INN"}
    for id_, entry in ID_DOC_MAPPINGS.items():
        for token in entry.get("type_by_country", {}).values():
            assert token == token.upper(), id_


def test_vessel_ids_route_to_dedicated_features():
    assert _instr(1626) == {"IMO_NUMBER": None}
    assert _instr(1264) == {"MMSI_NUMBER": None}


def test_vat_routes_to_tax_id_type_vat():
    # VAT is a tax scheme -> TAX_ID with a DISTINCT type (VAT) that keeps it out of the
    # national-tax-number namespace (both live on TAX_ID, different TAX_ID_TYPE).
    instr = _instr(1589)
    assert instr.get("TAX_ID_TYPE") == "VAT"
    assert "TAX_ID_NUMBER" in instr
    assert "NATIONAL_ID_NUMBER" not in instr


def test_shared_or_many_per_entity_stay_non_exclusive_other_id():
    # Canonical crosswalk tokens (LICENSE, PERMIT, ...), never a per-source spelling (licenseNumber, ...).
    expected = {
        1236: "LICENSE",
        1504: "LICENSE",
        1615: "LICENSE",
        1621: "LICENSE",
        1639: "LICENSE",
        1812: "LICENSE",
        2159: "PERMIT",
        1484: "PERMIT",
        1590: "ELECTORAL",
        1607: "ELECTORAL",
        1712: "ELECTORAL",
        2728: "MILITARY_SERVICE",
        1635: "RESIDENCY",
        1598: "IMMIGRATION",
        1623: "AIRCRAFT_MSN",
        2790: "GIIN",
        2897: "SCAC",
        2900: "CAAT",
    }
    for id_, typ in expected.items():
        assert _instr(id_).get("OTHER_ID_TYPE") == typ, id_


def test_other_id_tokens_are_upper_case():
    for id_, entry in ID_DOC_MAPPINGS.items():
        token = dict(entry["instructions"]).get("OTHER_ID_TYPE")
        if token is not None:
            assert token == token.upper(), id_


def test_non_identifier_codes_route_to_payload():
    # Chinese Commercial Code (a name telegraph-encoding, shared by same-named entities) and Government
    # Gazette Number (a shared publication reference) are not entity identifiers -> the explicit
    # "payload" key, a single non-feature attribute, so the mapper keeps them off the FEATURES path.
    for id_, attr in (
        (1508, "CHINESE_COMMERCIAL_CODE"),
        (1636, "GOVERNMENT_GAZETTE_NUMBER"),
        (1593, "SERIAL_NUMBER"),
        (2158, "TRADEMARK_NUMBER"),
        (1719, "BRANCH_UNIT_NUMBER"),
        (2883, "ENTITY_CODE"),
    ):
        entry = ID_DOC_MAPPINGS[id_]
        assert entry.get("payload") is True, id_
        instr = entry["instructions"]
        assert instr == [(attr, None)], id_
        assert not any(a.startswith(("NATIONAL_ID", "TAX_ID", "OTHER_ID")) for a, _ in instr), id_


def test_payload_key_only_on_non_identifiers():
    # "payload" is the only routing switch: it is a bool, set on exactly the non-identifier types.
    payload_ids = {id_ for id_, entry in ID_DOC_MAPPINGS.items() if entry.get("payload") is True}
    assert payload_ids == {1508, 1636, 1593, 2158, 1719, 2883}
    for id_, entry in ID_DOC_MAPPINGS.items():
        assert entry.get("payload", False) in (True, False), id_


# Where an entry's instructions route -> its descriptive group label.
_GROUP_BY_ROUTE = {
    "SPECIFIC": "SPECIFIC SENZING FEATURES (Direct Mappings)",
    "TAX_ID": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
    "REGISTRATION": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
    "NATIONAL_ID": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
    "OTHER_ID": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
}


def _route(entry):
    attrs = dict(entry["instructions"])
    if entry.get("payload") is True:
        return "PAYLOAD"
    if any(a.startswith(("PASSPORT_", "SSN_", "DRIVERS_LICENSE_", "LEI_", "IMO_", "MMSI_")) for a in attrs):
        return "SPECIFIC"
    if any(a.startswith("TAX_ID_") for a in attrs):
        return "TAX_ID"
    if any(a.startswith("NATIONAL_ID_") for a in attrs):
        return "REGISTRATION" if attrs.get("NATIONAL_ID_TYPE") == "REGISTRATION_NUMBER" else "NATIONAL_ID"
    if any(a.startswith("OTHER_ID_") for a in attrs):
        return "OTHER_ID"
    raise AssertionError(f"no route for {attrs}")


def test_group_label_matches_routing():
    # The label is documentation; a stale one (e.g. a tax id still labelled ACCOUNT/FINANCIAL) fails here.
    for id_, entry in ID_DOC_MAPPINGS.items():
        route = _route(entry)
        if route == "PAYLOAD":
            assert entry["group"].upper().startswith("PAYLOAD"), id_
        else:
            assert entry["group"] == _GROUP_BY_ROUTE[route], (id_, entry["group"], route)


def test_uk_country_normalized_to_gb():
    for id_ in (1601, 1603):  # BNO passport, UK Company Number
        vals = [v for _, v in ID_DOC_MAPPINGS[id_]["instructions"]]
        assert "UK" not in vals and "GB" in vals, id_


def test_all_instructions_well_formed():
    for id_, entry in ID_DOC_MAPPINGS.items():
        for pair in entry["instructions"]:
            assert isinstance(pair, tuple) and len(pair) == 2, id_
            assert isinstance(pair[0], str), id_

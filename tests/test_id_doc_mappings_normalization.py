"""Invariants for the OpenSanctions-aligned id-type normalization of ID_DOC_MAPPINGS.

Design (aligned to the OpenSanctions ``doc_types`` crosswalk = the canonical FtM vocabulary):

* The business-registration family collapses to one canonical ``NATIONAL_ID_TYPE=registrationNumber``.
* OGRNIP carries its distinct ``ogrnCode`` type (label-driven, matching OpenSanctions ``us_ofac``).
* Specific person schemes carry the OpenSanctions RAW label (e.g. ``C.U.R.P.``) so OFAC and
  OpenSanctions share one exclusivity namespace; generic "national id" labels stay UNTYPED (the
  NATIONAL_ID feature already declares the class).
* ALL tax identifiers live on NATIONAL_ID, each typed with its scheme label. ``TAX_ID`` is UNTYPED
  in the config today (no ``TAX_ID_TYPE`` element) so it can hold only one primary scheme per country
  and cannot distinguish schemes; NATIONAL_ID_TYPE works today and keeps each scheme namespaced.
  VAT -> ``vatCode``. Nothing is emitted on the TAX_ID feature until ``TAX_ID_TYPE`` ships.
* LEIs route to the self-typed global ``LEI_NUMBER`` feature.
* Many-per-entity or reissued documents (licenses/permits/gazette/trademark/serial) stay on the
  non-exclusive ``OTHER_ID`` feature (would otherwise manufacture F1ES denials).

The repo shipped no tests, so this is the regression net for the normalization.
"""

import importlib.util
import pathlib

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "config" / "id_doc_mappings.py"
_spec = importlib.util.spec_from_file_location("id_doc_mappings", _SRC)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
ID_DOC_MAPPINGS = _mod.ID_DOC_MAPPINGS


def _instr(id_):
    return dict((a, v) for a, v in ID_DOC_MAPPINGS[id_]["instructions"])


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
    ):
        instr = _instr(id_)
        assert instr.get("NATIONAL_ID_TYPE") == "registrationNumber", id_
        assert "NATIONAL_ID_NUMBER" in instr, id_


def test_ogrnip_carries_ogrn_code():
    # OGRNIP is one of several RU registration registries and a source may only know the broad
    # registrationNumber, so it is TYPE=registrationNumber + SUBTYPE=ogrnCode (NATIONAL_ID_SUBTYPE,
    # Senzing GDEV-4439) rather than a distinct top-level type that would fragment the namespace.
    instr = _instr(2772)
    assert instr.get("NATIONAL_ID_TYPE") == "registrationNumber"
    assert instr.get("NATIONAL_ID_SUBTYPE") == "ogrnCode"


def test_feature_class_unified_no_other_id_registration_leftovers():
    for id_, entry in ID_DOC_MAPPINGS.items():
        instr = dict(entry["instructions"])
        if instr.get("OTHER_ID_TYPE") == "registrationNumber":
            raise AssertionError(f"{id_} left registrationNumber on OTHER_ID")


def test_person_schemes_use_openSanctions_raw_labels():
    expected = {
        1600: "C.U.R.P.",
        1574: "D.N.I.",
        1570: "Cedula No.",
        1625: "C.U.I.P.",
        1854: "C.U.I.",
        1579: "N.I.E.",
        1634: "CNP (Personal Numerical Code)",
        1645: "Turkish Identification Number",
        1740: "UAE Identification",
        1739: "Citizen's Card Number",
        1492: "Tazkira National ID Card",
        1482: "Numero de Identidad",
    }
    for id_, typ in expected.items():
        assert _instr(id_).get("NATIONAL_ID_TYPE") == typ, id_


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
    # tax schemes (e.g. VAT -> vatCode) keep their own type. Entity Spec puts EIN/TIN/VAT on TAX_ID.
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


def test_cuit_is_national_id_not_tax():
    # CUIT is Argentina's universal identifier (used well beyond tax); OpenSanctions AND Sayari both
    # classify it NATIONAL_ID, not TAX_ID.
    assert _instr(1595).get("NATIONAL_ID_TYPE") == "C.U.I.T."


def test_vat_routes_to_tax_id_vatcode():
    # VAT is a tax scheme -> TAX_ID with a DISTINCT type (vatCode) that keeps it out of the
    # national-tax-number namespace (both live on TAX_ID, different TAX_ID_TYPE).
    instr = _instr(1589)
    assert instr.get("TAX_ID_TYPE") == "vatCode"
    assert "TAX_ID_NUMBER" in instr
    assert "NATIONAL_ID_NUMBER" not in instr


def test_shared_or_many_per_entity_stay_non_exclusive_other_id():
    expected = {
        1236: "licenseNumber",
        1504: "licenseNumber",
        1615: "licenseNumber",
        1621: "licenseNumber",
        1639: "licenseNumber",
        1812: "licenseNumber",
        2158: "trademarkNumber",
        2159: "permitNumber",
        1593: "serialNumber",
        1835: "fileNumber",
    }
    for id_, typ in expected.items():
        assert _instr(id_).get("OTHER_ID_TYPE") == typ, id_


def test_non_identifier_codes_route_to_payload():
    # Chinese Commercial Code (a name telegraph-encoding, shared by same-named entities) and Government
    # Gazette Number (a shared publication reference) are not entity identifiers -> PAYLOAD group, a
    # single non-feature attribute, so the mapper preserves them off the FEATURES/IDENTIFIER path.
    for id_, attr in ((1508, "CHINESE_COMMERCIAL_CODE"), (1636, "GOVERNMENT_GAZETTE_NUMBER")):
        entry = ID_DOC_MAPPINGS[id_]
        assert "PAYLOAD" in entry["group"].upper(), id_
        instr = entry["instructions"]
        assert instr == [(attr, None)], id_
        assert not any(a.startswith(("NATIONAL_ID", "TAX_ID", "OTHER_ID")) for a, _ in instr), id_


def test_uk_country_normalized_to_gb():
    for id_ in (1601, 1603):  # BNO passport, UK Company Number
        vals = [v for _, v in ID_DOC_MAPPINGS[id_]["instructions"]]
        assert "UK" not in vals and "GB" in vals, id_


def test_all_instructions_well_formed():
    for id_, entry in ID_DOC_MAPPINGS.items():
        for pair in entry["instructions"]:
            assert isinstance(pair, tuple) and len(pair) == 2, id_
            assert isinstance(pair[0], str), id_

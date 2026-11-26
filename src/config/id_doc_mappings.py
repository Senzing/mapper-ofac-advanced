# OFAC IDRegDocType -> Senzing identifier mapping. See docs/id-type-normalization.md for the rules and
# the full generated table.
#
# Entry keys:
#   "name"         OFAC document-type label. The mapper also matches a feed IDRegDocTypeID that is not a key
#                  here by this name (the live feed uses e.g. 91508 / 92772 for 1508 / 2772).
#   "group"        Descriptive only -- nothing routes on it. It is derived from where "instructions" route
#                  (tests/test_id_doc_mappings_normalization.py fails if a label drifts from the routing).
#   "payload"      True -> the value is not an entity identifier (a name telegraph-encoding, a shared
#                  publication reference); the mapper writes it as a top-level payload attribute, never as
#                  a FEATURES entry. Absent -> a normal identifier feature.
#   "type_by_country" Optional {issuing country: TYPE token}. When the document's issuing country is a key,
#                  the matching *_TYPE is added (e.g. a Russian "Tax ID No." is the INN). The label alone
#                  names no scheme; label + issuer does.
#   "instructions" (attribute, constant) pairs; a None constant takes the document's number/country/region.
#
# Every TYPE token is the canonical one from the Senzing identifier crosswalk vocabulary (this table is the
# OFAC-specific mapping of OFAC's labels onto it). Type values: the business-registration family (incl.
# state file numbers) -> NATIONAL_ID_TYPE=REGISTRATION_NUMBER; OGRNIP -> REGISTRATION_NUMBER +
# NATIONAL_ID_SUBTYPE=OGRNIP (registered by src/ofac_config_updates.gtc). Tax ids -> TAX_ID with no
# TAX_ID_TYPE for a generic tax number, TAX_ID_TYPE=VAT for VAT, CUIT (AR) and INN (a Russian Tax ID No.).
# LEIs -> LEI_NUMBER; IMO / MMSI -> the IMO_NUMBER / MMSI_NUMBER features (registered by the .gtc). Named
# person schemes carry an upper-case scheme token (CURP, DNI, CEDULA, ...); generic "national id" labels
# carry no NATIONAL_ID_TYPE (the feature already declares the class). Voter/electoral and many-per-entity or
# reissued documents stay on the non-exclusive OTHER_ID feature with a canonical token (LICENSE, PERMIT,
# ELECTORAL, RESIDENCY, ...). Values that do not identify the entity under a known scheme (a serial number of
# an unnamed document, a trademark, a branch unit number, OFAC's "Entity Code") are payload.
ID_DOC_MAPPINGS = {
    1236: {
        "name": "Afghan Money Service Provider License Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "LICENSE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", "AF"),
        ],
    },
    1264: {
        "name": "MMSI",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("MMSI_NUMBER", None),
        ],
    },
    1412: {
        "name": "Company Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1475: {
        "name": "Public Registration Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1478: {
        "name": "N.I.F.",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1481: {
        "name": "RTN",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1482: {
        "name": "Numero de Identidad",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1484: {
        "name": "SRE Permit No.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "PERMIT"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1492: {
        "name": "Tazkira National ID Card",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "TAZKIRA"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "AF"),
        ],
    },
    1504: {
        "name": "License",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "LICENSE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1508: {
        "name": "Chinese Commercial Code",
        "group": "PAYLOAD - Chinese commercial/telegraph code (a name encoding, not an identifier)",
        "payload": True,
        "instructions": [
            ("CHINESE_COMMERCIAL_CODE", None),
        ],
    },
    1570: {
        "name": "Cedula No.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "CEDULA"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1571: {
        "name": "Passport",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("PASSPORT_NUMBER", None),
            ("PASSPORT_COUNTRY", None),
        ],
    },
    1572: {
        "name": "SSN",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("SSN_NUMBER", None),
        ],
    },
    1573: {
        "name": "R.F.C.",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1574: {
        "name": "D.N.I.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "DNI"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1575: {
        "name": "NIT #",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1576: {
        "name": "US FEIN",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", "US"),
        ],
    },
    1577: {
        "name": "Driver's License No.",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("DRIVERS_LICENSE_NUMBER", None),
            ("DRIVERS_LICENSE_STATE", None),
        ],
    },
    1578: {
        "name": "RUC #",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1579: {
        "name": "N.I.E.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "NIE"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1580: {
        "name": "C.I.F.",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1581: {
        "name": "Business Registration Document #",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1582: {
        "name": "RIF #",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1584: {
        "name": "National ID No.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1585: {
        "name": "Registration ID",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1586: {
        "name": "LE Number",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("LEI_NUMBER", None),
        ],
    },
    1587: {
        "name": "Bosnian Personal ID No.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "BA"),
        ],
    },
    1588: {
        "name": "Registered Charity No.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1589: {
        "name": "V.A.T. Number",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_TYPE", "VAT"),
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1590: {
        "name": "Credencial electoral",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "ELECTORAL"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1591: {
        "name": "Kenyan ID No.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "KE"),
        ],
    },
    1592: {
        "name": "Italian Fiscal Code",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", "IT"),
        ],
    },
    1593: {
        "name": "Serial No.",
        "group": "PAYLOAD - serial number of an unnamed document (scheme unknown, not a typed identifier)",
        "payload": True,
        "instructions": [
            ("SERIAL_NUMBER", None),
        ],
    },
    1594: {
        "name": "C.I.N.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1595: {
        "name": "C.U.I.T.",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_TYPE", "CUIT"),
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", "AR"),
        ],
    },
    1596: {
        "name": "Tax ID No.",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "type_by_country": {"RU": "INN"},
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1597: {
        "name": "Moroccan Personal ID No.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "MA"),
        ],
    },
    1598: {
        "name": "Public Security and Immigration No.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "IMMIGRATION"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1600: {
        "name": "C.U.R.P.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "CURP"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "MX"),
        ],
    },
    1601: {
        "name": "British National Overseas Passport",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("PASSPORT_NUMBER", None),
            ("PASSPORT_COUNTRY", "GB"),
        ],
    },
    1602: {
        "name": "C.R. No.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1603: {
        "name": "UK Company Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "GB"),
        ],
    },
    1604: {
        "name": "Immigration No.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "IMMIGRATION"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1605: {
        "name": "Travel Document Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "TRAVEL_DOC"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1607: {
        "name": "Electoral Registry No.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "ELECTORAL"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1608: {
        "name": "Identification Number",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1609: {
        "name": "Paraguayan tax identification number",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", "PY"),
        ],
    },
    1611: {
        "name": "National Foreign ID Number",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1612: {
        "name": "RFC",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1613: {
        "name": "Diplomatic Passport",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("PASSPORT_NUMBER", None),
            ("PASSPORT_COUNTRY", None),
        ],
    },
    1614: {
        "name": "Dubai Chamber of Commerce Membership No.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "AE"),
        ],
    },
    1615: {
        "name": "Trade License No.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "LICENSE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1619: {
        "name": "Commercial Registry Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1620: {
        "name": "Certificate of Incorporation Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1621: {
        "name": "Tourism License No.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "LICENSE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1623: {
        "name": "Aircraft Serial Identification",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "AIRCRAFT_MSN"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1624: {
        "name": "Cartilla de Servicio Militar Nacional",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "MILITARY_SERVICE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1625: {
        "name": "C.U.I.P.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "CUIP"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1626: {
        "name": "Vessel Registration Identification",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("IMO_NUMBER", None),
        ],
    },
    1627: {
        "name": "Personal ID Card",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1628: {
        "name": "Federal ID Card",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1629: {
        "name": "Registration Certificate Number (Dubai)",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "AE"),
        ],
    },
    1630: {
        "name": "VisaNumberID",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "VISA"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1631: {
        "name": "Matricula Mercantil No",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1632: {
        "name": "Residency Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "RESIDENCY"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1633: {
        "name": "Numero Unico de Identificacao Tributaria (NUIT)",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1634: {
        "name": "CNP (Personal Numerical Code)",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "CNP"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1635: {
        "name": "Romanian Permanent Resident",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "RESIDENCY"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", "RO"),
        ],
    },
    1636: {
        "name": "Government Gazette Number",
        "group": "PAYLOAD - official gazette publication reference (shared, not an identifier)",
        "payload": True,
        "instructions": [
            ("GOVERNMENT_GAZETTE_NUMBER", None),
        ],
    },
    1638: {
        "name": "Fiscal Code",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", None),
        ],
    },
    1639: {
        "name": "Pilot License Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "LICENSE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1642: {
        "name": "Romanian C.R.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "RO"),
        ],
    },
    1643: {
        "name": "Folio Mercantil No.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1644: {
        "name": "Istanbul Chamber of Comm. No.",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "TR"),
        ],
    },
    1645: {
        "name": "Turkish Identification Number",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "TCKN"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "TR"),
        ],
    },
    1646: {
        "name": "Romanian Tax Registration",
        "group": "TAX IDENTIFIERS (Classification per Spec Decision Tree)",
        "instructions": [
            ("TAX_ID_NUMBER", None),
            ("TAX_ID_COUNTRY", "RO"),
        ],
    },
    1647: {
        "name": "Stateless Person Passport",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("PASSPORT_NUMBER", None),
            ("PASSPORT_COUNTRY", None),
        ],
    },
    1648: {
        "name": "Stateless Person ID Card",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "STATELESS_ID"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1649: {
        "name": "Refugee ID Card",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "REFUGEE_ID"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1712: {
        "name": "I.F.E.",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "ELECTORAL"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1719: {
        "name": "Branch Unit Number",
        "group": "PAYLOAD - branch/establishment unit number (identifies a branch, not the entity)",
        "payload": True,
        "instructions": [
            ("BRANCH_UNIT_NUMBER", None),
        ],
    },
    1720: {
        "name": "Enterprise Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1721: {
        "name": "Organization Code",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1739: {
        "name": "Citizen's Card Number",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "CCN"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1740: {
        "name": "UAE Identification",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "EID"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "AE"),
        ],
    },
    1747: {
        "name": "United Social Credit Code Certificate (USCCC)",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "CN"),
        ],
    },
    1751: {
        "name": "Chamber of Commerce Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1752: {
        "name": "Legal Entity Number",
        "group": "SPECIFIC SENZING FEATURES (Direct Mappings)",
        "instructions": [
            ("LEI_NUMBER", None),
        ],
    },
    1753: {
        "name": "Business Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1759: {
        "name": "Birth Certificate Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "BIRTH_CERT"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1760: {
        "name": "Business Registration Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1761: {
        "name": "Registration Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1812: {
        "name": "MSB Registration Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "LICENSE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1835: {
        "name": "File Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1854: {
        "name": "C.U.I.",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "CUI"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    1891: {
        "name": "Seafarer's Identification Document",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "SEAFARER_ID"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    2001: {
        "name": "Unified Social Credit Code (USCC)",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "CN"),
        ],
    },
    2067: {
        "name": "Central Registration System Number",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    2121: {
        "name": "Economic Register Number (CBLS)",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    2158: {
        "name": "Trademark number",
        "group": "PAYLOAD - trademark registration (identifies a mark, not the entity)",
        "payload": True,
        "instructions": [
            ("TRADEMARK_NUMBER", None),
        ],
    },
    2159: {
        "name": "Permit Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "PERMIT"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    2728: {
        "name": "Military Registration Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "MILITARY_SERVICE"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    # OGRNIP (feed IDRegDocTypeID 92772, matched by name): the individual-entrepreneur sub-registry of the
    # Russian registration number. NATIONAL_ID_SUBTYPE is not in the default Senzing config; it is registered
    # by src/ofac_config_updates.gtc (without it the value is ignored at load).
    2772: {
        "name": "Russian State Individual Business Registration Number Pattern (OGRNIP)",
        "group": "BUSINESS/ORGANIZATION REGISTRATIONS (National-level)",
        "instructions": [
            ("NATIONAL_ID_TYPE", "REGISTRATION_NUMBER"),
            ("NATIONAL_ID_SUBTYPE", "OGRNIP"),
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", "RU"),
        ],
    },
    2790: {
        "name": "Global Intermediary Identification Number",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "GIIN"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", None),
        ],
    },
    1483: {
        "name": "Tarjeta de Identidad",
        "group": "NATIONAL IDENTIFIERS (Country-issued unique per person/org)",
        "instructions": [
            ("NATIONAL_ID_NUMBER", None),
            ("NATIONAL_ID_COUNTRY", None),
        ],
    },
    2883: {
        "name": "Entity Code",
        "group": "PAYLOAD - entity code of unknown issuing scheme (pending a crosswalk entry)",
        "payload": True,
        "instructions": [
            ("ENTITY_CODE", None),
        ],
    },
    2897: {
        "name": "Standard Carrier Alpha Code (SCAC)",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "SCAC"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", "US"),
        ],
    },
    2900: {
        "name": "Codigo Alfanumerico Armonizado del Transportista (CAAT)",
        "group": "OTHER IDENTIFIERS (Specialized/Unknown codes)",
        "instructions": [
            ("OTHER_ID_TYPE", "CAAT"),
            ("OTHER_ID_NUMBER", None),
            ("OTHER_ID_COUNTRY", "MX"),
        ],
    },
}

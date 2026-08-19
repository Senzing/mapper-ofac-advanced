# FtM-canonical identifier-type normalization aligned to the OpenSanctions `doc_types` crosswalk
# (cross-checked against the reviewed Senzing mapper-sayari-spark classifications). Business
# registration family -> NATIONAL_ID/registrationNumber; OGRNIP -> registrationNumber +
# NATIONAL_ID_SUBTYPE=ogrnCode (one of several RU registration registries; Senzing GDEV-4439).
# Tax ids -> TAX_ID: blank TAX_ID_TYPE for the generic number, own type for named schemes
# (VAT -> vatCode). LEI -> dedicated LEI feature; specific person
# schemes carry the OpenSanctions RAW label (e.g. "C.U.R.P.") so all sources share one exclusivity
# namespace; generic "national id" labels are left UNTYPED (the NATIONAL_ID feature already declares
# the class); voter/electoral and many-per-entity or reissued documents (licenses/permits/gazette/
# trademark/serial) stay on non-exclusive OTHER_ID.
ID_DOC_MAPPINGS = {
    1236: {
        "name": 'Afghan Money Service Provider License Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'licenseNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', 'AF'),
        ],
    },
    1264: {
        "name": 'MMSI',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'MMSI'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1412: {
        "name": 'Company Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1475: {
        "name": 'Public Registration Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1478: {
        "name": 'N.I.F.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1481: {
        "name": 'RTN',
        "group": 'ACCOUNT/FINANCIAL IDENTIFIERS',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1482: {
        "name": 'Numero de Identidad',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'Numero de Identidad'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1484: {
        "name": 'SRE Permit No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'SRE_PERMIT'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1492: {
        "name": 'Tazkira National ID Card',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'Tazkira National ID Card'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'AF'),
        ],
    },
    1504: {
        "name": 'License',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'licenseNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1508: {
        "name": 'Chinese Commercial Code',
        "group": 'PAYLOAD - Chinese commercial/telegraph code (a name encoding, not an identifier)',
        "instructions": [
            ('CHINESE_COMMERCIAL_CODE', None),
        ],
    },
    1570: {
        "name": 'Cedula No.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'Cedula No.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1571: {
        "name": 'Passport',
        "group": 'SPECIFIC SENZING FEATURES (Direct Mappings)',
        "instructions": [
            ('PASSPORT_NUMBER', None),
            ('PASSPORT_COUNTRY', None),
        ],
    },
    1572: {
        "name": 'SSN',
        "group": 'SPECIFIC SENZING FEATURES (Direct Mappings)',
        "instructions": [
            ('SSN_NUMBER', None),
        ],
    },
    1573: {
        "name": 'R.F.C.',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1574: {
        "name": 'D.N.I.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'D.N.I.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1575: {
        "name": 'NIT #',
        "group": 'ACCOUNT/FINANCIAL IDENTIFIERS',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1576: {
        "name": 'US FEIN',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', 'US'),
        ],
    },
    1577: {
        "name": "Driver's License No.",
        "group": 'SPECIFIC SENZING FEATURES (Direct Mappings)',
        "instructions": [
            ('DRIVERS_LICENSE_NUMBER', None),
            ('DRIVERS_LICENSE_STATE', None),
        ],
    },
    1578: {
        "name": 'RUC #',
        "group": 'ACCOUNT/FINANCIAL IDENTIFIERS',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1579: {
        "name": 'N.I.E.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'N.I.E.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1580: {
        "name": 'C.I.F.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1581: {
        "name": 'Business Registration Document #',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1582: {
        "name": 'RIF #',
        "group": 'ACCOUNT/FINANCIAL IDENTIFIERS',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1584: {
        "name": 'National ID No.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1585: {
        "name": 'Registration ID',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1586: {
        "name": 'LE Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('LEI_NUMBER', None),
        ],
    },
    1587: {
        "name": 'Bosnian Personal ID No.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'BA'),
        ],
    },
    1588: {
        "name": 'Registered Charity No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1589: {
        "name": 'V.A.T. Number',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('TAX_ID_TYPE', 'vatCode'),
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1590: {
        "name": 'Credencial electoral',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'ELECTORAL'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1591: {
        "name": 'Kenyan ID No.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'KE'),
        ],
    },
    1592: {
        "name": 'Italian Fiscal Code',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', 'IT'),
        ],
    },
    1593: {
        "name": 'Serial No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'serialNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1594: {
        "name": 'C.I.N.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1595: {
        "name": 'C.U.I.T.',
        "group": 'ACCOUNT/FINANCIAL IDENTIFIERS',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'C.U.I.T.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1596: {
        "name": 'Tax ID No.',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1597: {
        "name": 'Moroccan Personal ID No.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'MA'),
        ],
    },
    1598: {
        "name": 'Public Security and Immigration No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'PSI'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1600: {
        "name": 'C.U.R.P.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'C.U.R.P.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'MX'),
        ],
    },
    1601: {
        "name": 'British National Overseas Passport',
        "group": 'SPECIFIC SENZING FEATURES (Direct Mappings)',
        "instructions": [
            ('PASSPORT_NUMBER', None),
            ('PASSPORT_COUNTRY', 'GB'),
        ],
    },
    1602: {
        "name": 'C.R. No.',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1603: {
        "name": 'UK Company Number',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'GB'),
        ],
    },
    1604: {
        "name": 'Immigration No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'IMMIGRATION'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1605: {
        "name": 'Travel Document Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'TRAVEL_DOC'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1607: {
        "name": 'Electoral Registry No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'ELECTORAL_REG'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1608: {
        "name": 'Identification Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1609: {
        "name": 'Paraguayan tax identification number',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', 'PY'),
        ],
    },
    1611: {
        "name": 'National Foreign ID Number',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1612: {
        "name": 'RFC',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1613: {
        "name": 'Diplomatic Passport',
        "group": 'SPECIFIC SENZING FEATURES (Direct Mappings)',
        "instructions": [
            ('PASSPORT_NUMBER', None),
            ('PASSPORT_COUNTRY', None),
        ],
    },
    1614: {
        "name": 'Dubai Chamber of Commerce Membership No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'AE'),
        ],
    },
    1615: {
        "name": 'Trade License No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'licenseNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1619: {
        "name": 'Commercial Registry Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1620: {
        "name": 'Certificate of Incorporation Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1621: {
        "name": 'Tourism License No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'licenseNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1623: {
        "name": 'Aircraft Serial Identification',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'serialNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1624: {
        "name": 'Cartilla de Servicio Militar Nacional',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'MILITARY_SERVICE'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1625: {
        "name": 'C.U.I.P.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'C.U.I.P.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1626: {
        "name": 'Vessel Registration Identification',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'imoNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1627: {
        "name": 'Personal ID Card',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1628: {
        "name": 'Federal ID Card',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1629: {
        "name": 'Registration Certificate Number (Dubai)',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'AE'),
        ],
    },
    1630: {
        "name": 'VisaNumberID',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'VISA'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1631: {
        "name": 'Matricula Mercantil No',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1632: {
        "name": 'Residency Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'RESIDENCY'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1633: {
        "name": 'Numero Unico de Identificacao Tributaria (NUIT)',
        "group": 'ACCOUNT/FINANCIAL IDENTIFIERS',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1634: {
        "name": 'CNP (Personal Numerical Code)',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'CNP (Personal Numerical Code)'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1635: {
        "name": 'Romanian Permanent Resident',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'ROM_PERM_RES'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', 'RO'),
        ],
    },
    1636: {
        "name": 'Government Gazette Number',
        "group": 'PAYLOAD - official gazette publication reference (shared, not an identifier)',
        "instructions": [
            ('GOVERNMENT_GAZETTE_NUMBER', None),
        ],
    },
    1638: {
        "name": 'Fiscal Code',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', None),
        ],
    },
    1639: {
        "name": 'Pilot License Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'licenseNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1642: {
        "name": 'Romanian C.R.',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'RO'),
        ],
    },
    1643: {
        "name": 'Folio Mercantil No.',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1644: {
        "name": 'Istanbul Chamber of Comm. No.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'TR'),
        ],
    },
    1645: {
        "name": 'Turkish Identification Number',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'Turkish Identification Number'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'TR'),
        ],
    },
    1646: {
        "name": 'Romanian Tax Registration',
        "group": 'TAX IDENTIFIERS (Classification per Spec Decision Tree)',
        "instructions": [
            ('TAX_ID_NUMBER', None),
            ('TAX_ID_COUNTRY', 'RO'),
        ],
    },
    1647: {
        "name": 'Stateless Person Passport',
        "group": 'SPECIFIC SENZING FEATURES (Direct Mappings)',
        "instructions": [
            ('PASSPORT_NUMBER', None),
            ('PASSPORT_COUNTRY', None),
        ],
    },
    1648: {
        "name": 'Stateless Person ID Card',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'STATELESS_ID'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1649: {
        "name": 'Refugee ID Card',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'REFUGEE_ID'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1712: {
        "name": 'I.F.E.',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'IFE'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1719: {
        "name": 'Branch Unit Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'branchUnitNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1720: {
        "name": 'Enterprise Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1721: {
        "name": 'Organization Code',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1739: {
        "name": "Citizen's Card Number",
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', "Citizen's Card Number"),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1740: {
        "name": 'UAE Identification',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'UAE Identification'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'AE'),
        ],
    },
    1747: {
        "name": 'United Social Credit Code Certificate (USCCC)',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'CN'),
        ],
    },
    1751: {
        "name": 'Chamber of Commerce Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1752: {
        "name": 'Legal Entity Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('LEI_NUMBER', None),
        ],
    },
    1753: {
        "name": 'Business Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1759: {
        "name": 'Birth Certificate Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'BIRTH_CERT'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1760: {
        "name": 'Business Registration Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1761: {
        "name": 'Registration Number',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1812: {
        "name": 'MSB Registration Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'licenseNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1835: {
        "name": 'File Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'fileNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    1854: {
        "name": 'C.U.I.',
        "group": 'NATIONAL IDENTIFIERS (Country-issued unique per person/org)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'C.U.I.'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    1891: {
        "name": "Seafarer's Identification Document",
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'SEAFARER_ID'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    2001: {
        "name": 'Unified Social Credit Code (USCC)',
        "group": 'BUSINESS/ORGANIZATION REGISTRATIONS (National-level)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'CN'),
        ],
    },
    2067: {
        "name": 'Central Registration System Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    2121: {
        "name": 'Economic Register Number (CBLS)',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', None),
        ],
    },
    2158: {
        "name": 'Trademark number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'trademarkNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    2159: {
        "name": 'Permit Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'permitNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    2728: {
        "name": 'Military Registration Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'militaryRegistrationNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
    2772: {
        "name": 'Russian State Individual Business Registration Number Pattern (OGRNIP)',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('NATIONAL_ID_TYPE', 'registrationNumber'),
            ('NATIONAL_ID_SUBTYPE', 'ogrnCode'),
            ('NATIONAL_ID_NUMBER', None),
            ('NATIONAL_ID_COUNTRY', 'RU'),
        ],
    },
    2790: {
        "name": 'Global Intermediary Identification Number',
        "group": 'OTHER IDENTIFIERS (Specialized/Unknown codes)',
        "instructions": [
            ('OTHER_ID_TYPE', 'giiNumber'),
            ('OTHER_ID_NUMBER', None),
            ('OTHER_ID_COUNTRY', None),
        ],
    },
}

# OFAC advanced mapper - identifier-type normalization

## Why

Every identifier the mapper emits carries a **TYPE** (e.g. `NATIONAL_ID_TYPE`, `TAX_ID_TYPE`) that Senzing
uses as an **exclusivity namespace**: two records only compare a given id number when they agree on
feature + type + country. Historically this mapper emitted *typed but not normalized* labels - one concept
under many spellings (`REG_NUMBER` / `COMPANY_NUMBER` / `Business Registration Number` ...), and the same
concept split across features (`NATIONAL_ID` vs `OTHER_ID` vs `ACCOUNT`). The effect: a shared registration
number under two different labels scores as a **conflict** (~50, `NO_CHANCE`) instead of a match, and OFAC
records never align cross-source with the same entities from OpenSanctions or Sayari.

This normalization makes the mapper emit, for each OFAC document type, the **correct Senzing feature** with a
**standardized type value**, so identical ids from different sources share one namespace and resolve.

## Authorities (in precedence order)

0. **Senzing identifier crosswalk** (`identifier_crosswalk.json`, the canonical VOCABULARY) - per scheme, the
   feature + TYPE token every source must emit, and the values that are never identifiers. Mapping OFAC's
   own labels onto it is this mapper's job (`src/config/`); the output is checked against it.

1. **Senzing Entity Specification** - the authoritative "Senzing home" (which *feature* an id belongs on).
   Confirms: tax ids -> `TAX_ID` (it marks *"EIN as NATIONAL_ID"* as wrong); `CEDULA`/`OGRN` -> `NATIONAL_ID`;
   `VAT`/`TIN` -> `TAX_ID`; `LEI`/`SSN`/`Passport` -> their own features; and *"code-driven
   'identifier' tables ... name-encodings, free-text notes ... do not map as identifiers (including OTHER_ID) ->
   route to a registered feature, otherwise payload or omit."*
2. **OpenSanctions `doc_types` crosswalk** (FtM property names) - which OFAC labels are the **same scheme**
   (e.g. every business-registration label is one `registrationNumber` concept). The mapper emits each
   scheme as one upper-case token: `REGISTRATION_NUMBER`, `VAT`, `CURP`, `DNI`, ... (see the generated table).
3. **`Senzing/mapper-sayari-spark`** reviewed `sayari_codes.csv` (all 980 rows `REVIEWED=Y`) - an independent
   cross-check of feature placement (tax -> `TAX_ID`, voter -> `OTHER_ID`, CUIT -> `NATIONAL_ID`).

## Rules

- **R1 - generic labels stay UNTYPED.** A label that merely restates the feature (`National ID No.`,
  `Identification Number`, `Personal ID Card`) carries no `NATIONAL_ID_TYPE` - the feature already declares
  the class, and an explicit generic type would falsely conflict with a specific-typed value of the same
  number.
- **R2 - exclusivity by the strict test.** An id goes on an **exclusive** feature (`NATIONAL_ID`/`TAX_ID`)
  only if each entity has exactly **one for its lifetime per country+type** and it is **uniquely held**.
  Many-per-entity / reissued / shared values (licenses, permits, trademarks, serials, voter rolls) -> the
  **non-exclusive `OTHER_ID`** feature (otherwise they manufacture F1ES denials or spurious links).
- **R3 - dedicated features win.** `PASSPORT`, `SSN`, `LEI_NUMBER`, `DUNS_NUMBER`, `DRIVERS_LICENSE` keep
  their own features rather than collapsing into `NATIONAL_ID`.
- **R4 - not-an-identifier -> payload.** Name-encodings and publication references are preserved as top-level
  payload attributes, never as resolvable identifiers.

## Crosswalk (by class)

| Class | OFAC labels (examples) | Senzing home | Type value |
| --- | --- | --- | --- |
| Business registration | Registration Number, Company Number, Commercial Registry Number, Enterprise Number, C.I.N., Registered Charity No., USCC/USCCC, File Number (a US state / Czech company file number) ... | `NATIONAL_ID` | `NATIONAL_ID_TYPE=REGISTRATION_NUMBER` |
| Russian individual-entrepreneur reg | OGRNIP | `NATIONAL_ID` | `NATIONAL_ID_TYPE=REGISTRATION_NUMBER` + `NATIONAL_ID_SUBTYPE=OGRNIP` (see below) |
| LEI | Legal Entity Number, LE Number | `LEI_NUMBER` | - |
| Tax numbers | R.F.C., N.I.F., C.I.F., RTN, RUC #, RIF #, US FEIN, Italian Fiscal Code, Fiscal Code, Tax ID No., Paraguayan tax, Romanian Tax Registration, NIT #, NUIT | `TAX_ID` | no `TAX_ID_TYPE`; a Russian-issued Tax ID No. is `TAX_ID_TYPE=INN` |
| VAT | V.A.T. Number | `TAX_ID` | `TAX_ID_TYPE=VAT` |
| CUIT | C.U.I.T. | `TAX_ID` | `TAX_ID_TYPE=CUIT`, `TAX_ID_COUNTRY=AR` |
| Person national-id schemes | C.U.R.P., D.N.I., Cedula No., C.U.I.P., C.U.I., N.I.E., CNP, Turkish/UAE/Citizen's Card, Tazkira | `NATIONAL_ID` | upper-case scheme token (`CURP`, `DNI`, `CEDULA`, `CUIP`, `CUI`, `NIE`, `CNP`, `TCKN`, `EID`, `CCN`, `TAZKIRA`) |
| Generic national-id (R1) | National ID No., Identification Number, Numero de Identidad, Tarjeta de Identidad, Personal/Federal ID Card, Kenyan/Bosnian/Moroccan ID No., National Foreign ID Number | `NATIONAL_ID` | no `NATIONAL_ID_TYPE` |
| Passport / SSN | Passport variants, SSN | `PASSPORT` / `SSN` | - |
| Licenses / permits / documents (R2) | Trade/Tourism/Pilot/MSB/Afghan-MSP License, Permit, SRE Permit, Travel Document, Visa, Immigration, Public Security and Immigration No., Refugee/Stateless/Seafarer ID, Residency, Romanian Permanent Resident, Military Registration / Cartilla, Birth Certificate, voter/electoral (Credencial electoral, I.F.E., Electoral Registry No.) | `OTHER_ID` | crosswalk token: `LICENSE` / `PERMIT` / `TRAVEL_DOC` / `VISA` / `IMMIGRATION` / `RESIDENCY` / `MILITARY_SERVICE` / `BIRTH_CERT` / `ELECTORAL` / ... |
| Transport / financial codes | Aircraft Serial, GIIN, SCAC, CAAT | `OTHER_ID` | `AIRCRAFT_MSN` / `GIIN` / `SCAC` (US) / `CAAT` (MX) |
| Vessel ids | Vessel Registration Identification (IMO), MMSI | `IMO_NUMBER` / `MMSI_NUMBER` (custom features, see the config section) | - |
| Driver's license | Driver's License No. | `DRIVERS_LICENSE` | - |
| Not identifiers (R4) -> payload | Chinese Commercial Code (a Chinese-name telegraph encoding, shared by same-named entities), Government Gazette Number (a publication reference, shared per issue), Serial No. (of an unnamed document), Trademark number (identifies a mark, not its holder), Branch Unit Number (identifies a branch), Entity Code (issuing scheme unknown) | payload `CHINESE_COMMERCIAL_CODE` / `GOVERNMENT_GAZETTE_NUMBER` / `SERIAL_NUMBER` / `TRADEMARK_NUMBER` / `BRANCH_UNIT_NUMBER` / `ENTITY_CODE` | - |

The complete per-type table is generated from the code at the end of this document.

### Tax deep-dive

`TAX_ID` in the config is **untyped without `TAX_ID_TYPE`** - one shared namespace per country. A generic
national tax number carries **no** `TAX_ID_TYPE` (so a bare tax number from another source bridges instead of
conflicting on a token); only VAT is typed, `TAX_ID_TYPE=VAT`, which keeps it apart from the national tax
number once `TAX_ID_TYPE` is configured (below). **CUIT** is Argentina's tax id: the crosswalk vocabulary
puts it on `TAX_ID` with `TAX_ID_TYPE=CUIT`, country `AR` (OpenSanctions and Sayari had placed it on
`NATIONAL_ID`). **INN**: OFAC labels the Russian taxpayer number "Tax ID No."; the label names the scheme only
together with its issuer, so the entry's `type_by_country` (`{"RU": "INN"}`) adds `TAX_ID_TYPE=INN` when the
document is issued by Russia - the same token the OpenSanctions exporter emits for `innCode`. **NIT / NUIT / Italian Fiscal Code** are tax ids -> `TAX_ID` (owner-confirmed; diverges from
OpenSanctions' `idNumber`/`OTHER_ID` bucketing for those, which is arguably an upstream OpenSanctions defect).

## Config dependency - `TAX_ID_TYPE`

`TAX_ID_TYPE` requires the `ID_TYPE` element on the `TAX_ID` feature **and in its comparison call** (the
comparison-call row is what actually makes the type *scored* - the feature-BOM element alone only stores it).
**Senzing 4.4's default config ships it.** For engines whose config predates that (e.g. 4.4.0-26224, whose
template has `NATIONAL_ID ID_TYPE` scored but nothing on `TAX_ID`), apply
`src/ofac_config_updates_tax_id_type.gtc` (`sz_configtool -f ...`) - three commands mirroring `NATIONAL_ID`:
`addElementToFeature`, `addComparisonCallElement` (scores it), and `addAttribute`. Do not apply it to a config
that already has `TAX_ID_TYPE`: sz_configtool reports the commands as already present and does not save.
Verified live by the maintainer: two
records sharing a tax number but with different `TAX_ID_TYPE` values are held apart only once the
comparison-call element is present.

## Config dependency - custom features (`src/ofac_config_updates.gtc`)

The mapper emits features that are not in the default Senzing config. `src/ofac_config_updates.gtc` registers
them; apply it (`sz_configtool -f src/ofac_config_updates.gtc`) before loading, or the attributes are ignored
at load. Verified with sz_configtool against the Senzing 4.4.3 default config (all commands succeed, the
config saves, and a loaded record shows the features).

| Attribute | What | Command |
| --- | --- | --- |
| `OFAC_ID` | the SDN uid, on every record (also `RECORD_ID`), so sources citing it (OpenSanctions) bridge | `templateAdd` `GLOBAL_ID`, `F1E` |
| `IMO_NUMBER` | Vessel Registration Identification (value as the feed gives it, `IMO 1234567`) | `templateAdd` `GLOBAL_ID`, `F1E` |
| `MMSI_NUMBER` | MMSI | `templateAdd` `GLOBAL_ID`, `F1E` |
| `CALL_SIGN` | Vessel Call Sign / Other Vessel Call Sign | `templateAdd` `GLOBAL_ID`, `F1E` |
| `ISIN_NUMBER` | ISIN | `templateAdd` `GLOBAL_ID`, `F1E` |
| `NATIONAL_ID_SUBTYPE` | the sub-registry under a broad type: `OGRNIP` under `REGISTRATION_NUMBER` | `addElementToFeature` + `addComparisonCallElement` + `addAttribute` (element `ID_SUBTYPE`) |

OGRNIP (mapping entry 2772; feed `IDRegDocTypeID` 92772, matched by name) is
`NATIONAL_ID_TYPE=REGISTRATION_NUMBER` + `NATIONAL_ID_SUBTYPE=OGRNIP`, country `RU`. A blank subtype on one
side still matches a populated one (Senzing GDEV-4439), so a source that knows only the broad registration
number still bridges. On the 2026-10-30 SDN file it occurs on 2 records.

## Payload attributes

Documents marked `"payload": True` in `src/config/id_doc_mappings.py` (Chinese Commercial Code, Government
Gazette Number, Serial No., Trademark number, Branch Unit Number, Entity Code) become top-level record
attributes, never `FEATURES`. Routing is that key only; the `group`
label is descriptive. When a party holds several documents of one payload type, every distinct value is kept,
joined with `"; "` in document order (the form `SANCTIONS_LIST` and `SANCTIONS_PROGRAMS` use). On the
2026-10-02 SDN file no party holds more than one such document (351 Government Gazette Numbers on 351 parties;
12 Chinese Commercial Codes on 12 parties), so no value was being dropped there.

The same rule (one helper, `_append_payload_value`) applies to payload *features* from
`src/config/feature_mappings.py` (e.g. `SECONDARY_SANCTIONS_RISK`, `EO_13846_INFO`, `ORGANIZATION_TYPE`). Those
previously kept only the first value: on the 2026-10-02 file 731 records carried 747 payload keys repeated with
different values, and 991 distinct values were dropped. They are now kept, after the previously kept first
value. Identical repeats stay one value; a single value is written unchanged. The longest joined value on that
file is 373 characters (`EO_13846_INFO`).

Payload *features* that are not identifiers: Title (a position held, e.g. "Member of the State Duma ...",
`TITLE` - as a separate `NAME_PREFIX` object it fragmented the name feature), vessel flags (`VESSEL_FLAG`,
`FORMER_VESSEL_FLAG`, `OTHER_VESSEL_FLAG` - a country shared by many vessels), `UN_LOCODE` (a place code),
`EQUITY_TICKER` and `MICEX_CODE` (tickers, reused across instruments and venues), and
`PAIPA_SECTION_2_INFO` (sanctions-program text).

## Cross-source alignment & validation

- **Alignment** is realized when OFAC, OpenSanctions, and Sayari emit the same feature + type for the same
  id, so the peer mappers must emit the same type tokens listed here (`REGISTRATION_NUMBER`, `VAT`,
  `CURP`, ...). (The Sayari mapper is a separate future project.)
- **Pure relabel:** on the full Treasury SDN the id-NUMBER multiset is unchanged before/after - only
  feature/type labels move; Chinese Commercial Codes and Government Gazette Numbers move to payload (12 and
  351 on the 2026-10-02 file, 19,488 records). The crosswalk-conformance pass (2026-10-30 file, 19,484
  records) likewise moves only the non-identifiers to payload - 1,733 values: vessel flags 1,659, Entity Code
  40, Branch Unit Number 11, PAIPA text 11, UN/LOCODE 5, equity ticker 3, Serial No. 2, MICEX 1, Trademark 1 -
  and every other id number is kept.
- **Gates:** the mapped SDN passes the Senzing MCP JSON analyzer (no structural errors) and the identifier
  crosswalk conformance check (0 violations).
- **A/B gate:** run a ground-truth entity-resolution A/B (baseline vs normalized) before the change reaches the
  deployed demo; re-mapping OFAC should co-land with the OpenSanctions peer normalization so cross-source ids
  align rather than detach.

## Open / borderline (kept conservative - promote only with confirmation)

- Voter/electoral -> `OTHER_ID` (Sayari's reviewed call; voter rolls are not one-per-lifetime-unique).
- Military Registration / Cartilla Militar / Birth Certificate -> `OTHER_ID` (uniqueness/one-per-lifetime
  uncertain; promote to `NATIONAL_ID` only if confirmed).
- Permit -> `OTHER_ID` `PERMIT` (many-per-entity; OpenSanctions makes permits exclusive
  `registrationNumber`, which would manufacture denials). Trademark and Serial No. -> payload.
- Branch Unit Number (Belgian establishment-unit / Dutch KvK branch numbers) and Entity Code (Cuban entities;
  scheme unconfirmed) -> payload until the crosswalk names a canonical entry for them.

## Generated table (every mapped document type)

`OFAC type` is the mapping key; the live feed uses different IDs for some types (e.g. 91508, 92772) and the
mapper matches those by label. A `-` means the attribute is not emitted or takes its value from the document.

<!-- BEGIN GENERATED ID TABLE (python tests/test_docs_id_table.py) -->

| OFAC type | OFAC label | Senzing feature | Type value | Subtype | Fixed country |
| --- | --- | --- | --- | --- | --- |
| 1236 | Afghan Money Service Provider License Number | `OTHER_ID` | `LICENSE` | - | `AF` |
| 1264 | MMSI | `MMSI_NUMBER` | - | - | - |
| 1412 | Company Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1475 | Public Registration Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1478 | N.I.F. | `TAX_ID` | - | - | - |
| 1481 | RTN | `TAX_ID` | - | - | - |
| 1482 | Numero de Identidad | `NATIONAL_ID` | - | - | - |
| 1483 | Tarjeta de Identidad | `NATIONAL_ID` | - | - | - |
| 1484 | SRE Permit No. | `OTHER_ID` | `PERMIT` | - | - |
| 1492 | Tazkira National ID Card | `NATIONAL_ID` | `TAZKIRA` | - | `AF` |
| 1504 | License | `OTHER_ID` | `LICENSE` | - | - |
| 1508 | Chinese Commercial Code | payload `CHINESE_COMMERCIAL_CODE` | - | - | - |
| 1570 | Cedula No. | `NATIONAL_ID` | `CEDULA` | - | - |
| 1571 | Passport | `PASSPORT` | - | - | - |
| 1572 | SSN | `SSN` | - | - | - |
| 1573 | R.F.C. | `TAX_ID` | - | - | - |
| 1574 | D.N.I. | `NATIONAL_ID` | `DNI` | - | - |
| 1575 | NIT # | `TAX_ID` | - | - | - |
| 1576 | US FEIN | `TAX_ID` | - | - | `US` |
| 1577 | Driver's License No. | `DRIVERS_LICENSE` | - | - | - |
| 1578 | RUC # | `TAX_ID` | - | - | - |
| 1579 | N.I.E. | `NATIONAL_ID` | `NIE` | - | - |
| 1580 | C.I.F. | `TAX_ID` | - | - | - |
| 1581 | Business Registration Document # | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1582 | RIF # | `TAX_ID` | - | - | - |
| 1584 | National ID No. | `NATIONAL_ID` | - | - | - |
| 1585 | Registration ID | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1586 | LE Number | `LEI` | - | - | - |
| 1587 | Bosnian Personal ID No. | `NATIONAL_ID` | - | - | `BA` |
| 1588 | Registered Charity No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1589 | V.A.T. Number | `TAX_ID` | `VAT` | - | - |
| 1590 | Credencial electoral | `OTHER_ID` | `ELECTORAL` | - | - |
| 1591 | Kenyan ID No. | `NATIONAL_ID` | - | - | `KE` |
| 1592 | Italian Fiscal Code | `TAX_ID` | - | - | `IT` |
| 1593 | Serial No. | payload `SERIAL_NUMBER` | - | - | - |
| 1594 | C.I.N. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1595 | C.U.I.T. | `TAX_ID` | `CUIT` | - | `AR` |
| 1596 | Tax ID No. | `TAX_ID` | `INN` if RU | - | - |
| 1597 | Moroccan Personal ID No. | `NATIONAL_ID` | - | - | `MA` |
| 1598 | Public Security and Immigration No. | `OTHER_ID` | `IMMIGRATION` | - | - |
| 1600 | C.U.R.P. | `NATIONAL_ID` | `CURP` | - | `MX` |
| 1601 | British National Overseas Passport | `PASSPORT` | - | - | `GB` |
| 1602 | C.R. No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1603 | UK Company Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `GB` |
| 1604 | Immigration No. | `OTHER_ID` | `IMMIGRATION` | - | - |
| 1605 | Travel Document Number | `OTHER_ID` | `TRAVEL_DOC` | - | - |
| 1607 | Electoral Registry No. | `OTHER_ID` | `ELECTORAL` | - | - |
| 1608 | Identification Number | `NATIONAL_ID` | - | - | - |
| 1609 | Paraguayan tax identification number | `TAX_ID` | - | - | `PY` |
| 1611 | National Foreign ID Number | `NATIONAL_ID` | - | - | - |
| 1612 | RFC | `TAX_ID` | - | - | - |
| 1613 | Diplomatic Passport | `PASSPORT` | - | - | - |
| 1614 | Dubai Chamber of Commerce Membership No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `AE` |
| 1615 | Trade License No. | `OTHER_ID` | `LICENSE` | - | - |
| 1619 | Commercial Registry Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1620 | Certificate of Incorporation Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1621 | Tourism License No. | `OTHER_ID` | `LICENSE` | - | - |
| 1623 | Aircraft Serial Identification | `OTHER_ID` | `AIRCRAFT_MSN` | - | - |
| 1624 | Cartilla de Servicio Militar Nacional | `OTHER_ID` | `MILITARY_SERVICE` | - | - |
| 1625 | C.U.I.P. | `NATIONAL_ID` | `CUIP` | - | - |
| 1626 | Vessel Registration Identification | `IMO_NUMBER` | - | - | - |
| 1627 | Personal ID Card | `NATIONAL_ID` | - | - | - |
| 1628 | Federal ID Card | `NATIONAL_ID` | - | - | - |
| 1629 | Registration Certificate Number (Dubai) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `AE` |
| 1630 | VisaNumberID | `OTHER_ID` | `VISA` | - | - |
| 1631 | Matricula Mercantil No | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1632 | Residency Number | `OTHER_ID` | `RESIDENCY` | - | - |
| 1633 | Numero Unico de Identificacao Tributaria (NUIT) | `TAX_ID` | - | - | - |
| 1634 | CNP (Personal Numerical Code) | `NATIONAL_ID` | `CNP` | - | - |
| 1635 | Romanian Permanent Resident | `OTHER_ID` | `RESIDENCY` | - | `RO` |
| 1636 | Government Gazette Number | payload `GOVERNMENT_GAZETTE_NUMBER` | - | - | - |
| 1638 | Fiscal Code | `TAX_ID` | - | - | - |
| 1639 | Pilot License Number | `OTHER_ID` | `LICENSE` | - | - |
| 1642 | Romanian C.R. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `RO` |
| 1643 | Folio Mercantil No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1644 | Istanbul Chamber of Comm. No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `TR` |
| 1645 | Turkish Identification Number | `NATIONAL_ID` | `TCKN` | - | `TR` |
| 1646 | Romanian Tax Registration | `TAX_ID` | - | - | `RO` |
| 1647 | Stateless Person Passport | `PASSPORT` | - | - | - |
| 1648 | Stateless Person ID Card | `OTHER_ID` | `STATELESS_ID` | - | - |
| 1649 | Refugee ID Card | `OTHER_ID` | `REFUGEE_ID` | - | - |
| 1712 | I.F.E. | `OTHER_ID` | `ELECTORAL` | - | - |
| 1719 | Branch Unit Number | payload `BRANCH_UNIT_NUMBER` | - | - | - |
| 1720 | Enterprise Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1721 | Organization Code | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1739 | Citizen's Card Number | `NATIONAL_ID` | `CCN` | - | - |
| 1740 | UAE Identification | `NATIONAL_ID` | `EID` | - | `AE` |
| 1747 | United Social Credit Code Certificate (USCCC) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `CN` |
| 1751 | Chamber of Commerce Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1752 | Legal Entity Number | `LEI` | - | - | - |
| 1753 | Business Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1759 | Birth Certificate Number | `OTHER_ID` | `BIRTH_CERT` | - | - |
| 1760 | Business Registration Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1761 | Registration Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1812 | MSB Registration Number | `OTHER_ID` | `LICENSE` | - | - |
| 1835 | File Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1854 | C.U.I. | `NATIONAL_ID` | `CUI` | - | - |
| 1891 | Seafarer's Identification Document | `OTHER_ID` | `SEAFARER_ID` | - | - |
| 2001 | Unified Social Credit Code (USCC) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `CN` |
| 2067 | Central Registration System Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 2121 | Economic Register Number (CBLS) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 2158 | Trademark number | payload `TRADEMARK_NUMBER` | - | - | - |
| 2159 | Permit Number | `OTHER_ID` | `PERMIT` | - | - |
| 2728 | Military Registration Number | `OTHER_ID` | `MILITARY_SERVICE` | - | - |
| 2772 | Russian State Individual Business Registration Number Pattern (OGRNIP) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | `OGRNIP` | `RU` |
| 2790 | Global Intermediary Identification Number | `OTHER_ID` | `GIIN` | - | - |
| 2883 | Entity Code | payload `ENTITY_CODE` | - | - | - |
| 2897 | Standard Carrier Alpha Code (SCAC) | `OTHER_ID` | `SCAC` | - | `US` |
| 2900 | Codigo Alfanumerico Armonizado del Transportista (CAAT) | `OTHER_ID` | `CAAT` | - | `MX` |

<!-- END GENERATED ID TABLE -->

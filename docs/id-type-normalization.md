# OFAC advanced mapper — identifier-type normalization

## Why

Every identifier the mapper emits carries a **TYPE** (e.g. `NATIONAL_ID_TYPE`, `TAX_ID_TYPE`) that Senzing
uses as an **exclusivity namespace**: two records only compare a given id number when they agree on
feature + type + country. Historically this mapper emitted *typed but not normalized* labels — one concept
under many spellings (`REG_NUMBER` / `COMPANY_NUMBER` / `Business Registration Number` …), and the same
concept split across features (`NATIONAL_ID` vs `OTHER_ID` vs `ACCOUNT`). The effect: a shared registration
number under two different labels scores as a **conflict** (~50, `NO_CHANCE`) instead of a match, and OFAC
records never align cross-source with the same entities from OpenSanctions or Sayari.

This normalization makes the mapper emit, for each OFAC document type, the **correct Senzing feature** with a
**standardized type value**, so identical ids from different sources share one namespace and resolve.

## Authorities (in precedence order)

1. **Senzing Entity Specification** — the authoritative "Senzing home" (which *feature* an id belongs on).
   Confirms: tax ids → `TAX_ID` (it marks *"EIN as NATIONAL_ID"* as wrong); `CEDULA`/`OGRN` → `NATIONAL_ID`;
   `VAT`/`TIN` → `TAX_ID`; `IMO` → `OTHER_ID`; `LEI`/`SSN`/`Passport` → their own features; and *"code-driven
   'identifier' tables … name-encodings, free-text notes … do not map as identifiers (including OTHER_ID) →
   route to a registered feature, otherwise payload or omit."*
2. **OpenSanctions `doc_types` crosswalk** (FtM property names) — which OFAC labels are the **same scheme**
   (e.g. every business-registration label is one `registrationNumber` concept). The mapper emits each
   scheme as one upper-case token: `REGISTRATION_NUMBER`, `VAT`, `CURP`, `DNI`, … (see the generated table).
3. **`Senzing/mapper-sayari-spark`** reviewed `sayari_codes.csv` (all 980 rows `REVIEWED=Y`) — an independent
   cross-check of feature placement (tax → `TAX_ID`, voter → `OTHER_ID`, CUIT → `NATIONAL_ID`).

## Rules

- **R1 — generic labels stay UNTYPED.** A label that merely restates the feature (`National ID No.`,
  `Identification Number`, `Personal ID Card`) carries no `NATIONAL_ID_TYPE` — the feature already declares
  the class, and an explicit generic type would falsely conflict with a specific-typed value of the same
  number.
- **R2 — exclusivity by the strict test.** An id goes on an **exclusive** feature (`NATIONAL_ID`/`TAX_ID`)
  only if each entity has exactly **one for its lifetime per country+type** and it is **uniquely held**.
  Many-per-entity / reissued / shared values (licenses, permits, trademarks, serials, voter rolls) → the
  **non-exclusive `OTHER_ID`** feature (otherwise they manufacture F1ES denials or spurious links).
- **R3 — dedicated features win.** `PASSPORT`, `SSN`, `LEI_NUMBER`, `DUNS_NUMBER`, `DRIVERS_LICENSE` keep
  their own features rather than collapsing into `NATIONAL_ID`.
- **R4 — not-an-identifier → payload.** Name-encodings and publication references are preserved as top-level
  payload attributes, never as resolvable identifiers.

## Crosswalk (by class)

| Class | OFAC labels (examples) | Senzing home | Type value |
| --- | --- | --- | --- |
| Business registration | Registration Number, Company Number, Commercial Registry Number, Enterprise Number, C.I.N., Registered Charity No., USCC/USCCC … | `NATIONAL_ID` | `NATIONAL_ID_TYPE=REGISTRATION_NUMBER` |
| Russian individual-entrepreneur reg | OGRNIP | `NATIONAL_ID` | `NATIONAL_ID_TYPE=REGISTRATION_NUMBER` + `NATIONAL_ID_SUBTYPE=OGRN` (see below) |
| LEI | Legal Entity Number, LE Number | `LEI_NUMBER` | — |
| Tax numbers | R.F.C., N.I.F., C.I.F., RTN, RUC #, RIF #, US FEIN, Italian Fiscal Code, Fiscal Code, Tax ID No., Paraguayan tax, Romanian Tax Registration, NIT #, NUIT | `TAX_ID` | no `TAX_ID_TYPE` |
| VAT | V.A.T. Number | `TAX_ID` | `TAX_ID_TYPE=VAT` |
| Person national-id schemes | C.U.R.P., D.N.I., Cedula No., C.U.I.P., C.U.I., C.U.I.T., N.I.E., CNP, Turkish/UAE/Citizen's Card, Tazkira | `NATIONAL_ID` | upper-case scheme token (`CURP`, `DNI`, `CEDULA`, `CUIP`, `CUI`, `CUIT`, `NIE`, `CNP`, `TCKN`, `EID`, `CCN`, `TAZKIRA`) |
| Generic national-id (R1) | National ID No., Identification Number, Numero de Identidad, Personal/Federal ID Card, Kenyan/Bosnian/Moroccan ID No., National Foreign ID Number | `NATIONAL_ID` | no `NATIONAL_ID_TYPE` |
| Passport / SSN | Passport variants, SSN | `PASSPORT` / `SSN` | — |
| Licenses / permits / documents (R2) | Trade/Tourism/Pilot/MSB/Afghan-MSP License, Trademark, Permit, Serial No., Travel Document, Visa, Immigration, Refugee/Stateless/Seafarer ID, Residency, File Number, voter/electoral (Credencial electoral, I.F.E., Electoral Registry No.) | `OTHER_ID` | `licenseNumber` / `permitNumber` / `trademarkNumber` / `serialNumber` / `fileNumber` / an upper-case label (`TRAVEL_DOC`, `VISA`, `ELECTORAL`, …) |
| Vessel / aircraft / GIIN / MMSI | IMO, Aircraft Serial, GIIN, MMSI | `OTHER_ID` | `imoNumber` / `serialNumber` / `giiNumber` / `MMSI` |
| Driver's license | Driver's License No. | `DRIVERS_LICENSE` | — |
| Not identifiers (R4) → payload | Chinese Commercial Code (a Chinese-name telegraph encoding, shared by same-named entities), Government Gazette Number (a publication reference, shared per issue) | payload `CHINESE_COMMERCIAL_CODE` / `GOVERNMENT_GAZETTE_NUMBER` | — |

The complete per-type table is generated from the code at the end of this document.

### Tax deep-dive

`TAX_ID` in the config is **untyped without `TAX_ID_TYPE`** — one shared namespace per country. A generic
national tax number carries **no** `TAX_ID_TYPE` (so a bare tax number from another source bridges instead of
conflicting on a token); only VAT is typed, `TAX_ID_TYPE=VAT`, which keeps it apart from the national tax
number once `TAX_ID_TYPE` is configured (below). **CUIT** is
Argentina's universal identifier (used well beyond tax) — OpenSanctions *and* Sayari place it on `NATIONAL_ID`,
so it stays there. **NIT / NUIT / Italian Fiscal Code** are tax ids → `TAX_ID` (owner-confirmed; diverges from
OpenSanctions' `idNumber`/`OTHER_ID` bucketing for those, which is arguably an upstream OpenSanctions defect).

## Config dependency — `TAX_ID_TYPE`

`TAX_ID_TYPE` requires the `ID_TYPE` element on the `TAX_ID` feature **and in its comparison call** (the
comparison-call row is what actually makes the type *scored* — the feature-BOM element alone only stores it).
**Senzing 4.4's default config ships it.** For engines whose config predates that (e.g. 4.4.0-26224, whose
template has `NATIONAL_ID ID_TYPE` scored but nothing on `TAX_ID`), apply `src/ofac_config_updates.gtc`
(`sz_configtool -f …`) — three commands mirroring `NATIONAL_ID`: `addElementToFeature`,
`addComparisonCallElement` (scores it), and `addAttribute`. Idempotent. Verified live by the maintainer: two
records sharing a tax number but with different `TAX_ID_TYPE` values are held apart only once the
comparison-call element is present.

## Config dependency — `NATIONAL_ID_SUBTYPE` (OGRNIP)

The mapper emits `NATIONAL_ID_SUBTYPE=OGRN` for OGRNIP (mapping entry 2772; feed `IDRegDocTypeID` 92772,
matched by name), next to `NATIONAL_ID_TYPE=REGISTRATION_NUMBER`. `NATIONAL_ID_SUBTYPE` is **not** registered by
`src/ofac_config_updates.gtc` and is **not** in the default Senzing config yet. This is deliberate: the attribute
is planned for a future default config. Until it is added — by that config, or by a user who registers the
subtype in their own config — the value is ignored at load. On the 2026-10-02 SDN file it occurs on 2 records.

## Payload attributes

Documents marked `"payload": True` in `src/config/id_doc_mappings.py` (Chinese Commercial Code, Government
Gazette Number) become top-level record attributes, never `FEATURES`. Routing is that key only; the `group`
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

## Cross-source alignment & validation

- **Alignment** is realized when OFAC, OpenSanctions, and Sayari emit the same feature + type for the same
  id, so the peer mappers must emit the same type tokens listed here (`REGISTRATION_NUMBER`, `VAT`,
  `CURP`, …). (The Sayari mapper is a separate future project.)
- **Pure relabel:** on the full Treasury SDN the id-NUMBER multiset is unchanged before/after — only
  feature/type labels move; Chinese Commercial Codes and Government Gazette Numbers move to payload (12 and
  351 on the 2026-10-02 file, 19,488 records).
- **A/B gate:** run a ground-truth entity-resolution A/B (baseline vs normalized) before the change reaches the
  deployed demo; re-mapping OFAC should co-land with the OpenSanctions peer normalization so cross-source ids
  align rather than detach.

## Open / borderline (kept conservative — promote only with confirmation)

- Voter/electoral → `OTHER_ID` (Sayari's reviewed call; voter rolls are not one-per-lifetime-unique).
- Military Registration / Cartilla Militar / Birth Certificate → `OTHER_ID` (uniqueness/one-per-lifetime
  uncertain; promote to `NATIONAL_ID` only if confirmed).
- Trademark / Permit / Serial No. → `OTHER_ID` (many-per-entity; OpenSanctions makes them exclusive
  `registrationNumber`, which would manufacture denials).

## Generated table (every mapped document type)

`OFAC type` is the mapping key; the live feed uses different IDs for some types (e.g. 91508, 92772) and the
mapper matches those by label. A `-` means the attribute is not emitted or takes its value from the document.

<!-- BEGIN GENERATED ID TABLE (python tests/test_docs_id_table.py) -->

| OFAC type | OFAC label | Senzing feature | Type value | Subtype | Fixed country |
| --- | --- | --- | --- | --- | --- |
| 1236 | Afghan Money Service Provider License Number | `OTHER_ID` | `licenseNumber` | - | `AF` |
| 1264 | MMSI | `OTHER_ID` | `MMSI` | - | - |
| 1412 | Company Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1475 | Public Registration Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1478 | N.I.F. | `TAX_ID` | - | - | - |
| 1481 | RTN | `TAX_ID` | - | - | - |
| 1482 | Numero de Identidad | `NATIONAL_ID` | - | - | - |
| 1484 | SRE Permit No. | `OTHER_ID` | `SRE_PERMIT` | - | - |
| 1492 | Tazkira National ID Card | `NATIONAL_ID` | `TAZKIRA` | - | `AF` |
| 1504 | License | `OTHER_ID` | `licenseNumber` | - | - |
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
| 1593 | Serial No. | `OTHER_ID` | `serialNumber` | - | - |
| 1594 | C.I.N. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1595 | C.U.I.T. | `NATIONAL_ID` | `CUIT` | - | - |
| 1596 | Tax ID No. | `TAX_ID` | - | - | - |
| 1597 | Moroccan Personal ID No. | `NATIONAL_ID` | - | - | `MA` |
| 1598 | Public Security and Immigration No. | `OTHER_ID` | `PSI` | - | - |
| 1600 | C.U.R.P. | `NATIONAL_ID` | `CURP` | - | `MX` |
| 1601 | British National Overseas Passport | `PASSPORT` | - | - | `GB` |
| 1602 | C.R. No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1603 | UK Company Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `GB` |
| 1604 | Immigration No. | `OTHER_ID` | `IMMIGRATION` | - | - |
| 1605 | Travel Document Number | `OTHER_ID` | `TRAVEL_DOC` | - | - |
| 1607 | Electoral Registry No. | `OTHER_ID` | `ELECTORAL_REG` | - | - |
| 1608 | Identification Number | `NATIONAL_ID` | - | - | - |
| 1609 | Paraguayan tax identification number | `TAX_ID` | - | - | `PY` |
| 1611 | National Foreign ID Number | `NATIONAL_ID` | - | - | - |
| 1612 | RFC | `TAX_ID` | - | - | - |
| 1613 | Diplomatic Passport | `PASSPORT` | - | - | - |
| 1614 | Dubai Chamber of Commerce Membership No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `AE` |
| 1615 | Trade License No. | `OTHER_ID` | `licenseNumber` | - | - |
| 1619 | Commercial Registry Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1620 | Certificate of Incorporation Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1621 | Tourism License No. | `OTHER_ID` | `licenseNumber` | - | - |
| 1623 | Aircraft Serial Identification | `OTHER_ID` | `serialNumber` | - | - |
| 1624 | Cartilla de Servicio Militar Nacional | `OTHER_ID` | `MILITARY_SERVICE` | - | - |
| 1625 | C.U.I.P. | `NATIONAL_ID` | `CUIP` | - | - |
| 1626 | Vessel Registration Identification | `OTHER_ID` | `imoNumber` | - | - |
| 1627 | Personal ID Card | `NATIONAL_ID` | - | - | - |
| 1628 | Federal ID Card | `NATIONAL_ID` | - | - | - |
| 1629 | Registration Certificate Number (Dubai) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `AE` |
| 1630 | VisaNumberID | `OTHER_ID` | `VISA` | - | - |
| 1631 | Matricula Mercantil No | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1632 | Residency Number | `OTHER_ID` | `RESIDENCY` | - | - |
| 1633 | Numero Unico de Identificacao Tributaria (NUIT) | `TAX_ID` | - | - | - |
| 1634 | CNP (Personal Numerical Code) | `NATIONAL_ID` | `CNP` | - | - |
| 1635 | Romanian Permanent Resident | `OTHER_ID` | `ROM_PERM_RES` | - | `RO` |
| 1636 | Government Gazette Number | payload `GOVERNMENT_GAZETTE_NUMBER` | - | - | - |
| 1638 | Fiscal Code | `TAX_ID` | - | - | - |
| 1639 | Pilot License Number | `OTHER_ID` | `licenseNumber` | - | - |
| 1642 | Romanian C.R. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `RO` |
| 1643 | Folio Mercantil No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 1644 | Istanbul Chamber of Comm. No. | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `TR` |
| 1645 | Turkish Identification Number | `NATIONAL_ID` | `TCKN` | - | `TR` |
| 1646 | Romanian Tax Registration | `TAX_ID` | - | - | `RO` |
| 1647 | Stateless Person Passport | `PASSPORT` | - | - | - |
| 1648 | Stateless Person ID Card | `OTHER_ID` | `STATELESS_ID` | - | - |
| 1649 | Refugee ID Card | `OTHER_ID` | `REFUGEE_ID` | - | - |
| 1712 | I.F.E. | `OTHER_ID` | `IFE` | - | - |
| 1719 | Branch Unit Number | `OTHER_ID` | `branchUnitNumber` | - | - |
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
| 1812 | MSB Registration Number | `OTHER_ID` | `licenseNumber` | - | - |
| 1835 | File Number | `OTHER_ID` | `fileNumber` | - | - |
| 1854 | C.U.I. | `NATIONAL_ID` | `CUI` | - | - |
| 1891 | Seafarer's Identification Document | `OTHER_ID` | `SEAFARER_ID` | - | - |
| 2001 | Unified Social Credit Code (USCC) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | `CN` |
| 2067 | Central Registration System Number | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 2121 | Economic Register Number (CBLS) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | - | - |
| 2158 | Trademark number | `OTHER_ID` | `trademarkNumber` | - | - |
| 2159 | Permit Number | `OTHER_ID` | `permitNumber` | - | - |
| 2728 | Military Registration Number | `OTHER_ID` | `militaryRegistrationNumber` | - | - |
| 2772 | Russian State Individual Business Registration Number Pattern (OGRNIP) | `NATIONAL_ID` | `REGISTRATION_NUMBER` | `OGRN` | `RU` |
| 2790 | Global Intermediary Identification Number | `OTHER_ID` | `giiNumber` | - | - |

<!-- END GENERATED ID TABLE -->

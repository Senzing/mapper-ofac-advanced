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
2. **OpenSanctions `doc_types` crosswalk** (FtM property names) — the **type-value vocabulary** we standardize
   on, so OFAC and OpenSanctions emit identical strings (`registrationNumber`, `taxNumber`, `vatCode`,
   `ogrnCode`, and the raw scheme label for person national-ids, e.g. `C.U.R.P.`).
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
|---|---|---|---|
| Business registration | Registration Number, Company Number, Commercial Registry Number, Enterprise Number, C.I.N., Registered Charity No., USCC/USCCC … | `NATIONAL_ID` | `registrationNumber` |
| Russian individual-entrepreneur reg | OGRNIP | `NATIONAL_ID` | `ogrnCode` |
| LEI | Legal Entity Number, LE Number | `LEI_NUMBER` | — |
| **Tax numbers** | R.F.C., N.I.F., C.I.F., RTN, RUC #, RIF #, US FEIN, Italian Fiscal Code, Fiscal Code, Tax ID No., Paraguayan tax, Romanian Tax Registration, **NIT #**, **NUIT** | `TAX_ID` | `taxNumber` |
| **VAT** | V.A.T. Number | `TAX_ID` | `vatCode` |
| Person national-id schemes | C.U.R.P., D.N.I., Cedula No., C.U.I.P., C.U.I., **C.U.I.T.**, N.I.E., CNP, Turkish/UAE/Citizen's Card, Tazkira, Numero de Identidad | `NATIONAL_ID` | OpenSanctions raw label (e.g. `C.U.R.P.`) |
| Generic national-id (R1) | National ID No., Identification Number, Personal/Federal ID Card, Kenyan/Bosnian/Moroccan ID No., National Foreign ID Number | `NATIONAL_ID` | *(untyped)* |
| Passport / SSN | Passport variants, SSN | `PASSPORT` / `SSN` | — |
| Licenses / permits / documents (R2) | Trade/Tourism/Pilot/MSB/Afghan-MSP License, Trademark, Permit, Serial No., Travel Document, Visa, Immigration, Refugee/Stateless/Seafarer ID, Residency, File Number, **voter/electoral** (Credencial electoral, I.F.E., Electoral Registry No.) | `OTHER_ID` | `licenseNumber` / `permitNumber` / `trademarkNumber` / `serialNumber` / `fileNumber` / raw |
| Vessel / aircraft / GIIN / MMSI | IMO, Aircraft Serial, GIIN, MMSI | `OTHER_ID` | `imoNumber` / `serialNumber` / `giiNumber` / `MMSI` |
| Driver's license | Driver's License No. | `DRIVERS_LICENSE` | — |
| **Not identifiers (R4) → payload** | **Chinese Commercial Code** (a Chinese-name telegraph encoding, shared by same-named entities), **Government Gazette Number** (a publication reference, shared per issue) | *(payload)* `CHINESE_COMMERCIAL_CODE` / `GOVERNMENT_GAZETTE_NUMBER` | — |

### Tax deep-dive

`TAX_ID` in the config is **untyped without `TAX_ID_TYPE`** — one shared namespace per country. With
`TAX_ID_TYPE` (below), distinct schemes stay separate: national tax → `taxNumber`, VAT → `vatCode`. **CUIT** is
Argentina's universal identifier (used well beyond tax) — OpenSanctions *and* Sayari place it on `NATIONAL_ID`,
so it stays there. **NIT / NUIT / Italian Fiscal Code** are tax ids → `TAX_ID` (owner-confirmed; diverges from
OpenSanctions' `idNumber`/`OTHER_ID` bucketing for those, which is arguably an upstream OpenSanctions defect).

## Config dependency — `TAX_ID_TYPE`

`TAX_ID_TYPE` requires the `ID_TYPE` element on the `TAX_ID` feature **and in its comparison call** (the
comparison-call row is what actually makes the type *scored* — the feature-BOM element alone only stores it).
**Senzing 4.4's default config ships it.** For engines whose config predates that (e.g. 4.4.0-26224, whose
template has `NATIONAL_ID ID_TYPE` scored but nothing on `TAX_ID`), apply `src/ofac_config_updates.gtc`
(`sz_configtool -f …`) — three commands mirroring `NATIONAL_ID`: `addElementToFeature`,
`addComparisonCallElement` (scores it), and `addAttribute`. Idempotent. Verified live: two records sharing a
tax number but typed `taxNumber` vs `vatCode` are held apart only once the comparison-call element is present.

## Cross-source alignment & validation

- **Alignment** is realized when OFAC, OpenSanctions, and Sayari emit the same feature + type for the same
  id. This mapper + the OpenSanctions exporter now agree on `registrationNumber` / `taxNumber` / `vatCode` /
  `ogrnCode` / person raw labels. (The Sayari mapper is a separate future project.)
- **Pure relabel:** on the full Treasury SDN (19,024 records) the id-NUMBER multiset is unchanged before/after
  — only feature/type labels move; 12 Chinese Commercial Codes + 349 Government Gazette Numbers move to payload.
- **A/B gate:** run a ground-truth entity-resolution A/B (baseline vs normalized) before the change reaches the
  deployed demo; re-mapping OFAC should co-land with the OpenSanctions peer normalization so cross-source ids
  align rather than detach.

## Open / borderline (kept conservative — promote only with confirmation)

- Voter/electoral → `OTHER_ID` (Sayari's reviewed call; voter rolls are not one-per-lifetime-unique).
- Military Registration / Cartilla Militar / Birth Certificate → `OTHER_ID` (uniqueness/one-per-lifetime
  uncertain; promote to `NATIONAL_ID` only if confirmed).
- Trademark / Permit / Serial No. → `OTHER_ID` (many-per-entity; OpenSanctions makes them exclusive
  `registrationNumber`, which would manufacture denials).

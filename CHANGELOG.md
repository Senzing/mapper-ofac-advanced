# Changelog

All notable changes to this project will be documented in this file.

The changelog format is based on [Keep a Changelog] and [CommonMark].
This project adheres to [Semantic Versioning].

## [Unreleased]

Behavior changes to the emitted JSON; re-map and reload to pick them up.

### Changed in Unreleased

- Identifier types normalized (see `docs/id-type-normalization.md`): the business-registration family is
  `NATIONAL_ID_TYPE=REGISTRATION_NUMBER`; tax numbers move to `TAX_ID` (no type for a generic tax number,
  `TAX_ID_TYPE=VAT` for VAT); LEIs to `LEI_NUMBER`; named person schemes use upper-case tokens (`CURP`, `DNI`,
  ...); generic national-id labels are untyped; `UK` country constants are `GB`.
- Names: a person's name is `NAME_FULL`; an organization, vessel or aircraft name is `NAME_ORG`; a record with
  no `RECORD_TYPE` gets `NAME_FULL` (Entity Specification attribute for an unknown name type).
- Dates (`DATE_OF_BIRTH`, `REGISTRATION_DATE`, `AIRCRAFT_MANUFACTURE_DATE`, EO 14024 dates) are read from the
  feed's `DatePeriod` at the precision its bounds state: a year-only period is `YYYY` and a month-only period
  `YYYY-MM` (not a fabricated `-01-01`). A period spanning several years emits every year in it as its own
  date (`1955 to 1957` -> three `DATE_OF_BIRTH` features 1955, 1956, 1957; same for `REGISTRATION_DATE`;
  a payload date such as `AIRCRAFT_MANUFACTURE_DATE` holds the years joined with `"; "`). Spans longer than
  20 years (`MAX_DATE_SPAN_YEARS`; the longest in the 2026-10-02 file is 9) emit only their two bound years.
- `NATIONALITY` / `CITIZENSHIP` come from the referenced Location's text (its Primary value).
- Chinese Commercial Code and Government Gazette Number are top-level payload attributes, not identifiers;
  several values for one party are joined with `"; "`.

- Identifiers conform to the Senzing identifier crosswalk vocabulary: every record carries `OFAC_ID` (the
  SDN uid, also its `RECORD_ID`); IMO, MMSI, call signs and ISINs use the `IMO_NUMBER`, `MMSI_NUMBER`,
  `CALL_SIGN` and `ISIN_NUMBER` features; `OTHER_ID` types are the canonical tokens (`LICENSE`, `PERMIT`,
  `SWIFT_BIC`, `GIIN`, `AIRCRAFT_MSN`, `AIRCRAFT_TAIL`, `ELECTORAL`, `MILITARY_SERVICE`, `RESIDENCY`,
  `IMMIGRATION`, `SCAC`, `CAAT`, `CRYPTO_<ASSET>`); CUIT is `TAX_ID_TYPE=CUIT` (AR); a Russian "Tax ID No." is
  `TAX_ID_TYPE=INN`; OGRNIP's subtype is `OGRNIP`; File Number is a `REGISTRATION_NUMBER`.
- Values that do not identify the entity are top-level payload, not identifiers: vessel flags, Title (a
  position held; was a separate `NAME_PREFIX` that fragmented the name), UN/LOCODE, equity ticker, MICEX code,
  PAIPA text, Serial No., Trademark number, Branch Unit Number, Entity Code.
- `src/ofac_config_updates.gtc` now registers the custom features (`OFAC_ID`, `IMO_NUMBER`, `MMSI_NUMBER`,
  `CALL_SIGN`, `ISIN_NUMBER`) and `NATIONAL_ID_SUBTYPE`; the `TAX_ID_TYPE` commands for older engine configs
  moved to `src/ofac_config_updates_tax_id_type.gtc`.

### Removed in Unreleased

- The unreachable person-name part parsing (`NAME_PART_MAP`, codes 1480-1484; the feed's name parts carry no
  `NamePartTypeID`, so it never fired). Persons are `NAME_FULL` only; names are not parsed. Genuine typed parts,
  if a feed provides them, belong in a separate name feature, never mixed with `NAME_FULL` in one feature.

### Fixed in Unreleased

- Payload features repeated on one party with different values (e.g. `SECONDARY_SANCTIONS_RISK`,
  `EO_13846_INFO`, `ORGANIZATION_TYPE`) kept only the first value. Every distinct value is now kept, in
  document order, joined with `"; "` (731 records / 991 values on the 2026-10-02 SDN file).

### Added in Unreleased

- `src/ofac_config_updates.gtc`: adds `TAX_ID_TYPE` to a Senzing config that predates it
  (`sz_configtool -f src/ofac_config_updates.gtc`).
- `NATIONAL_ID_SUBTYPE=OGRN` for OGRNIP. The attribute is not in the default config yet; until it is
  registered (future default config, or by the user) the value is ignored at load.
- pytest suite under `tests/`.

## [1.0.0] - yyyy-mm-dd

### Added to 1.0.0

- Initial content

[CommonMark]: https://commonmark.org/
[Keep a Changelog]: https://keepachangelog.com/
[Semantic Versioning]: https://semver.org/

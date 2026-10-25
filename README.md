# mapper-ofac-advanced

Strict-mode ETL that converts the U.S. Treasury OFAC Advanced XML feed into
Senzing-compatible JSON records.

## Overview

This mapper ingests the OFAC "Advanced" sanctions feed and emits JSONL that
conforms to Senzing's strict-mode schema. It implements the mapping rules laid
out in `mapping_proposal.md`, including full coverage of the 72 `FeatureType`
codes and 86 `IDRegDocType` codes provided with the data set. The transformer:

- Parses OFAC Advanced XML using `lxml` when available (falls back to the
  Python standard library parser).
- Applies deterministic lookups from the static tables under `src/config/` for
  feature and identity-document handling.
- Preserves relationships between parties, builds sanctions metadata, and
  emits a JSON record per entity.
- Produces JSON that is compatible with `lint_senzing_json.py` in strict mode.

## Repository Layout

- `src/ofac_advanced_mapper.py` - main transformer and CLI entry point.
- `src/config/feature_mappings.py` - map of OFAC feature codes to Senzing
  feature definitions.
- `src/config/id_doc_mappings.py` - map of identity document types.
- `input/` - optional location for raw OFAC XML (sample file: `sdn_advanced.xml`).
- `output/` - suggested destination for generated JSONL.

## Requirements

- Python 3.9 or newer.
- Optional: `lxml` for faster XML parsing (`pip install lxml`).

No runtime packages beyond the Python standard library are required. Development
tooling such as `black`, `flake8`, and `pytest` are listed in
`development-requirements.txt`.

## Quick Start

1. (Optional) Create a virtual environment and install development tools:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r development-requirements.txt
   ```

2. Obtain the latest OFAC Advanced XML (`sdn_advanced.xml`) from the Treasury
   Sanctions List Service and place it under `input/` or another directory of
   your choice.

3. Run the transformer:

   ```bash
   python src/ofac_advanced_mapper.py input/sdn_advanced.xml --output-jsonl output/ofac_strict.jsonl
   ```

   The command writes one JSON object per line to the specified destination and
   logs processing statistics on completion.

4. Load the emitted JSONL into Senzing or feed it into downstream tooling as
   needed. Senzing config prerequisite: the mapper emits `TAX_ID_TYPE`. Senzing
   4.4's default config has it; for an engine whose config predates it, apply
   `sz_configtool -f src/ofac_config_updates.gtc` before loading, or
   `TAX_ID_TYPE` is dropped at load. The mapper also emits
   `NATIONAL_ID_SUBTYPE` for OGRNIP; that attribute is not in the default
   config yet and is ignored at load until it is registered (a future default
   config, or add it to your own config).

## Configuration and Mapping

Feature-family and identity-document handling is driven by the static lookup
tables in `src/config/`. Each file exports dictionaries keyed by OFAC code
values with metadata describing the Senzing feature payload. Update these files
when OFAC introduces new codes or when mapping rules evolve.

The transformer also builds lookups directly from the XML (country codes,
relationship types, sanctions measures, etc.) to ensure the emitted JSON uses
the latest values packaged with the feed.

Identifier types are documented in `docs/id-type-normalization.md`, whose
per-type table is generated from `src/config/id_doc_mappings.py`
(`python tests/test_docs_id_table.py`). In that file `"group"` is descriptive
only; `"payload": True` routes a document type to a top-level payload attribute.

Top-level payload attributes (from payload features or payload document types)
keep every distinct value a party carries, in document order, joined with
`"; "`; identical repeats collapse to one value and a single value is written
unchanged.

Dates: the feed states every date as a `DatePeriod`. The mapper emits it at the
precision the bounds state: an exact date as `YYYY-MM-DD`, a whole month as
`YYYY-MM`, a whole year (or two dates within one year) as `YYYY`. A period
spanning several years ("1955 to 1957") emits each year as its own date feature
(one `DATE_OF_BIRTH` / `REGISTRATION_DATE` per year; a top-level payload date
such as `AIRCRAFT_MANUFACTURE_DATE` holds the years joined with `"; "`). A span
longer than `MAX_DATE_SPAN_YEARS` (20) is treated as malformed: only its two
bound years are emitted and a warning is logged.

## Development Notes

- Formatting: the project targets Black's default style with a 120-character
  line length (`pyproject.toml`).
- Linting: run `flake8 src` to surface warnings such as unused imports or
  variables.
- Testing: `python -m pytest --cov=src tests` (coverage target 80%).
- Editing: adhere to ASCII unless a file already uses non-ASCII characters.

## Troubleshooting

- **Memory or performance issues** - install `lxml` to leverage C-accelerated
  parsing. The fallback ElementTree parser is slower on large feeds.
- **Malformed JSON output** - ensure you are using the latest mapping tables
  and that the source XML is the official OFAC Advanced feed without
  modification.
- **Schema validation errors in Senzing** - run the generated file through
  `lint_senzing_json.py` (shipped alongside the mapper) to identify formatting
  gaps.

## Contributing

Bug reports and pull requests are welcome. Please adhere to the coding and
formatting guidance above and include updates to the mapping documentation when
introducing new features.

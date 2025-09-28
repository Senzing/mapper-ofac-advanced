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
   needed.

## Configuration and Mapping

Feature-family and identity-document handling is driven by the static lookup
tables in `src/config/`. Each file exports dictionaries keyed by OFAC code
values with metadata describing the Senzing feature payload. Update these files
when OFAC introduces new codes or when mapping rules evolve.

The transformer also builds lookups directly from the XML (country codes,
relationship types, sanctions measures, etc.) to ensure the emitted JSON uses
the latest values packaged with the feed.

## Development Notes

- Formatting: the project targets Black's default style with a 120-character
  line length (`pyproject.toml`).
- Linting: run `flake8 src` to surface warnings such as unused imports or
  variables.
- Testing: no automated tests ship with this repository yet; add pytest suites
  under `tests/` if you expand the functionality.
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

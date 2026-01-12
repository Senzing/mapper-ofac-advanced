# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a strict-mode ETL transformer that converts U.S. Treasury OFAC Advanced XML sanctions data into Senzing-compatible JSONL records. The mapper parses OFAC's XML feed, applies deterministic mappings for 72 FeatureType codes and 86 IDRegDocType codes, and outputs records conforming to Senzing's strict-mode schema.

## Commands

### Run the transformer

```bash
python src/ofac_advanced_mapper.py input/sdn_advanced.xml --output-jsonl output/ofac_strict.jsonl
```

### Install development dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install --group all .
```

### Linting

```bash
# Run pylint on all Python files
pylint $(git ls-files '*.py' ':!:docs/source/*')

# Run flake8
flake8 src

# Run mypy type checking
mypy src
```

### Formatting

```bash
black src
isort src
```

## Architecture

The transformer follows a single-pass processing model:

1. **Load phase** (`StrictOFACTransformer.load()`): Parses XML and builds lookup tables for countries, feature types, document types, relationships, sanctions entries, and locations from reference data embedded in the feed.

2. **Transform phase** (`StrictOFACTransformer.transform()`): Iterates through `DistinctParty` elements, converting each to a Senzing JSON record with:
   - Names extracted from `Alias` elements with type classification (PRIMARY, AKA, FKA, NKA)
   - Features mapped via `src/config/feature_mappings.py` (72 OFAC feature codes → Senzing attributes)
   - Identity documents mapped via `src/config/id_doc_mappings.py` (86 document types → Senzing identifiers)
   - Relationship anchors and pointers for inter-entity connections
   - Sanctions metadata (list, date, programs, type) from `SanctionsEntry` elements

### Key Files

- `src/ofac_advanced_mapper.py` - Main transformer class and CLI entry point
- `src/config/feature_mappings.py` - Static map of OFAC FeatureTypeID → Senzing feature definitions
- `src/config/id_doc_mappings.py` - Static map of OFAC IDRegDocTypeID → Senzing identifier definitions

### Mapping Structure

Both mapping files use a consistent structure:

```python
{
    <ofac_id>: {
        "name": "<human readable name>",
        "section": "<category>",  # or "group" for id_doc
        "instructions": [(<senzing_attr>, <constant_or_None>), ...]
    }
}
```

When `instructions` contains `None` as the second tuple element, the value is extracted dynamically from the XML. Constants provide fixed values (e.g., country codes, type identifiers).

## Code Style

- **Line length**: 120 characters (Black default with custom line-length)
- **Formatter**: Black with isort (profile: black)
- **Python**: 3.10+ required
- **XML parsing**: Uses `lxml` when available for performance, falls back to stdlib `xml.etree.ElementTree`

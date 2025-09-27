#!/usr/bin/env python3
"""Strict-mode OFAC Advanced XML → Senzing JSON transformer.

This implementation follows the mapping described in ``mapping_proposal.md``
and uses pre-generated lookup tables under ``config/`` for feature and
identity-document handling.  The goal is 100% coverage of the 72 FeatureType
values and 86 IDRegDocType values enumerated in the strict-mode deliverables.

The transformer loads the OFAC Advanced XML, applies deterministic mappings to
Senzing JSON feature families, preserves relationships, and emits JSONL output.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:  # Prefer lxml for speed; fall back to stdlib if unavailable.
    from lxml import etree as ET  # type: ignore

    LXML_AVAILABLE = True
except ImportError:  # pragma: no cover - executed only when lxml missing.
    import xml.etree.ElementTree as ET  # type: ignore

    LXML_AVAILABLE = False

from config.feature_mappings import FEATURE_MAPPINGS
from config.id_doc_mappings import ID_DOC_MAPPINGS

NS = {
    "ofac": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/ADVANCED_XML",
    "un": "http://www.un.org/sanctions/1.0",
}

PARTY_SUBTYPE_TO_RECORD_TYPE = {
    1: "VESSEL",
    2: "AIRCRAFT",
    3: "ORGANIZATION",
    4: "PERSON",
}

ALIAS_TYPE_TO_NAME_TYPE = {
    1400: "AKA",
    1401: "FKA",
    1402: "NKA",
    1403: "PRIMARY",
}

RELATIONSHIP_ROLE_MAP = {
    15003: "CONTROLLED_BY",
    15001: "SUPPORTS",
    15002: "AGENT_OF",
    92122: "PROPERTY_OF",
    91725: "LEADER_OF",
    15004: "FAMILY_OF",
    92019: "OWNS_CONTROLS",
    91422: "SIGNIFICANT_ROLE",
    91900: "EXECUTIVE_OF",
    1555: "ASSOCIATE_OF",
}

NAME_PART_MAP = {
    "1480": "NAME_FIRST",
    "1481": "NAME_LAST",
    "1482": "NAME_MIDDLE",
    "1483": "NAME_SUFFIX",
    "1484": "NAME_PREFIX",
}

COUNTRY_ATTRS = {
    "CITIZENSHIP",
    "NATIONALITY",
    "REGISTRATION_COUNTRY",
    "PASSPORT_COUNTRY",
    "TAX_ID_COUNTRY",
    "NATIONAL_ID_COUNTRY",
    "OTHER_ID_COUNTRY",
}

DATE_ATTRS = {
    "DATE_OF_BIRTH",
    "REGISTRATION_DATE",
    "SANCTIONS_DATE",
    "EO_14024_D2_LISTING_DATE",
    "EO_14024_D2_EFFECTIVE_DATE",
    "EO_14024_D3_LISTING_DATE",
    "EO_14024_D3_EFFECTIVE_DATE",
    "AIRCRAFT_MANUFACTURE_DATE",
}

EMAIL_ATTRS = {"EMAIL_ADDRESS"}


class StrictOFACTransformer:
    """Transform OFAC Advanced XML into strict Senzing JSON."""

    def __init__(self, *, data_source: str = "OFAC_ADVANCED") -> None:
        self.data_source = data_source

        self.country_lookup: Dict[str, str] = {}
        self.feature_type_lookup: Dict[str, str] = {}
        self.id_reg_doc_type_lookup: Dict[str, str] = {}
        self.relation_type_lookup: Dict[str, str] = {}
        self.sanctions_type_lookup: Dict[str, str] = {}
        self.detail_reference_lookup: Dict[str, str] = {}
        self.list_lookup: Dict[str, str] = {}

        self.profile_id_to_fixed_ref: Dict[str, str] = {}
        self.profile_elements: Dict[str, ET.Element] = {}
        self.relationships_by_profile: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        self.identity_documents: Dict[str, List[ET.Element]] = defaultdict(list)
        self.id_doc_name_mappings: Dict[str, Dict[str, object]] = {}
        self.location_lookup: Dict[str, ET.Element] = {}
        self.sanctions_entries_by_profile: Dict[str, List[ET.Element]] = defaultdict(list)
        self.primary_name_cache: Dict[str, str] = {}

        self.root: Optional[ET.Element] = None

        self._warned_feature_ids: set[int] = set()
        self._warned_doc_ids: set[int] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def load(self, xml_path: Path) -> None:
        """Parse XML and build lookup tables."""

        parser = None
        if LXML_AVAILABLE:
            parser = ET.XMLParser(resolve_entities=False)
        elif hasattr(ET, "XMLParser"):
            parser = ET.XMLParser()

        tree = ET.parse(str(xml_path), parser=parser) if parser is not None else ET.parse(str(xml_path))
        self.root = tree.getroot()

        self._build_reference_lookups()
        self._build_profile_lookup()
        self._build_relationship_lookup()
        self._build_location_lookup()
        self._build_identity_document_lookup()
        self._build_sanctions_entry_lookup()
        self._build_id_doc_name_lookup()

    def transform(
        self,
        *,
        output_jsonl: Path,
    ) -> Dict[str, int]:
        """Transform loaded XML to JSONL, returning processing statistics."""

        if self.root is None:
            raise RuntimeError("Transformer not initialized; call load() first")

        stats = {
            "processed": 0,
            "emitted": 0,
            "skipped": 0,
            "features": 0,
            "relationships": 0,
            "identifiers": 0,
        }

        start_time = datetime.now()

        with output_jsonl.open("w", encoding="utf-8") as jsonl_file:
            parties = self.root.findall(".//ofac:DistinctParty", NS)

            for party in parties:
                stats["processed"] += 1

                record = self._transform_party(party)
                if record is None:
                    stats["skipped"] += 1
                    continue

                jsonl_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                stats["emitted"] += 1
                stats["features"] += len(record["FEATURES"])
                stats["relationships"] += sum(1 for f in record["FEATURES"] if "REL_POINTER_KEY" in f)
                stats["identifiers"] += sum(1 for f in record["FEATURES"] if any(k.endswith("_NUMBER") for k in f))

        duration = datetime.now() - start_time
        logging.info(
            "Completed %s entities in %s (emitted %s records)",
            f"{stats['processed']:,}",
            str(duration).split(".")[0],
            f"{stats['emitted']:,}",
        )

        return stats

    # ------------------------------------------------------------------
    # Data loading helpers
    # ------------------------------------------------------------------
    def _build_reference_lookups(self) -> None:
        if self.root is None:
            return

        for country in list(self.root.findall(".//ofac:Country", NS)) + list(self.root.findall(".//un:Country", NS)):
            country_id = country.get("ID")
            iso2 = country.get("ISO2")
            if country_id and iso2:
                self.country_lookup[country_id] = iso2

        for feature_type in list(self.root.findall(".//ofac:FeatureType", NS)) + list(
            self.root.findall(".//un:FeatureType", NS)
        ):
            type_id = feature_type.get("ID")
            if type_id and feature_type.text:
                self.feature_type_lookup[type_id] = feature_type.text.strip()

        for doc_type in list(self.root.findall(".//ofac:IDRegDocType", NS)) + list(
            self.root.findall(".//un:IDRegDocType", NS)
        ):
            type_id = doc_type.get("ID")
            if type_id and doc_type.text:
                self.id_reg_doc_type_lookup[type_id] = doc_type.text.strip()

        for rel_type in list(self.root.findall(".//ofac:RelationType", NS)) + list(
            self.root.findall(".//un:RelationType", NS)
        ):
            type_id = rel_type.get("ID")
            if type_id and rel_type.text:
                self.relation_type_lookup[type_id] = rel_type.text.strip()

        for sanctions_type in list(self.root.findall(".//ofac:SanctionsType", NS)) + list(
            self.root.findall(".//un:SanctionsType", NS)
        ):
            type_id = sanctions_type.get("ID")
            if type_id and sanctions_type.text:
                self.sanctions_type_lookup[type_id] = sanctions_type.text.strip()

        for detail_ref in list(self.root.findall(".//ofac:DetailReference", NS)) + list(
            self.root.findall(".//un:DetailReference", NS)
        ):
            ref_id = detail_ref.get("ID")
            if ref_id and detail_ref.text:
                self.detail_reference_lookup[ref_id] = detail_ref.text.strip()

        for list_elem in list(self.root.findall(".//ofac:List", NS)) + list(self.root.findall(".//un:List", NS)):
            list_id = list_elem.get("ID")
            if list_id and list_elem.text:
                self.list_lookup[list_id] = list_elem.text.strip()

    def _build_profile_lookup(self) -> None:
        if self.root is None:
            return

        for party in self.root.findall(".//ofac:DistinctParty", NS):
            profile = party.find("ofac:Profile", NS)
            fixed_ref = party.get("FixedRef")
            if profile is None or not fixed_ref:
                continue
            profile_id = profile.get("ID")
            if profile_id:
                self.profile_id_to_fixed_ref[profile_id] = fixed_ref
                self.profile_elements[profile_id] = party

    def _build_relationship_lookup(self) -> None:
        if self.root is None:
            return

        for rel in self.root.findall(".//ofac:ProfileRelationship", NS):
            from_profile = rel.get("From-ProfileID")
            if not from_profile:
                continue
            rel_info = {
                "to": rel.get("To-ProfileID"),
                "type": rel.get("RelationTypeID"),
                "quality": rel.get("RelationQualityID"),
                "former": rel.get("Former"),
            }
            self.relationships_by_profile[from_profile].append(rel_info)

    def _build_identity_document_lookup(self) -> None:
        if self.root is None:
            return

        for doc in self.root.findall(".//ofac:IDRegDocument", NS):
            identity_id = doc.get("IdentityID")
            if identity_id:
                self.identity_documents[identity_id].append(doc)

    def _build_sanctions_entry_lookup(self) -> None:
        if self.root is None:
            return

        for entry in self.root.findall(".//ofac:SanctionsEntry", NS):
            profile_id = entry.get("ProfileID")
            if profile_id:
                self.sanctions_entries_by_profile[profile_id].append(entry)

    def _build_location_lookup(self) -> None:
        if self.root is None:
            return

        for location in self.root.findall(".//ofac:Location", NS):
            loc_id = location.get("ID")
            if loc_id:
                self.location_lookup[loc_id] = location

    def _build_id_doc_name_lookup(self) -> None:
        # Build normalized-name lookup from static mapping table for fallback by name.
        for entry in ID_DOC_MAPPINGS.values():
            normalized = self._sanitize_identifier_name(entry["name"]) if entry.get("name") else None
            if normalized:
                self.id_doc_name_mappings[normalized] = entry

    # ------------------------------------------------------------------
    # Entity transformation
    # ------------------------------------------------------------------
    def _transform_party(self, party: ET.Element) -> Optional[Dict]:
        fixed_ref = party.get("FixedRef")
        if not fixed_ref:
            return None

        record: Dict[str, object] = {
            "DATA_SOURCE": self.data_source,
            "RECORD_ID": fixed_ref,
            "FEATURES": [],
        }

        profile = party.find("ofac:Profile", NS)
        if profile is None:
            return None

        self._add_record_type(record, profile)
        self._add_names(record, party)

        max_reliability = self._add_features(record, party)
        self._add_identity_documents(record, party)

        # Relationship anchor
        record["FEATURES"].append(
            {
                "REL_ANCHOR_DOMAIN": self.data_source,
                "REL_ANCHOR_KEY": fixed_ref,
            }
        )

        self._add_relationships(record, profile)
        self._add_payload_attributes(record, profile, max_reliability)

        return record

    def _add_record_type(self, record: Dict, profile: ET.Element) -> None:
        subtype = profile.get("PartySubTypeID")
        if not subtype:
            return
        try:
            subtype_code = int(subtype)
        except ValueError:
            return
        record_type = PARTY_SUBTYPE_TO_RECORD_TYPE.get(subtype_code)
        if record_type:
            record["FEATURES"].append({"RECORD_TYPE": record_type})

    def _add_names(self, record: Dict, party: ET.Element) -> None:
        for alias in party.findall(".//ofac:Alias", NS):
            is_primary = alias.get("Primary", "").lower() == "true"
            alias_type_id = alias.get("AliasTypeID")

            for documented_name in alias.findall("ofac:DocumentedName", NS):
                name_feature: Dict[str, str] = {}
                parts: List[str] = []

                for part in documented_name.findall("ofac:DocumentedNamePart", NS):
                    value_elem = part.find("ofac:NamePartValue", NS)
                    text = self._clean_text(value_elem.text if value_elem is not None else None)
                    if not text:
                        continue
                    parts.append(text)
                    name_attr = NAME_PART_MAP.get(part.get("NamePartTypeID", ""))
                    if name_attr and name_attr not in name_feature:
                        name_feature[name_attr] = text

                if not parts:
                    continue

                name_feature["NAME_FULL"] = " ".join(parts)

                if is_primary:
                    name_feature["NAME_TYPE"] = "PRIMARY"
                elif alias_type_id:
                    try:
                        mapped = ALIAS_TYPE_TO_NAME_TYPE[int(alias_type_id)]
                        name_feature["NAME_TYPE"] = mapped
                    except (ValueError, KeyError):
                        name_feature["NAME_TYPE"] = "AKA"
                else:
                    name_feature["NAME_TYPE"] = "AKA"

                record["FEATURES"].append(name_feature)

    def _add_features(self, record: Dict, party: ET.Element) -> Optional[int]:
        max_reliability: Optional[int] = None

        for feature in party.findall(".//ofac:Feature", NS):
            feature_type = feature.get("FeatureTypeID")
            if feature_type is None:
                continue

            try:
                type_id = int(feature_type)
            except ValueError:
                continue

            reliability = self._extract_reliability(feature)
            if reliability is not None:
                max_reliability = reliability if max_reliability is None else max(max_reliability, reliability)

            mapping_info = FEATURE_MAPPINGS.get(type_id)
            if mapping_info is None:
                fallback = self._fallback_other_id_feature(type_id, feature)
                if fallback:
                    record["FEATURES"].append(fallback)
                elif type_id not in self._warned_feature_ids:
                    logging.warning("No mapping for FeatureTypeID=%s", type_id)
                    self._warned_feature_ids.add(type_id)
                continue

            target = "payload" if "PAYLOAD" in mapping_info["section"].upper() else "feature"
            if mapping_info["section"].startswith("DESCRIPTIVE"):
                target = "payload"

            values = self._build_attribute_dict(feature, mapping_info["instructions"])
            if not values:
                continue

            if target == "feature":
                record["FEATURES"].append(values)
            else:
                for key, value in values.items():
                    if value in (None, ""):
                        continue
                    if key not in record:
                        record[key] = value
                    elif record[key] != value:
                        # In the unlikely case of conflicting payload values, prefer first.
                        continue

        return max_reliability

    def _build_attribute_dict(
        self, feature: ET.Element, instructions: Iterable[Tuple[str, Optional[str]]]
    ) -> Optional[Dict[str, object]]:
        attr_values: Dict[str, object] = {}

        text_value = self._extract_feature_text(feature)
        date_value = self._extract_feature_date(feature)
        country_value = self._extract_country_code(feature)
        address_details = self._extract_address_details(feature)
        address_value = address_details.get("full") if address_details else None
        if not country_value and address_details and address_details.get("country"):
            country_value = address_details["country"]

        for attr, constant in instructions:
            if constant is not None:
                attr_values[attr] = constant
                continue

            if attr in DATE_ATTRS:
                if date_value:
                    attr_values[attr] = date_value
            elif attr in COUNTRY_ATTRS:
                if country_value:
                    attr_values[attr] = country_value
            elif attr in EMAIL_ATTRS:
                if text_value:
                    attr_values[attr] = text_value.lower()
            elif attr == "ADDR_FULL":
                if address_value:
                    attr_values[attr] = address_value
                elif text_value:
                    attr_values[attr] = text_value
            elif attr == "REGISTRATION_COUNTRY":
                if country_value:
                    attr_values[attr] = country_value
            else:
                if text_value:
                    attr_values[attr] = text_value

        # Ensure required dynamic attributes present.
        for attr, constant in instructions:
            if constant is not None:
                continue
            if attr not in attr_values:
                return None

        return attr_values or None

    def _fallback_other_id_feature(self, feature_type_id: int, feature: ET.Element) -> Optional[Dict[str, str]]:
        text_value = self._extract_feature_text(feature)
        if not text_value:
            return None
        feature_name = self.feature_type_lookup.get(str(feature_type_id), f"FEATURE_{feature_type_id}")
        type_value = re.sub(r"[^A-Z0-9]+", "_", feature_name.upper()).strip("_") or feature_name
        return {
            "OTHER_ID_TYPE": type_value,
            "OTHER_ID_NUMBER": text_value,
        }

    def _add_identity_documents(self, record: Dict, party: ET.Element) -> None:
        identities = party.findall(".//ofac:Identity", NS)
        for identity in identities:
            identity_id = identity.get("ID")
            if not identity_id:
                continue

            for doc in self.identity_documents.get(identity_id, []):
                doc_type = doc.get("IDRegDocTypeID")
                if doc_type is None:
                    continue

                try:
                    doc_type_id = int(doc_type)
                except ValueError:
                    continue

                doc_number = self._extract_identity_number(doc)
                if not doc_number:
                    continue

                mapping = ID_DOC_MAPPINGS.get(doc_type_id)
                if mapping is None:
                    doc_type_name = self.id_reg_doc_type_lookup.get(str(doc_type_id))
                    if doc_type_name:
                        normalized_name = self._sanitize_identifier_name(doc_type_name)
                        mapping = self.id_doc_name_mappings.get(normalized_name)
                if mapping is None:
                    if doc_type_id not in self._warned_doc_ids:
                        logging.warning("No mapping for IDRegDocTypeID=%s", doc_type_id)
                        self._warned_doc_ids.add(doc_type_id)
                    fallback = {
                        "OTHER_ID_TYPE": self._sanitize_identifier_name(
                            self.id_reg_doc_type_lookup.get(str(doc_type_id), f"DOCTYPE_{doc_type_id}")
                        ),
                        "OTHER_ID_NUMBER": doc_number,
                    }
                    country = self._extract_identity_country(doc)
                    if country:
                        fallback["OTHER_ID_COUNTRY"] = country
                    record["FEATURES"].append(fallback)
                    continue

                feature_obj: Dict[str, object] = {}
                country = self._extract_identity_country(doc)

                for attr, constant in mapping["instructions"]:
                    if constant is not None:
                        feature_obj[attr] = constant
                    elif attr.endswith("_NUMBER"):
                        feature_obj[attr] = doc_number
                    elif attr.endswith("_COUNTRY"):
                        if country:
                            feature_obj[attr] = country
                    elif attr.endswith("_STATE"):
                        region = self._extract_identity_region(doc)
                        if region:
                            feature_obj[attr] = region
                    elif attr == "ACCOUNT_DOMAIN":
                        feature_obj[attr] = self._sanitize_identifier_name(mapping["name"])
                    else:
                        feature_obj[attr] = doc_number

                if feature_obj:
                    record["FEATURES"].append(feature_obj)

    def _add_relationships(self, record: Dict, profile: ET.Element) -> None:
        profile_id = profile.get("ID")
        if not profile_id:
            return

        for rel in self.relationships_by_profile.get(profile_id, []):
            to_profile = rel.get("to")
            if not to_profile:
                continue
            target_ref = self.profile_id_to_fixed_ref.get(to_profile)
            if not target_ref:
                continue

            rel_feature: Dict[str, object] = {
                "REL_POINTER_DOMAIN": self.data_source,
                "REL_POINTER_KEY": target_ref,
            }

            role_value = self._determine_relationship_role(rel)
            if role_value:
                rel_feature["REL_POINTER_ROLE"] = role_value

            record["FEATURES"].append(rel_feature)

    def _add_payload_attributes(
        self,
        record: Dict,
        profile: ET.Element,
        max_reliability: Optional[int],
    ) -> None:
        profile_id = profile.get("ID")
        entries = self.sanctions_entries_by_profile.get(profile_id, []) if profile_id else []

        list_names: List[str] = []
        program_codes: List[str] = []
        sanction_types: List[str] = []
        entry_dates: List[str] = []

        for entry in entries:
            list_id = entry.get("ListID")
            if list_id:
                list_name = self.list_lookup.get(list_id, list_id)
                normalized = self._normalize_list_name(list_name)
                if normalized and normalized not in list_names:
                    list_names.append(normalized)

            for event in entry.findall("ofac:EntryEvent", NS):
                event_date = self._extract_entry_event_date(event)
                if event_date:
                    entry_dates.append(event_date)
                break  # Primary dates are uniform per entry

            for measure in entry.findall("ofac:SanctionsMeasure", NS):
                measure_type = measure.get("SanctionsTypeID")
                comment_elem = measure.find("ofac:Comment", NS)
                comment_text = self._clean_text(comment_elem.text if comment_elem is not None else None)

                if measure_type == "1":
                    if comment_text and comment_text not in program_codes:
                        program_codes.append(comment_text)
                else:
                    type_name = self.sanctions_type_lookup.get(measure_type, measure_type)
                    if type_name and type_name not in sanction_types:
                        sanction_types.append(type_name)

        if list_names:
            record.setdefault("SANCTIONS_LIST", "; ".join(list_names))

        if entry_dates:
            entry_dates.sort()
            record.setdefault("SANCTIONS_DATE", entry_dates[0])

        if program_codes:
            record.setdefault("SANCTIONS_PROGRAMS", "; ".join(program_codes))

        if sanction_types:
            record.setdefault("SANCTIONS_TYPE", "; ".join(sanction_types))

        primary_attr = profile.get("Primary")
        if primary_attr:
            record.setdefault("IS_PRIMARY", primary_attr.lower() == "true")

        if max_reliability is not None:
            record.setdefault("DATA_QUALITY_SCORE", max_reliability)

        if profile_id:
            remarks = self._build_relationship_remarks(profile_id)
            if remarks and "SANCTIONS_REMARKS" not in record:
                record["SANCTIONS_REMARKS"] = "; ".join(remarks)

    def _determine_relationship_role(self, rel: Dict[str, str]) -> Optional[str]:
        rel_type = rel.get("type")
        role_value: Optional[str] = None
        if rel_type:
            try:
                rel_type_id = int(rel_type)
            except ValueError:
                rel_type_id = None
            if rel_type_id is not None:
                role = RELATIONSHIP_ROLE_MAP.get(rel_type_id)
                if role is None:
                    role_name = self.relation_type_lookup.get(rel_type, f"RELATION_{rel_type_id}")
                    role = self._sanitize_identifier_name(role_name)
                role_value = role

        former = rel.get("former", "").lower()
        if former in {"true", "1", "yes"}:
            if role_value:
                if not role_value.startswith("FORMER_"):
                    role_value = f"FORMER_{role_value}"
            else:
                role_value = "FORMER"

        return role_value

    def _normalize_list_name(self, name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        trimmed = name.strip()
        if not trimmed:
            return None
        lowered = trimmed.lower()
        if lowered.endswith(" list"):
            trimmed = trimmed[: -len(" list")].strip()
        return trimmed

    def _extract_entry_event_date(self, entry_event: ET.Element) -> Optional[str]:
        date_elem = entry_event.find("ofac:Date", NS)
        if date_elem is not None:
            year = self._clean_text(date_elem.findtext("ofac:Year", default="", namespaces=NS))
            month = self._clean_text(date_elem.findtext("ofac:Month", default="", namespaces=NS))
            day = self._clean_text(date_elem.findtext("ofac:Day", default="", namespaces=NS))
            formatted = self._format_date_parts(year, month, day)
            if formatted:
                return formatted

        date_period = entry_event.find("ofac:DatePeriod", NS)
        if date_period is not None:
            start = date_period.find("ofac:Start", NS)
            if start is not None:
                year = self._clean_text(start.findtext("ofac:From/ofac:Year", default="", namespaces=NS))
                month = self._clean_text(start.findtext("ofac:From/ofac:Month", default="", namespaces=NS))
                day = self._clean_text(start.findtext("ofac:From/ofac:Day", default="", namespaces=NS))
                formatted = self._format_date_parts(year, month, day)
                if formatted:
                    return formatted
        return None

    def _build_relationship_remarks(self, profile_id: str) -> List[str]:
        remarks: List[str] = []
        for rel in self.relationships_by_profile.get(profile_id, []):
            target_profile = rel.get("to")
            if not target_profile:
                continue

            target_name = self._get_primary_name_for_profile(target_profile)
            if not target_name:
                target_name = self.profile_id_to_fixed_ref.get(target_profile, target_profile)

            display_role = "Linked To"
            remarks.append(f"{display_role}: {target_name}")

        return remarks

    def _get_primary_name_for_profile(self, profile_id: str) -> Optional[str]:
        cached = self.primary_name_cache.get(profile_id)
        if cached is not None:
            return cached

        party = self.profile_elements.get(profile_id)
        if party is None:
            return None

        profile_elem = party.find("ofac:Profile", NS)
        if profile_elem is None:
            return None

        identities = profile_elem.findall("ofac:Identity", NS)
        aliases: List[ET.Element] = []
        for identity in identities:
            aliases.extend(identity.findall("ofac:Alias", NS))

        primary_aliases = [alias for alias in aliases if alias.get("Primary", "").lower() == "true"]
        search_aliases = primary_aliases or aliases

        for alias in search_aliases:
            name = self._compose_alias_name(alias)
            if name:
                self.primary_name_cache[profile_id] = name
                return name

        return None

    def _compose_alias_name(self, alias: ET.Element) -> Optional[str]:
        for documented_name in alias.findall("ofac:DocumentedName", NS):
            parts: List[str] = []
            for part in documented_name.findall("ofac:DocumentedNamePart", NS):
                value_elem = part.find("ofac:NamePartValue", NS)
                text = self._clean_text(value_elem.text if value_elem is not None else None)
                if text:
                    parts.append(text)
            if parts:
                return " ".join(parts)
        return None

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------
    def _extract_feature_text(self, feature: ET.Element) -> Optional[str]:
        version_details = feature.findall("ofac:FeatureVersion/ofac:VersionDetail", NS)
        if not version_details:
            version_detail = feature.find("ofac:VersionDetail", NS)
            version_details = [version_detail] if version_detail is not None else []

        for version_detail in version_details:
            text = self._clean_text(version_detail.text if version_detail is not None else None)
            if text:
                return text

            ref_id = version_detail.get("DetailReferenceID") if version_detail is not None else None
            if ref_id:
                ref_text = self.detail_reference_lookup.get(ref_id)
                if ref_text:
                    return ref_text

        return None

    def _extract_feature_date(self, feature: ET.Element) -> Optional[str]:
        date_part = feature.find("ofac:FeatureVersion/ofac:DatePart", NS)
        if date_part is not None:
            year_elem = date_part.find("ofac:Year", NS)
            month_elem = date_part.find("ofac:Month", NS)
            day_elem = date_part.find("ofac:Day", NS)
            year = self._clean_text(year_elem.text if year_elem is not None else None)
            month = self._clean_text(month_elem.text if month_elem is not None else None)
            day = self._clean_text(day_elem.text if day_elem is not None else None)
            formatted = self._format_date_parts(year, month, day)
            if formatted:
                return formatted

        text_value = self._extract_feature_text(feature)
        if text_value:
            parsed = self._normalize_date_string(text_value)
            if parsed:
                return parsed
        return None

    def _extract_country_code(self, element: ET.Element) -> Optional[str]:
        country_id = element.get("CountryID")
        if country_id and country_id in self.country_lookup:
            return self.country_lookup[country_id]

        for child in element.iter():
            cid = child.get("CountryID")
            if cid and cid in self.country_lookup:
                return self.country_lookup[cid]
        return None

    def _extract_address_details(self, feature: ET.Element) -> Optional[Dict[str, Optional[str]]]:
        components: List[str] = []
        seen: set[str] = set()
        country = self._extract_country_code(feature)

        def add_component(value: Optional[str]) -> None:
            if not value:
                return
            if value not in seen:
                components.append(value)
                seen.add(value)

        def collect_location_parts(element: ET.Element) -> None:
            for part in element.findall("ofac:LocationPart", NS):
                value_elem = part.find("ofac:LocationPartValue/ofac:Value", NS)
                if value_elem is None:
                    value_elem = part.find("ofac:LocationPartValue", NS)
                value = self._clean_text(value_elem.text if value_elem is not None else None)
                add_component(value)

        # Direct location parts inside the feature version
        for part in feature.findall("ofac:FeatureVersion/ofac:LocationPart", NS):
            value_elem = part.find("ofac:LocationPartValue/ofac:Value", NS)
            if value_elem is None:
                value_elem = part.find("ofac:LocationPartValue", NS)
            value = self._clean_text(value_elem.text if value_elem is not None else None)
            add_component(value)

        # Location parts directly under the feature
        for part in feature.findall("ofac:LocationPart", NS):
            value_elem = part.find("ofac:LocationPartValue/ofac:Value", NS)
            if value_elem is None:
                value_elem = part.find("ofac:LocationPartValue", NS)
            value = self._clean_text(value_elem.text if value_elem is not None else None)
            add_component(value)

        # Resolve VersionLocation references to the Locations table
        version_location = feature.find("ofac:FeatureVersion/ofac:VersionLocation", NS)
        if version_location is not None:
            loc_id = version_location.get("LocationID")
            if loc_id:
                location_elem = self.location_lookup.get(loc_id)
                if location_elem is not None:
                    loc_country = self._extract_country_code(location_elem)
                    if loc_country and not country:
                        country = loc_country
                    collect_location_parts(location_elem)

        if not components:
            text_value = self._extract_feature_text(feature)
            add_component(text_value)

        if not components:
            return None

        return {"full": ", ".join(components), "country": country}

    def _extract_reliability(self, feature: ET.Element) -> Optional[int]:
        feature_version = feature.find("ofac:FeatureVersion", NS)
        if feature_version is None:
            return None
        reliability = feature_version.get("ReliabilityID")
        if not reliability:
            return None
        try:
            return int(reliability)
        except ValueError:
            return None

    def _extract_identity_number(self, doc: ET.Element) -> Optional[str]:
        direct = doc.find("ofac:IDRegistrationNo", NS)
        if direct is not None and direct.text:
            value = self._clean_text(direct.text)
            if value:
                return value

        detail_value = doc.find(".//ofac:DetailValue/ofac:Value", NS)
        if detail_value is not None and detail_value.text:
            value = self._clean_text(detail_value.text)
            if value:
                return value

        return None

    def _extract_identity_country(self, doc: ET.Element) -> Optional[str]:
        country_id = doc.get("IssuedBy-CountryID")
        if country_id and country_id in self.country_lookup:
            return self.country_lookup[country_id]
        return None

    def _extract_identity_region(self, doc: ET.Element) -> Optional[str]:
        region_elem = doc.find("ofac:IssuedBy-RegionText", NS)
        if region_elem is not None and region_elem.text:
            return self._clean_text(region_elem.text)
        return None

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _format_date_parts(year: Optional[str], month: Optional[str], day: Optional[str]) -> Optional[str]:
        if not year:
            return None
        if month and month.isdigit():
            month = month.zfill(2)
        else:
            month = None
        if day and day.isdigit():
            day = day.zfill(2)
        else:
            day = None

        if month and day:
            return f"{year}-{month}-{day}"
        if month:
            return f"{year}-{month}"
        return year

    @staticmethod
    def _normalize_date_string(value: str) -> Optional[str]:
        value = value.strip()
        if not value:
            return None

        for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%Y/%m/%d", "%m/%d/%Y", "%Y.%m.%d"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        if value.isdigit() and len(value) == 4:
            return value
        iso_match = re.match(r"^(\d{4})-(\d{2})(?:-(\d{2}))?", value)
        if iso_match:
            year, month, day = iso_match.group(1), iso_match.group(2), iso_match.group(3)
            return StrictOFACTransformer._format_date_parts(year, month, day)
        return None

    @staticmethod
    def _sanitize_identifier_name(name: Optional[str]) -> str:
        if not name:
            return "UNKNOWN"
        return re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_") or name


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform OFAC Advanced XML into strict Senzing JSONL output",
    )
    parser.add_argument("xml_file", type=Path, help="Path to OFAC Advanced XML file")
    parser.add_argument(
        "--output-jsonl",
        dest="output_jsonl",
        type=Path,
        default=Path("ofac_strict.jsonl"),
        help="Destination JSONL file (default: ofac_strict.jsonl)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
    )

    transformer = StrictOFACTransformer()
    logging.info("Loading %s", args.xml_file)
    transformer.load(args.xml_file)

    stats = transformer.transform(
        output_jsonl=args.output_jsonl,
    )

    logging.info(
        "Emitted %s records with %s features (%s relationships, %s identifiers)",
        f"{stats['emitted']:,}",
        f"{stats['features']:,}",
        f"{stats['relationships']:,}",
        f"{stats['identifiers']:,}",
    )

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point.
    sys.exit(main())

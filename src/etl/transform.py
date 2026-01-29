# transform.py - REFACTORED
import sys
from datetime import datetime
from schemas import HSProduct
from hs_parser import (
    extract_section_and_chapter,
    extract_designation,
    extract_taxes,
    extract_documents,
    extract_agreements,
    extract_history,
    refine_hierarchy_labels,
    extract_unit_of_measure
)

def transform(raw: dict) -> dict:
    """
    Transform raw scraped ADIL payload into storage-ready HS product.
    Delegates complex parsing logic to hs_parser.py.
    """
    hs_code = raw.get("hs_code", "NA")
    print(f"\n>>>> TRANSFORMING: {hs_code}")
    
    # 1. Prepare Data Context
    sections_list = raw.get("sections", [])
    sections = {s["section_name"]: s["content"] for s in sections_list}
    pos_tarifaire = sections.get("Position tarifaire", {})
    raw_text = pos_tarifaire.get("raw_text", "")
    scraped_at = raw.get("scraped_at") or datetime.utcnow().isoformat() + "Z"
    parser_version = "v1.4" # Bumped version for refactor

    # 2. Extract Base Hierarchy (Section, Chapter)
    section_code, section_label, chapter_code, chapter_label = extract_section_and_chapter(sections, pos_tarifaire)
    
    print(f"DEBUG: Section: {section_code}, Chapter: {chapter_code}")

    # 3. Extract Designation
    designation = extract_designation(pos_tarifaire, hs_code)
    
    # 4. Extract Detailed Hierarchy (Stateful Parsing)
    # This refines HS4/HS6/HS8 labels and updates designation if a better specific label is found
    hierarchy_data = refine_hierarchy_labels(pos_tarifaire, hs_code, designation)
    
    hs4_label = hierarchy_data["hs4_label"]
    hs6_label = hierarchy_data["hs6_label"]
    hs8_label = hierarchy_data["hs8_label"]
    # HS10 label is effectively the final designation
    final_designation = hierarchy_data["final_designation"]
    
    print(f"DEBUG: Labels - HS4: {hs4_label}, HS6: {hs6_label}, HS8: {hs8_label}")

    # 5. Extract Details
    unit_of_measure = extract_unit_of_measure(pos_tarifaire, raw_text)
    taxes = extract_taxes(sections)
    documents = extract_documents(sections)
    agreements = extract_agreements(sections)
    history = extract_history(sections)

    # 6. Build Final Product Structure
    hs4_code = hs_code[:4]
    hs6_code = hs_code[:6]
    hs8_code = hs_code[:8]

    product = {
        "hs_code": hs_code,
        "section_label": section_label,
        "chapter_label": chapter_label,
        "hs4_label": hs4_label,
        "hs6_label": hs6_label,
        "hs8_label": hs8_label,
        "designation": final_designation,
        "hierarchy": {
            "section_code": section_code,
            "section_label": section_label,
            "chapter_code": chapter_code,
            "chapter_label": chapter_label,
            "hs4": {"code": hs4_code, "label": hs4_label, "present": hs4_label != "NA"},
            "hs6": {"code": hs6_code, "label": hs6_label, "present": hs6_label != "NA"},
            "hs8": {"code": hs8_code, "label": hs8_label, "present": hs8_label != "NA"},
            "meta": {
                "source": "ADII",
                "scraped_at": scraped_at,
                "parser_version": parser_version,
                "lang": "fr"
            }
        },
        "unit_of_measure": unit_of_measure,
        "taxation": {
            "taxes": taxes, 
            "meta": {"source": "ADII", "scraped_at": scraped_at, "parser_version": parser_version, "lang": "fr"}
        },
        "documents": {
            "documents": documents, 
            "meta": {"source": "ADII", "scraped_at": scraped_at, "parser_version": parser_version, "lang": "fr"}
        },
        "accord_convention": {
            "accord_convention": agreements, 
            "meta": {"source": "ADII", "scraped_at": scraped_at, "parser_version": parser_version, "lang": "fr"}
        },
        "historique": {
            "items": history, 
            "meta": {"source": "ADII", "scraped_at": scraped_at, "parser_version": parser_version, "lang": "fr"}
        },
        "lineage": {
            "scraped_at": scraped_at,
            "status": raw.get("scrape_status", "success"),
            "url": raw.get("url"),
            "http": {
                "status_code": 200,
                "etag": None,
                "last_modified": None
            },
            "pipeline": {
                "scraper": "selenium-scraper",
                "parser_version": parser_version,
                "schema_version": "v1.0"
            },
            "quality": {
                "encoding_fixed": True,
                "missing_sections": [],
                "warnings": []
            },
            "sources": ["ADII"],
            "errors": []
        },
        "raw": raw
    }
    
    # 7. Validate with Pydantic
    try:
        HSProduct(**product)
        print(f"✅ Data validation passed for {hs_code}")
    except Exception as e:
        print(f"⚠️ Validation warning for {hs_code}: {e}")
    
    return product
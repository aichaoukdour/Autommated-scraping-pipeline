# transform.py - FIXED VERSION

import re
from datetime import datetime
from cleaners import clean_text_block, parse_french_date, parse_percentage, remove_adil_boilerplate
from schemas import HSProduct

def transform(raw: dict) -> dict:
    """
    Transform raw scraped ADIL payload into storage-ready HS product.
    Handles the nested 'sections' structure from adil_detailed.json.
    """
    import sys
    hs_code = raw.get("hs_code", "NA")
    print(f"\n>>>> TRANSFORMING: {hs_code}")
    
    # ============================================================================
    # DETERMINISTIC HIERARCHY EXTRACTION (from HS code directly)
    # ============================================================================
    hs4_code = hs_code[:4]   # "0101"
    hs6_code = hs_code[:6]   # "010121" or "010129"
    hs8_code = hs_code[:8]   # "01012100" or "01012900"
    
    print(f"DEBUG: Derived hierarchy - HS4: {hs4_code}, HS6: {hs6_code}, HS8: {hs8_code}")
    sys.stdout.flush()
    
    # Helper to map sections by name
    sections_list = raw.get("sections", [])
    print(f"DEBUG: Found {len(sections_list)} sections: {[s.get('section_name') for s in sections_list]}")
    sys.stdout.flush()
    
    sections = {s["section_name"]: s["content"] for s in sections_list}
    
    # ============================================================================
    # 1. Extract Position Tarifaire (Hierarchy & Basic Info)
    # ============================================================================
    pos_tarifaire = sections.get("Position tarifaire", {})
    kv = pos_tarifaire.get("key_values", {})
    raw_text = pos_tarifaire.get("raw_text", "")
    
    # === SECTION EXTRACTION (Robust) ===
    section_code = "NA"
    section_label = "NA"
    
    section_raw = kv.get("SECTION", "")
    if not section_raw and raw_text:
        # Try to find SECTION in raw text directly with multi-line support
        # Pattern: SECTION followed by colon, optional whitespace, then 2 digits
        s_match = re.search(r"SECTION\s*:\s*(\d{2})", raw_text)
        if s_match:
            section_code = s_match.group(1)
            # Find the label: usually the rest of the line or subsequent lines until next keyword
            # But for simplicity, we'll try to get it from the KV or a simpler regex
            s_label_match = re.search(rf"SECTION\s*:\s*{section_code}\s*[\-–—]\s*(.+?)(?:\n|CHAPITRE|$)", raw_text, re.DOTALL | re.I)
            if s_label_match:
                section_label = s_label_match.group(1).strip()
    
    if section_raw and section_code == "NA":
        match = re.match(r"^(\d{2})\s*[\-–—]\s*(.+)$", section_raw.strip(), re.DOTALL)
        if match:
            section_code = match.group(1)
            section_label = remove_adil_boilerplate(match.group(2).strip())
        else:
            parts = section_raw.split("-", 1)
            if len(parts) == 2:
                section_code = parts[0].strip()
                section_label = remove_adil_boilerplate(parts[1].strip())

    print(f"DEBUG: Section extracted - code='{section_code}', label='{section_label[:50]}...'")
    
    # === CHAPTER EXTRACTION (Robust) ===
    chapter_code = "NA"
    chapter_label = "NA"
    
    chapter_raw = kv.get("CHAPITRE", "")
    if not chapter_raw and raw_text:
        c_match = re.search(r"CHAPITRE\s*:\s*(\d{2})", raw_text)
        if c_match:
            chapter_code = c_match.group(1)
            c_label_match = re.search(rf"CHAPITRE\s*:\s*{chapter_code}\s*[\-–—]\s*(.+?)(?:\n|DESIGNATION|$)", raw_text, re.DOTALL | re.I)
            if c_label_match:
                chapter_label = c_label_match.group(1).strip()

    if chapter_raw and chapter_code == "NA":
        match = re.match(r"^(\d{2})\s*[\-–—]\s*(.+)$", chapter_raw.strip(), re.DOTALL)
        if match:
            chapter_code = match.group(1)
            chapter_label = remove_adil_boilerplate(match.group(2).strip())
        else:
            parts = chapter_raw.split("-", 1)
            if len(parts) == 2:
                chapter_code = parts[0].strip()
                chapter_label = remove_adil_boilerplate(parts[1].strip())
    
    print(f"DEBUG: Chapter extracted - code='{chapter_code}', label='{chapter_label[:50]}...'")
    sys.stdout.flush()
    
    # === DESIGNATION EXTRACTION (Context-Aware) ===
    designation = "NA"
    
    # Strategy 1: Look for the specific HS6 pattern and extract designation after it
    hs6_formatted = f"{hs_code[:4]}.{hs_code[4:6]}"  # "0101.21" or "0101.29"
    hs6_index = raw_text.find(hs6_formatted)
    
    if hs6_index != -1:
        # Extract text after the HS6 code
        text_after_hs6 = raw_text[hs6_index:]
        
        # Look for the HS10 sub-digits
        hs10_pattern = rf"{re.escape(hs6_formatted)}\s*\n?\s*(\d{{2}})\s*\n?\s*(\d{{2}})\s*\n?\s*-\s*-+\s*(.*?)(?:\n|$)"
        hs10_match = re.search(hs10_pattern, text_after_hs6, re.DOTALL)
        
        if hs10_match and hs10_match.group(1) == hs_code[6:8] and hs10_match.group(2) == hs_code[8:10]:
            # Found exact match for our HS10
            designation = hs10_match.group(3).strip()
            print(f"DEBUG: Designation found via HS10 pattern: '{designation[:50]}...'")
        else:
            # Fallback: just look for first "- -" after HS6
            des_match = re.search(r"-\s*-+\s*(.*?)(?:\n|$)", text_after_hs6)
            if des_match:
                designation = des_match.group(1).strip()
                print(f"DEBUG: Designation found via fallback: '{designation[:50]}...'")
    
    # Strategy 2: Fallback to key-value
    if designation == "NA" or not designation:
        designation = kv.get("DESIGNATION DU PRODUIT", "NA")
        if designation != "NA":
            print(f"DEBUG: Designation from key-value: '{designation[:50]}...'")
    
    # Clean up designation
    if designation and designation != "NA":
        # Remove common artifacts
        designation = re.sub(r'â€"', '-', designation)  # Fix encoding
        designation = re.sub(r'\s+', ' ', designation)  # Normalize whitespace
        designation = remove_adil_boilerplate(designation)
    
    scraped_at = raw.get("scraped_at") or datetime.utcnow().isoformat() + "Z"
    parser_version = "v1.3"  # Incremented version
    
    # ============================================================================
    # 2. Taxation Section (Enhanced with Code Extraction)
    # ============================================================================
    tax_content = sections.get("Droits et Taxes", {})
    raw_tax_text = tax_content.get("raw_text", "")
    taxes = []
    
    if raw_tax_text:
        # Join lines to handle multi-line labels and abbreviations
        # Example raw: "- Droit d'Importation * ( DI ) : 10 %"
        clean_raw = " ".join(raw_tax_text.split())
        
        # Pattern: - [Label] * ( [CODE] ) : [VALUE]
        tax_matches = re.findall(r"-\s*([^(*]+?)\s*\*?\s*\(\s*([A-Z]+)\s*\)\s*:\s*([^%-]+%)", clean_raw)
        
        for label, code, value in tax_matches:
            taxes.append({
                "code": code.strip(),
                "label": remove_adil_boilerplate(label.strip().replace("*", "")),
                "raw": value.strip()
            })
            
    # Fallback to key_values if no matches found via regex
    if not taxes:
        tax_kv = tax_content.get("key_values", {})
        for k, v in tax_kv.items():
            if any(x in k for x in ["Position tarifaire", "Situation du", "Source", "ADiL"]):
                continue
            
            code_match = re.search(r"\(([^)]+)\)", k)
            code = code_match.group(1).strip() if code_match else "NA"
            label = re.sub(r"^-?\s*", "", k).split("(")[0].strip().replace("*", "")
            
            taxes.append({
                "code": code,
                "label": remove_adil_boilerplate(label),
                "raw": v
            })
    
    taxation_meta = {
        "source": "ADII",
        "scraped_at": scraped_at,
        "parser_version": parser_version,
        "lang": "fr"
    }
    
    # ============================================================================
    # 3. Documents Section
    # ============================================================================
    doc_content = sections.get("Documents et Normes", {})
    doc_raw = doc_content.get("raw_text", "")
    documents = []
    
    # Simple lines-based parsing for "N° document\nDocument\nEmetteur" table
    doc_lines = [l.strip() for l in doc_raw.splitlines() if l.strip()]
    
    try:
        start_idx = -1
        for i, line in enumerate(doc_lines):
            if "Emetteur" in line:
                start_idx = i + 1
                break
        
        if start_idx != -1:
            # Process lines in groups of 3
            for i in range(start_idx, len(doc_lines), 3):
                if i + 2 < len(doc_lines):
                    documents.append({
                        "code": doc_lines[i],
                        "name": remove_adil_boilerplate(doc_lines[i+1]),
                        "issuer": remove_adil_boilerplate(doc_lines[i+2]),
                        "raw": f"{doc_lines[i]} {doc_lines[i+1]}"
                    })
    except Exception as e:
        print(f"WARNING: Failed to parse documents section: {e}")
    
    documents_meta = {
        "source": "ADII",
        "scraped_at": scraped_at,
        "parser_version": parser_version,
        "lang": "fr"
    }
    
    # ============================================================================
    # 4. Agreements Section
    # ============================================================================
    agg_content = sections.get("Accords et Convention", {})
    agg_raw = agg_content.get("raw_text", "")
    agreements = []
    
    # Parse agreements: data is structured as lines in groups of 4:
    # COUNTRY_NAME
    # LIST_TYPE (e.g., FRANCHISE, AGRI : LISTE 1 GROUPE 1)
    # DI_RATE (e.g., 0, 2.5)
    # TPI_RATE (e.g., 0, (*))
    agg_lines = [l.strip() for l in agg_raw.splitlines() if l.strip()]
    
    # Find where the data starts (after the header row containing "TPI")
    start_idx = -1
    for i, line in enumerate(agg_lines):
        if line == "TPI":
            # The next line is usually "( en % )"
            if i + 1 < len(agg_lines) and "%" in agg_lines[i+1]:
                start_idx = i + 2
                break
    
    if start_idx != -1:
        # Process lines in groups of 4
        for i in range(start_idx, len(agg_lines) - 3, 4):
            country = agg_lines[i]
            list_type = agg_lines[i + 1]
            di_rate = agg_lines[i + 2]
            tpi_rate = agg_lines[i + 3]
            
            # Skip footnotes or end of section
            if country.startswith("(") or "Taux" in country or "Source" in country:
                continue
                
            agreements.append({
                "country": country,
                "list": list_type,
                "DI": di_rate,
                "TPI": tpi_rate,
                "raw": f"{country} {list_type} DI:{di_rate} TPI:{tpi_rate}"
            })
    
    print(f"DEBUG: Extracted {len(agreements)} agreements from 'Accords et Convention' section")
    
    agreements_meta = {
        "source": "ADII",
        "scraped_at": scraped_at,
        "parser_version": parser_version,
        "lang": "fr"
    }
    
    # ============================================================================
    # 5. History Section
    # ============================================================================
    hist_content = sections.get("Historique Droit d'Importation", {})
    hist_raw = hist_content.get("raw_text", "")
    history = []
    
    # Format: "Date\n02/01/2015\nTaux\n2,5 %"
    hist_lines = [l.strip() for l in hist_raw.splitlines() if l.strip()]
    
    for i, line in enumerate(hist_lines):
        # Look for date pattern
        if re.match(r"\d{2}/\d{2}/\d{4}", line):
            rate = hist_lines[i+2] if i + 2 < len(hist_lines) else ""
            history.append({
                "date": parse_french_date(line),
                "raw": f"Taux: {rate}"
            })
    
    history_meta = {
        "source": "ADII",
        "scraped_at": scraped_at,
        "parser_version": parser_version,
        "lang": "fr"
    }
    
    # === STATEFUL HIERARCHY PARSER ===
    # Tracks the sequence of codes to accurately attribute labels
    hs4_label = "NA"
    hs6_label = "NA"
    hs8_label = "NA"
    hs10_label = "NA"

    # Find where the actual table starts to avoid matching header text/codes
    table_start_marker = "Codification" if "Codification" in raw_text else "01.01"
    table_text = raw_text[raw_text.find(table_start_marker):] if table_start_marker in raw_text else raw_text

    # Define targets
    hs4_fmt = f"{hs4_code[:2]}.{hs4_code[2:]}"
    hs6_fmt = f"{hs6_code[:4]}.{hs6_code[4:]}"
    hs8_part = hs_code[6:8]
    hs10_part = hs_code[8:10]

    # Process line by line (excluding the last line which is the Unit of Measure)
    lines = [l.strip() for l in table_text.splitlines() if l.strip()]
    unit_line = lines[-1] if lines else None
    clean_lines = lines[:-1] if lines else []
    
    active_level = None
    level_labels = {"HS4": [], "HS6": [], "HS8": [], "HS10": []}

    for line in clean_lines:
        # 1. Check for Level Transitions (Code Matches)
        if line == hs4_fmt:
            active_level = "HS4"
            continue
        elif line == hs6_fmt:
            active_level = "HS6"
            continue
        elif active_level == "HS6" and line == hs8_part:
            active_level = "HS8"
            continue
        elif active_level == "HS8" and line == hs10_part:
            active_level = "HS10"
            continue
        
        # 2. Accumulate Labels for the Active Level (if it's not a code)
        if active_level and not re.match(r"^\d+[\d\.]*$", line):
            level_labels[active_level].append(line)

    # 3. Finalize Labels
    hs4_label = remove_adil_boilerplate(" ".join(level_labels["HS4"])) or "NA"
    hs6_label = remove_adil_boilerplate(" ".join(level_labels["HS6"])) or "NA"
    hs8_label = remove_adil_boilerplate(" ".join(level_labels["HS8"])) or "NA"
    # For HS10, if we found nothing in the sequence, fallback to the main designation
    hs10_label = remove_adil_boilerplate(" ".join(level_labels["HS10"])) or designation

    # Ensure designation is synced with the most specific extracted label
    designation = hs10_label if hs10_label != "NA" else designation

    print(f"DEBUG: Stateful Labels - HS4: '{hs4_label}', HS6: '{hs6_label}', HS8: '{hs8_label}', HS10: '{hs10_label}'")

    # Extract Unit of Measure: Use the last non-empty line of raw_text (e.g., 'U', 'KGS')
    # Fallback to metadata if raw_text is missing or doesn't have a clear unit
    unit_of_measure = "NA"
    if raw_text:
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        if lines:
            potential_unit = lines[-1]
            # Units are typically short codes like 'U', 'KGS', 'M2', etc.
            if len(potential_unit) <= 5: 
                unit_of_measure = potential_unit
            else:
                # Fallback to metadata if last line looks like a label
                unit_of_measure = pos_tarifaire.get("metadata", {}).get("unit", "U")

    # ============================================================================
    # 6. Build Final Product
    # ============================================================================
    product = {
        "hs_code": hs_code,
        "section_label": remove_adil_boilerplate(section_label),
        "chapter_label": remove_adil_boilerplate(chapter_label),
        "hs4_label": hs4_label,
        "hs6_label": hs6_label,
        "hs8_label": hs8_label,
        "designation": remove_adil_boilerplate(designation),
        "hierarchy": {
            "section_code": section_code,
            "section_label": remove_adil_boilerplate(section_label),
            "chapter_code": chapter_code,
            "chapter_label": remove_adil_boilerplate(chapter_label),
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
        "taxation": {"taxes": taxes, "meta": taxation_meta},
        "documents": {"documents": documents, "meta": documents_meta},
        "accord_convention": {"accord_convention": agreements, "meta": agreements_meta},
        "historique": {"items": history, "meta": history_meta},
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
    
    # ============================================================================
    # 7. Validate with Pydantic
    # ============================================================================
    try:
        HSProduct(**product)
        print(f"✅ Data validation passed for {hs_code}")
    except Exception as e:
        print(f"⚠️ Validation warning for {hs_code}: {e}")
    
    return product
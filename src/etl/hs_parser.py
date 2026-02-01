"""ADIL Text Parser module."""
import re
from typing import Optional, Tuple, List, Dict
from scraper.config import logger
from cleaners import parse_french_date, remove_adil_boilerplate, clean_hs_label_for_rag, normalize_text
from cleaning_constants import SECTION_CHAPTER_PATTERNS, TAX_PATTERNS, DOCUMENTS_KEYS, AGREEMENT_KEYS, BOILERPLATE

def _extract_hierarchy_component(
    pos_tarifaire: dict, 
    component_type: str,
    key_name: str,
    code_pattern_key: str,
    label_pattern_key: str
) -> Tuple[str, str]:
    """
    Generic extraction logic for Section or Chapter.
    
    Args:
        pos_tarifaire: Position tarifaire data
        component_type: "section" or "chapter" (for logging)
        key_name: Key to look for in key_values (e.g., "SECTION", "CHAPITRE")
        code_pattern_key: Key in SECTION_CHAPTER_PATTERNS for code regex
        label_pattern_key: Key in SECTION_CHAPTER_PATTERNS for label regex
    
    Returns:
        Tuple of (code, label)
    """
    key_values = pos_tarifaire.get("key_values", {})
    raw_text = pos_tarifaire.get("raw_text", "")
    
    code, label = "NA", "NA"
    component_raw = key_values.get(key_name, "")
    
    # Strategy 1: Regex from raw text
    if not component_raw and raw_text:
        match = re.search(SECTION_CHAPTER_PATTERNS[code_pattern_key], raw_text)
        if match:
            code = match.group(1)
            pattern = SECTION_CHAPTER_PATTERNS[label_pattern_key].format(code=code)
            match_lbl = re.search(pattern, raw_text, re.DOTALL | re.I)
            if match_lbl:
                label = match_lbl.group(1).strip()
    
    # Strategy 2: Fallback to structured key/value
    if component_raw and code == "NA":
        match = re.match(SECTION_CHAPTER_PATTERNS["FALLBACK_SPLIT"], component_raw.strip(), re.DOTALL)
        if match:
            code = match.group(1)
            label = remove_adil_boilerplate(match.group(2).strip())
        else:
            parts = component_raw.split("-", 1)
            if len(parts) == 2:
                code = parts[0].strip()
                label = remove_adil_boilerplate(parts[1].strip())
                
    return code, label

def extract_section(pos_tarifaire: dict) -> Tuple[str, str]:
    """Extract Section code and label."""
    return _extract_hierarchy_component(
        pos_tarifaire, "section", "SECTION", "SECTION_CODE", "SECTION_LABEL"
    )

def extract_chapter(pos_tarifaire: dict) -> Tuple[str, str]:
    """Extract Chapter code and label."""
    return _extract_hierarchy_component(
        pos_tarifaire, "chapter", "CHAPITRE", "CHAPTER_CODE", "CHAPTER_LABEL"
    )

def extract_section_and_chapter(sections: dict, pos_tarifaire: dict) -> Tuple[str, str, str, str]:
    """Legacy wrapper for backward compatibility."""
    s_code, s_label = extract_section(pos_tarifaire)
    c_code, c_label = extract_chapter(pos_tarifaire)
    return s_code, s_label, c_code, c_label



def extract_designation(pos_tarifaire: dict, hs_code: str) -> str:
    """Extract product designation from text or metadata."""
    key_values = pos_tarifaire.get("key_values", {})
    raw_text = pos_tarifaire.get("raw_text", "")
    designation = "NA"
    
    hs6_fmt = f"{hs_code[:4]}.{hs_code[4:6]}"
    hs6_idx = raw_text.find(hs6_fmt)
    
    if hs6_idx != -1:
        text_after = raw_text[hs6_idx:]
        
        hs10_pattern = rf"{re.escape(hs6_fmt)}\s*\n?\s*(\d{{2}})\s*\n?\s*(\d{{2}})\s*\n?\s*-\s*-+\s*(.*?)(?:\n|$)"
        match = re.search(hs10_pattern, text_after, re.DOTALL)
        
        if match and match.group(1) == hs_code[6:8] and match.group(2) == hs_code[8:10]:
            designation = match.group(3).strip()
        else:
            match_des = re.search(r"-\s*-+\s*(.*?)(?:\n|$)", text_after)
            if match_des:
                designation = match_des.group(1).strip()
    
    if designation == "NA" or not designation:
        designation = key_values.get("DESIGNATION DU PRODUIT", "NA")
    
    if designation and designation != "NA":
        designation = normalize_text(designation) or ""
        designation = re.sub(r'\s+', ' ', designation)
        designation = remove_adil_boilerplate(designation)
        
    return designation


def extract_taxes(sections: dict) -> List[Dict[str, str]]:
    """Extract taxes and duties."""
    tax_content = sections.get("Droits et Taxes", {})
    raw_text = tax_content.get("raw_text", "")
    taxes = []
    
    if raw_text:
        clean_raw = " ".join(raw_text.split())
        matches = re.findall(TAX_PATTERNS["MAIN"], clean_raw)
        
        for label, code, value in matches:
            taxes.append({
                "code": code.strip(),
                "label": remove_adil_boilerplate(label.strip().replace("*", "")),
                "raw": value.strip()
            })
            
    if not taxes:
        tax_kv = tax_content.get("key_values", {})
        for key, value in tax_kv.items():
            if any(x in key for x in ["Position tarifaire", "Situation du", "Source", "ADiL"]):
                continue
            
            match_code = re.search(TAX_PATTERNS["CODE_FROM_KEY"], key)
            code = match_code.group(1).strip() if match_code else "NA"
            label = re.sub(TAX_PATTERNS["KEY_CLEAN"], "", key).split("(")[0].strip().replace("*", "")
            
            taxes.append({
                "code": code,
                "label": remove_adil_boilerplate(label),
                "raw": value
            })
    return taxes

def extract_documents(sections: dict) -> List[Dict[str, str]]:
    """Extract required documents."""
    doc_content = sections.get("Documents et Normes", {})
    raw_text = doc_content.get("raw_text", "")
    documents = []

    if not raw_text:
        return documents

    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    current_doc = None

    for line in lines:
        # Strict boilerplate filtering
        if any(k in line for k in DOCUMENTS_KEYS):
            continue
        if line in ["AD", "i", "L", "ADII", "Source :", "Situation du :"] or line in BOILERPLATE:
            continue
        # Check against raw BOILERPLATE list just in case
        if any(bp in line for bp in BOILERPLATE if len(bp) > 4):
            continue

        # Document codes are typically 3-5 digits (e.g., 06002)
        # Some are alphanumeric but rare. The garbage "AD i" was matching < 10 len.
        is_code = re.match(r"^\d{3,5}$", line)
        
        if is_code:
            if current_doc:
                documents.append(current_doc)
            current_doc = {
                "code": line,
                "name": "NA",
                "issuer": "NA",
                "raw": line
            }
        elif current_doc:
            # If we have a current doc, fill in fields
            if current_doc["name"] == "NA":
                current_doc["name"] = line
            elif current_doc["issuer"] == "NA":
                # Check directly if it looks like an issuer (all caps often) or just next line
                current_doc["issuer"] = line
            else:
                # Append extra text to name or issuer? usually 3 lines: code, name, issuer
                # If we already have issuer, maybe it's multi-line name?
                # For now, ignore or append to issuer
                pass
    
    if current_doc:
        documents.append(current_doc)
        
    # De-duplicate documents (based on code, name, and issuer)
    seen_docs = set()
    unique_documents = []
    for doc in documents:
        doc_fingerprint = (doc["code"], doc["name"], doc["issuer"])
        if doc_fingerprint not in seen_docs:
            unique_documents.append(doc)
            seen_docs.add(doc_fingerprint)
            
    return unique_documents

def extract_agreements(sections: dict) -> List[Dict[str, str]]:
    """Extract trade agreements."""
    agg_content = sections.get("Accords et Convention", {})
    raw_text = agg_content.get("raw_text", "")
    agreements = []
    
    if not raw_text:
        return agreements
        
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    # Heuristics for field types
    def is_rate_value(line):
        # Matches numbers (0, 2.5, 10), percentages (10%), or special markers like (*)
        return re.match(r"^[\d\.\,]+(\s*%)?$", line) or line == "(*)" or line == "0"

    def is_regime_keyword(line):
        keywords = ["FRANCHISE", "DEMANTELEMENT", "ANNEXE", "AGRI", "LISTE", "PAYS MOINS", "PROTOCOLE"]
        return any(k in line.upper() for k in keywords)

    current_acc = None
    
    for line in lines:
        # 1. Filter out known headers/noise (Exact matches or safe substrings)
        if line in ["Accords", "Liste", "DI", "( en % )", "TPI", "Source :", "ADII"]:
            continue
        if line in BOILERPLATE:
            continue
        if "Position tarifaire" in line or "Situation du" in line:
            continue
        if "Accords et Conventions" in line:
            continue
        # Filter very short garbage
        if len(line) < 3 and not is_rate_value(line) and line not in ["UE", "UK"]:
             continue

        # Filter dates (e.g., 01/02/2026 14:37:33)
        if re.search(r"\d{2}/\d{2}/\d{4}", line):
            continue

        # Filter footnotes/legends (e.g., (*) Taux du Régime du Droit Commun)
        if "(*) Taux du Régime" in line or line.startswith("(*)"):
            continue
            
        # 2. Identify Line Type
        if is_rate_value(line):
            if current_acc:
                if current_acc["DI"] == "NA":
                    current_acc["DI"] = line
                elif current_acc["TPI"] == "NA":
                    current_acc["TPI"] = line
                else:
                    # If we already have both rates, this might be a parsing artifact or extra column
                    pass 
        elif is_regime_keyword(line):
            if current_acc:
                # If we already have a list/regime, append to it (handles multi-line descriptions)
                if current_acc["list"] == "NA":
                    current_acc["list"] = line
                else:
                    current_acc["list"] += f" {line}"
        else:
            # Assume it's a Country/Agreement Name if it's not a rate or regime
            # This implicitly starts a new record
            
            # Save previous if complete-ish
            if current_acc:
                agreements.append(current_acc)
            
            # Start new
            current_acc = {
                "country": line,
                "list": "NA",
                "DI": "NA",
                "TPI": "NA",
                "raw": line
            }
            
    # Append last one
    if current_acc:
        agreements.append(current_acc)
        
    # De-duplicate agreements (based on country, list, DI, TPI)
    seen_acc = set()
    unique_agreements = []
    for acc in agreements:
        acc_fingerprint = (acc["country"], acc["list"], acc["DI"], acc["TPI"])
        if acc_fingerprint not in seen_acc:
            unique_agreements.append(acc)
            seen_acc.add(acc_fingerprint)
            
    return unique_agreements


def extract_history(sections: dict) -> List[Dict[str, str]]:
    """Extract import duty history."""
    content = sections.get("Historique Droit d'Importation", {})
    raw_text = content.get("raw_text", "")
    history = []
    
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    for i, line in enumerate(lines):
        if re.match(r"\d{2}/\d{2}/\d{4}", line):
            rate = lines[i+2] if i + 2 < len(lines) else ""
            history.append({
                "date": parse_french_date(line),
                "raw": f"Taux: {rate}"
            })
    return history


def refine_hierarchy_labels(pos_tarifaire: dict, hs_code: str, designation: str) -> Dict[str, str]:
    """Stateful extraction of hierarchy labels."""
    raw_text = pos_tarifaire.get("raw_text", "")
    
    hs4_c = hs_code[:4]
    hs6_c = hs_code[:6]
    hs8_part = hs_code[6:8]
    hs10_part = hs_code[8:10]

    hs4_fmt = f"{hs4_c[:2]}.{hs4_c[2:]}"
    hs6_fmt = f"{hs6_c[:4]}.{hs6_c[4:]}"
    
    start_marker = "Codification" if "Codification" in raw_text else "01.01"
    idx = raw_text.find(start_marker)
    text = raw_text[idx:] if idx != -1 else raw_text

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    clean_lines = lines[:-1] if lines else []
    
    active_level = None
    labels = {"HS4": [], "HS6": [], "HS8": [], "HS10": []}

    for line in clean_lines:
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
        
        if active_level and not re.match(r"^\d+[\d\.]*$", line):
            labels[active_level].append(line)

    hs4 = clean_hs_label_for_rag(remove_adil_boilerplate(" ".join(labels["HS4"]))) or "NA"
    hs6 = clean_hs_label_for_rag(remove_adil_boilerplate(" ".join(labels["HS6"]))) or "NA"
    hs8 = clean_hs_label_for_rag(remove_adil_boilerplate(" ".join(labels["HS8"]))) or "NA"
    hs10 = clean_hs_label_for_rag(remove_adil_boilerplate(" ".join(labels["HS10"]))) or designation

    final_des = hs10 if hs10 != "NA" else designation
    
    return {
        "hs4_label": hs4,
        "hs6_label": hs6,
        "hs8_label": hs8,
        "hs10_label": hs10,
        "final_designation": final_des
    }


def extract_unit_of_measure(pos_tarifaire: dict, raw_text: str) -> str:
    """Extract Unit of Measure."""
    uom = "NA"
    if raw_text:
        lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
        if lines:
            pot_unit = lines[-1]
            if len(pot_unit) <= 5: 
                uom = pot_unit
            else:
                uom = pos_tarifaire.get("metadata", {}).get("unit", "U")
    return uom
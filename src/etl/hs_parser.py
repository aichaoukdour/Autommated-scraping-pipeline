"""ADIL Text Parser module."""
import re
from datetime import datetime
from cleaners import parse_french_date, remove_adil_boilerplate, clean_hs_label_for_rag

def extract_section_and_chapter(sections: dict, pos_tarifaire: dict):
    """Extract Section and Chapter codes/labels."""
    key_values = pos_tarifaire.get("key_values", {})
    raw_text = pos_tarifaire.get("raw_text", "")
    
    section_code = "NA"
    section_label = "NA"
    section_raw = key_values.get("SECTION", "")
    
    if not section_raw and raw_text:
        match = re.search(r"SECTION\s*:\s*(\d{2})", raw_text)
        if match:
            section_code = match.group(1)
            match_lbl = re.search(
                rf"SECTION\s*:\s*{section_code}\s*[\-–—]\s*(.+?)(?:\n|CHAPITRE|$)", 
                raw_text, 
                re.DOTALL | re.I
            )
            if match_lbl:
                section_label = match_lbl.group(1).strip()
    
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

    chapter_code = "NA"
    chapter_label = "NA"
    chapter_raw = key_values.get("CHAPITRE", "")
    
    if not chapter_raw and raw_text:
        match = re.search(r"CHAPITRE\s*:\s*(\d{2})", raw_text)
        if match:
            chapter_code = match.group(1)
            match_lbl = re.search(
                rf"CHAPITRE\s*:\s*{chapter_code}\s*[\-–—]\s*(.+?)(?:\n|DESIGNATION|$)", 
                raw_text, 
                re.DOTALL | re.I
            )
            if match_lbl:
                chapter_label = match_lbl.group(1).strip()

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
                
    return section_code, section_label, chapter_code, chapter_label


def extract_designation(pos_tarifaire: dict, hs_code: str):
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
        designation = re.sub(r'â€"', '-', designation)
        designation = re.sub(r'\s+', ' ', designation)
        designation = remove_adil_boilerplate(designation)
        
    return designation


def extract_taxes(sections: dict):
    """Extract taxes and duties."""
    tax_content = sections.get("Droits et Taxes", {})
    raw_text = tax_content.get("raw_text", "")
    taxes = []
    
    if raw_text:
        clean_raw = " ".join(raw_text.split())
        matches = re.findall(r"-\s*([^(*]+?)\s*\*?\s*\(\s*([A-Z]+)\s*\)\s*:\s*([^%-]+%)", clean_raw)
        
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
            
            match_code = re.search(r"\(([^)]+)\)", key)
            code = match_code.group(1).strip() if match_code else "NA"
            label = re.sub(r"^-?\s*", "", key).split("(")[0].strip().replace("*", "")
            
            taxes.append({
                "code": code,
                "label": remove_adil_boilerplate(label),
                "raw": value
            })
    return taxes


def extract_documents(sections: dict):
    """Extract required documents."""
    doc_content = sections.get("Documents et Normes", {})
    raw_text = doc_content.get("raw_text", "")
    documents = []
    
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    try:
        start_idx = -1
        for i, line in enumerate(lines):
            if "Emetteur" in line:
                start_idx = i + 1
                break
        
        if start_idx != -1:
            for i in range(start_idx, len(lines), 3):
                if i + 2 < len(lines):
                    documents.append({
                        "code": lines[i],
                        "name": remove_adil_boilerplate(lines[i+1]),
                        "issuer": remove_adil_boilerplate(lines[i+2]),
                        "raw": f"{lines[i]} {lines[i+1]}"
                    })
    except Exception as e:
        print(f"WARNING: Parse error in documents: {e}")
        
    return documents


def extract_agreements(sections: dict):
    """Extract trade agreements."""
    agg_content = sections.get("Accords et Convention", {})
    raw_text = agg_content.get("raw_text", "")
    agreements = []
    
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    start_idx = -1
    for i, line in enumerate(lines):
        if line == "TPI":
            if i + 1 < len(lines) and "%" in lines[i+1]:
                start_idx = i + 2
                break
    
    if start_idx != -1:
        for i in range(start_idx, len(lines) - 3, 4):
            country = lines[i]
            list_type = lines[i + 1]
            di_rate = lines[i + 2]
            tpi_rate = lines[i + 3]
            
            if country.startswith("(") or "Taux" in country or "Source" in country:
                continue
                
            agreements.append({
                "country": country,
                "list": list_type,
                "DI": di_rate,
                "TPI": tpi_rate,
                "raw": f"{country} {list_type} DI:{di_rate} TPI:{tpi_rate}"
            })
            
    return agreements


def extract_history(sections: dict):
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


def refine_hierarchy_labels(pos_tarifaire: dict, hs_code: str, designation: str):
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


def extract_unit_of_measure(pos_tarifaire: dict, raw_text: str):
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

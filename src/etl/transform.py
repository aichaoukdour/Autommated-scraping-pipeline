# transform.py

from datetime import datetime
from cleaners import clean_text_block, parse_french_date, parse_percentage


import re
from datetime import datetime
from cleaners import clean_text_block, parse_french_date, parse_percentage

def transform(raw: dict) -> dict:
    """
    Transform raw scraped ADIL payload into storage-ready HS product.
    Handles the nested 'sections' structure from adil_detailed.json.
    """
    hs10 = raw.get("hs_code", "NA")
    
    # Helper to map sections by name
    sections = {s["section_name"]: s["content"] for s in raw.get("sections", [])}
    
    # 1. Extract Position Tarifaire (Hierarchy & Basic Info)
    pos_tarifaire = sections.get("Position tarifaire", {})
    kv = pos_tarifaire.get("key_values", {})
    
    # Section & Chapter
    section_raw = kv.get("SECTION", "")
    s_match = re.match(r"(\d+)\s*-\s*(.*)", section_raw)
    section_code = s_match.group(1) if s_match else "NA"
    section_label = s_match.group(2) if s_match else "NA"
    
    chapter_raw = kv.get("CHAPITRE", "")
    c_match = re.match(r"(\d+)\s*-\s*(.*)", chapter_raw)
    chapter_code = c_match.group(1) if c_match else "NA"
    chapter_label = c_match.group(2) if c_match else "NA"
    
    # Use regex to find HS4/HS6/Designation in raw_text if not in kv
    raw_text = pos_tarifaire.get("raw_text", "")
    hs4_match = re.search(r"(\d{2}\.\d{2})", raw_text)
    hs4_code = hs4_match.group(1).replace(".", "") if hs4_match else hs10[:4]
    
    hs6_match = re.search(r"(\d{4}\.\d{2})", raw_text)
    hs6_code = hs6_match.group(1).replace(".", "") if hs6_match else hs10[:6]

    # Designation: often follows HS6 or a list of digits
    # In the sample: "... 0101.21 \n 00 \n 00 \n - - Reproducteurs de race pure (a.)"
    des_match = re.search(r"-\s*-\s*(.*)", raw_text)
    designation = des_match.group(1).strip() if des_match else kv.get("DESIGNATION DU PRODUIT", "NA")

    # 2. Taxation Section
    tax_content = sections.get("Droits et Taxes", {})
    tax_kv = tax_content.get("key_values", {})
    taxes = []
    for k, v in tax_kv.items():
        if any(x in k for x in ["Position tarifaire", "Situation du", "Source"]):
            continue
        # Extract code from brackets: "- Droit d'Importation* ( DI )"
        code_m = re.search(r"\((.*?)\)", k)
        code = code_m.group(1) if code_m else "NA"
        label = re.sub(r"^-?\s*", "", k).split("(")[0].strip().replace("*", "")
        taxes.append({
            "code": code,
            "label": clean_text_block(label),
            "raw": v
        })

    # 3. Documents Section
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
            for i in range(start_idx, len(doc_lines), 3):
                if i + 2 < len(doc_lines):
                    documents.append({
                        "code": doc_lines[i],
                        "name": clean_text_block(doc_lines[i+1]),
                        "issuer": clean_text_block(doc_lines[i+2]),
                        "raw": f"{doc_lines[i]} {doc_lines[i+1]}"
                    })
    except Exception:
        pass

    # 4. Agreements Section
    agg_content = sections.get("Accords et Convention", {})
    agg_raw = agg_content.get("raw_text", "")
    agreements = []
    # Regex to capture: Name + Rate1 + Rate2
    for line in agg_raw.splitlines():
        match = re.search(r"^(.*?)\s+([\d,\.]+%?|\(\*\))\s+([\d,\.]+%?|\(\*\))$", line.strip())
        if match:
            agreements.append({
                "country": match.group(1).strip(),
                "benefit": match.group(2), # DI rate usually
                "raw": line.strip()
            })

    # 5. History Section
    hist_content = sections.get("Historique Droit d'Importation", {})
    hist_raw = hist_content.get("raw_text", "")
    history = []
    # Format: "Date\n02/01/2015\nTaux\n2,5 %"
    hist_lines = [l.strip() for l in hist_raw.splitlines() if l.strip()]
    for i, line in enumerate(hist_lines):
        if re.match(r"\d{2}/\d{2}/\d{4}", line):
            rate = hist_lines[i+2] if i + 2 < len(hist_lines) else ""
            history.append({
                "date": parse_french_date(line),
                "event": f"Taux: {rate}",
                "raw": line
            })

    # Build Product
    product = {
        "hs10": hs10,
        "hierarchy": {
            "section_code": section_code,
            "section_label": clean_text_block(section_label),
            "chapter_code": chapter_code,
            "chapter_label": clean_text_block(chapter_label),
            "hs4": {"code": hs4_code, "label": "NA", "present": True},
            "hs6": {"code": hs6_code, "label": "NA", "present": True}
        },
        "designation": clean_text_block(designation),
        "unit_of_measure": pos_tarifaire.get("metadata", {}).get("unit", "U"),
        "entry_into_force_date": None, # Could be extracted from main_content
        "taxation": {"taxes": taxes, "source": "ADII"},
        "documents": {"documents": documents, "source": "ADII"},
        "agreements": agreements,
        "import_duty_history": history,
        "lineage": {
            "scraped_at": raw.get("scraped_at", datetime.utcnow().isoformat() + "Z"),
            "status": raw.get("scrape_status", "success"),
            "parser_version": "v2",
            "quality": {"encoding_fixed": True, "missing_blocks": []}
        },
        "raw": raw
    }

    return product


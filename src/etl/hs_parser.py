import re
from ftfy import fix_text

def parse_hs_hierarchy(raw_text: str) -> dict:
    """
    Extract HS hierarchy dynamically: 4 / 6 / 10 digits.
    """
    text = fix_text(raw_text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    result = {"parent_category": "NA", "sub_category": "NA", "product_name": "NA", "unit": None}

    for line in lines:
        if re.fullmatch(r"\d{2}\.\d{2}", line):
            result["parent_category"] = line.replace(".", "")
        elif re.fullmatch(r"\d{4}\.\d{2}", line):
            result["sub_category"] = line.replace(".", "")
        elif line.startswith("-"):
            result["product_name"] = line.lstrip("- ").strip()
        elif line.isalpha() and len(line) <= 3:
            result["unit"] = line
    return result

def parse_annual_volumes(raw_text: str):
    from schemas import ImportExportHistory
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    # Try to split by "Année" and "Poids" headers if possible
    years = []
    weights = []
    
    current_section = None
    
    for line in lines:
        if "Année" in line:
            current_section = "years"
            continue
        elif "Poids" in line:
            current_section = "weights"
            continue
            
        if current_section == "years":
            # Filter for 4-digit years
            if line.isdigit() and len(line) == 4:
                years.append(int(line))
        elif current_section == "weights":
            # Filter for numbers
            clean_num = line.replace(" ", "").replace("\xa0", "")
            if clean_num.isdigit():
                weights.append(float(clean_num))
    
    # Fallback if specific headers weren't found or parsing failed, 
    # though typical ADIL format is consistent.
    # If we didn't use sections (e.g. they weren't found), we might try heuristic but 
    # given the samples, headers are present.
    
    history = []
    # Zip safely
    for y, w in zip(years, weights):
        history.append(ImportExportHistory(year=y, weight_kg=w))
        
    return history

def parse_suppliers_or_clients(raw_text: str):
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    countries = []
    weights = []
    
    current_section = None
    
    for line in lines:
        if line in ["Pays", "Poids"]:
            current_section = line
            continue
            
        if current_section == "Pays":
            # Exclude some noise if any, but usually it's just country names
            if len(line) > 1 and not line.replace(" ", "").isdigit():
                countries.append(line)
        elif current_section == "Poids":
            clean_num = line.replace(" ", "").replace("\xa0", "")
            if clean_num.isdigit():
                weights.append(float(clean_num))
                
    results = []
    for c, w in zip(countries, weights):
        results.append({"country": c, "weight_kg": w})
        
    return results

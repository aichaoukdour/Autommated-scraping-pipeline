import re
from ftfy import fix_text

def parse_hs_hierarchy(raw_text: str) -> dict:
    """
    Extract HS hierarchy dynamically: 4 / 6 / 10 digits and their descriptions.
    """
    text = fix_text(raw_text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    result = {
        "parent_category": "NA", 
        "parent_description": None,
        "sub_category": "NA", 
        "sub_description": None,
        "product_name": "NA", 
        "unit": None
    }

    for i, line in enumerate(lines):
        if re.fullmatch(r"\d{2}\.\d{2}", line):
            result["parent_category"] = line.replace(".", "")
            # Look ahead for description
            if i + 1 < len(lines):
                # Usually description follows, unless it's another code
                next_line = lines[i+1]
                if not re.match(r"\d", next_line) and not next_line.startswith("-"):
                     result["parent_description"] = next_line

        elif re.fullmatch(r"\d{4}\.\d{2}", line):
            result["sub_category"] = line.replace(".", "")
            if i + 1 < len(lines):
                next_line = lines[i+1]
                if next_line.startswith("-"):
                    result["sub_description"] = next_line.lstrip("- ").strip()
                elif not re.match(r"\d", next_line):
                    result["sub_description"] = next_line

        elif line.startswith("-"):
            # This is likely the final product name if it hasn't been captured as sub_description
            # Ideally we want the one closest to the end or identifiable as the 10-digit desc
            # But the structure is: 0101.29 -> - - Autres -> 10 00 -> - - - destinés à la boucherie
            # The simple startswith("-") logic in previous version might catch "Autres" as product name if not careful
            # We'll stick to capture it, but maybe refine.
            # In the sample: "10 00" is followed by "- - - destinés..."
            pass

        elif re.fullmatch(r"\d{2} \d{2}", line) or re.fullmatch(r"\d{4}", line):
             # 10 00 part of the 10-digit code (single line)
             if i + 1 < len(lines):
                 next_line = lines[i+1]
                 if next_line.startswith("-"):
                     result["product_name"] = next_line.lstrip("- ").strip()

        elif re.fullmatch(r"\d{2}", line):
             # Could be first part of split code (e.g. 10\n00)
             if i + 1 < len(lines) and re.fullmatch(r"\d{2}", lines[i+1]):
                 # Confirmed split code
                 if i + 2 < len(lines):
                     next_line = lines[i+2]
                     if next_line.startswith("-"):
                         result["product_name"] = next_line.lstrip("- ").strip()

        elif line.isalpha() and len(line) <= 3:
            result["unit"] = line
            
    return result

def parse_annual_volumes(raw_text: str):
    # This might need updating if we strictly use schemas, but for now returned dict/list is fine
    # if we wrap it later.
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
    
    # Fallback if specific headers weren't found or parsing failed
    history = []
    # Zip safely
    for y, w in zip(years, weights):
        history.append({"year": y, "weight_kg": w})
        
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

def parse_documents(raw_text: str):
    from .schemas import DocumentItem
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    docs = []
    start_parsing = False
    buffer = []
    
    # Identify table start
    for line in lines:
        if "Emetteur" in line:
            start_parsing = True
            continue
        
        if start_parsing:
            buffer.append(line)
            
    # Process buffer in chunks of 3 (Code, Name, Issuer)
    seen_docs = set()
    for i in range(0, len(buffer), 3):
        if i + 2 < len(buffer):
            code = buffer[i]
            name = buffer[i+1]
            issuer = buffer[i+2]
            
            # Simple deduplication based on unique tuple
            doc_key = (code, name, issuer)
            if doc_key not in seen_docs:
                docs.append(DocumentItem(code=code, name=name, issuer=issuer, raw=f"{code} {name} {issuer}"))
                seen_docs.add(doc_key)
            
    return docs

def parse_agreements(raw_text: str):
    from .schemas import AccordItem
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    
    agreements = []
    seen_agreements = set()
    start_parsing = False
    
    for line in lines:
        # Skip headers and noise
        if "( en % )" in line:
            start_parsing = True
            continue
            
        if line.strip() in ["DI", "TPI", "Accords"]:
            continue
            
        if start_parsing:
            # Attempt to split by last two tokens (rates)
            # Regex to capture: Name + Rate1 + Rate2
            # Rate can be number or (*) or 0%
            match = re.search(r"^(.*?)\s+([\d,\.]+%?|\(\*\))\s+([\d,\.]+%?|\(\*\))$", line)
            if match:
                name = match.group(1).strip()
                di_rate = match.group(2)
                tpi_rate = match.group(3)
                
                agr_key = (name, di_rate, tpi_rate)
                if agr_key not in seen_agreements:
                    agreements.append(AccordItem(
                        country=name, 
                        DI=di_rate,
                        TPI=tpi_rate,
                        raw=line
                    ))
                    seen_agreements.add(agr_key)
            else:
                # Fallback if regex fails
                # Note: This fallback might be too broad, but we'll keep it simple
                if line and len(line) > 3 and line not in seen_agreements:
                     # Attempt to parse lines that might be formatted differently or just country names
                     # For now, if we can't parse rates, we might just skip or add as raw
                     pass
                
    return agreements

def parse_taxation_from_text(raw_text: str):
    from .schemas import TaxItem
    # Simple regex based parser for standard taxation lines if they follow "Code Label Rate" pattern
    # This is a placeholder as taxation structure varies 
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    taxes = []
    
    for line in lines:
        # Example pattern: DI Droit d'importation 2.5%
        # Or: TVA Taxe sur la valeur ajoutée 20%
        # Needs refinement based on actual text
        match = re.match(r"^([A-Z]+)\s+(.*?)\s+([\d,\.]+\s?%|Exonéré|\(\*\))$", line)
        if match:
            taxes.append(TaxItem(
                code=match.group(1),
                label=match.group(2).strip(),
                raw=match.group(3)
            ))
    return taxes


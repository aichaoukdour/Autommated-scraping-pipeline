import re

def clean_text(text):
    if not text:
        return ""
    # Remove excessive whitespace
    return ' '.join(text.split())

def parse_percentage(text):
    match = re.search(r'(\d+(?:,\d+)?)\s*%', text)
    if match:
        return float(match.group(1).replace(',', '.'))
    return 0.0

def parse_product_info(text):
    info = {}
    if not text:
        return info
    
    # Extract header info
    hs_match = re.search(r'position tarifaire\s*:\s*([\d\.]+)', text)
    if hs_match:
        info['hs_code'] = hs_match.group(1)

    # Extract description
    # Usually after "Description du Produit Remarquable :" and before "Entrée en vigueur" or stars
    desc_match = re.search(r'Description du Produit Remarquable\s*:\s*(.*?)(?:\*|\sEntrée en vigueur)', text)
    if desc_match:
        info['description'] = clean_text(desc_match.group(1).replace('( Source : Office des Changes )', ''))
        
    return info

def parse_duties_taxes(text):
    data = {'duties': [], 'taxes': []}
    if not text:
        return data

    # Extract Droit d'Importation (DI)
    di_match = re.search(r"Droit d'Importation\*\s*\(\s*DI\s*\)\s*:\s*([\d,]+)\s*%", text)
    if di_match:
        data['duties'].append({
            'name': "Droit d'Importation",
            'rate': float(di_match.group(1).replace(',', '.'))
        })

    # Extract TVA
    tva_match = re.search(r"Taxe sur la Valeur Ajoutée.*?\(\s*TVA\s*\)\s*:\s*([\d,]+)\s*%", text)
    if tva_match:
        data['taxes'].append({
            'name': "TVA",
            'rate': float(tva_match.group(1).replace(',', '.'))
        })
        
    # Extract TPI (Taxe Parafiscale)
    tpi_match = re.search(r"Taxe Parafiscale.*?\(\s*TPI\s*\)\s*:\s*([\d,]+)\s*%", text)
    if tpi_match:
         data['taxes'].append({
            'name': "Taxe Parafiscale",
            'rate': float(tpi_match.group(1).replace(',', '.'))
        })
    
    return data

def parse_documents(text):
    docs = []
    if not text:
        return docs
    
    # Simple strategy: Look for pattern "Code document Document Emetteur" -> 06002 ...
    # This might be tricky with just regex on a flat string, but let's try finding the start of the table
    # The text usually flows like: ... Emetteur 06002 Résultat du contrôle. ONSSA ...
    
    # Find all sequences that look like a doc code (5 digits) followed by text until the next code or end
    # This is a heuristic and might need refinement based on more data
    # Regex: (Code) (Description) (Emitter is usually at the end of description, hard to separate without table structure)
    
    # Using a catch-all strategy for now: extract numeric codes
    matches = re.finditer(r'(\d{5})\s+(.*?)(?=\d{5}|$)', text)
    for m in matches:
        code = m.group(1)
        rest = m.group(2).strip()
        
        # Heuristic: The last word might be the issuer (e.g., ONSSA, MCI) if it's uppercase
        words = rest.split()
        issuer = words[-1] if words and words[-1].isupper() and len(words[-1]) > 1 else "Unknown"
        name = ' '.join(words[:-1]) if issuer != "Unknown" else rest
        
        docs.append({
            'code': code,
            'name': name,
            'issuer': issuer
        })
        
    return docs

def parse_agreements(text):
    agreements = []
    if not text:
        return agreements
        
    # Pattern: COUNTRY BENEFIT RATE ...
    # e.g., ALGERIE FRANCHISE 0 0
    # This is quite unstructured in the flat text.
    # We will look for known country names or zones if possible, or just generic "FRANCHISE" patterns
    
    # Let's extract anything that looks like "Country FRANCHISE Rate"
    # Or "Country ... Rate %"
    
    # Strategy: Split by known keywords or structure? 
    # Let's try splitting by "FRANCHISE" or percentages
    
    # For now, let's just extract the raw text segments that contain "FRANCHISE" or specific rates
    # Better: list known agreements if we had them.
    # Fallback: Return raw text segment for agreements for now to avoid data loss
    
    if "Accords Liste" in text:
        start = text.find("Accords Liste") + len("Accords Liste")
        content = text[start:].strip()
        agreements.append({'raw_text': content})
        
    return agreements

def clean_and_structure_data(raw_data_list):
    """
    Main ETL function.
    raw_data_list expected indices:
    0: Product Info
    1: Sidebar (ignore)
    2: Duties & Taxes
    3: Documents
    4: Agreements
    5: History
    """
    if not raw_data_list or len(raw_data_list) < 6:
        return {}

    structured_data = {}
    
    # 1. Product Info
    structured_data['product_info'] = parse_product_info(raw_data_list[0])
    
    # 2. Duties & Taxes
    dt_data = parse_duties_taxes(raw_data_list[2])
    structured_data['duties'] = dt_data['duties']
    structured_data['taxes'] = dt_data['taxes']
    
    # 3. Documents
    structured_data['documents'] = parse_documents(raw_data_list[3])
    
    # 4. Agreements
    structured_data['agreements'] = parse_agreements(raw_data_list[4])
    
    # 5. History (Optional, just keep raw or parse last date)
    # structured_data['history'] = ...
    
    return structured_data

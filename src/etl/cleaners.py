from ftfy import fix_text
from cleantext import clean
import unicodedata
from babel.numbers import parse_decimal
from typing import Optional
from dateparser import parse as dateparse
import re

def clean_text_block(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = fix_text(text)
    text = unicodedata.normalize("NFKC", text)
    text = clean(text, fix_unicode=True, to_ascii=False, strip_lines=True,
                 no_line_breaks=False, lower=False)
    return " ".join(text.split()).strip() or None

def parse_percentage(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        value = value.replace("%", "").strip()
        return float(parse_decimal(value, locale="fr_FR"))
    except Exception:
        return None

def parse_french_date(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    dt = dateparse(text, languages=["fr"])
    return dt.date().isoformat() if dt else None

def remove_adil_boilerplate(text: Optional[str]) -> Optional[str]:
    """Removes standard ADiL headers/footers from text."""
    if not text:
        return None
        
    boilerplate = [
        "ADiL", "Nomenclature douanière.", "Position tarifaire :", 
        "Source : ADII", "Source :", "Office des Changes", 
        "DESIGNATION DU PRODUIT :", "Codification", 
        "Désignation du Produit", "dans le Système Harmonisé", 
        "Unité", "de Quantité Normalisée",
        "Situation du :"
    ]
    
    clean_text = text
    for phrase in boilerplate:
        clean_text = clean_text.replace(phrase, "")
        
    # Remove timestamps like 07/01/2026 23:00:56
    clean_text = re.sub(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}", "", clean_text)
    
    return clean_text_block(clean_text)

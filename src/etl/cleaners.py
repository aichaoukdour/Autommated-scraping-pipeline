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
    """Removes standard ADiL headers/footers and navigational debris from text."""
    if not text:
        return None
        
    boilerplate = [
        "ADiL", "Nomenclature douanière.", "Position tarifaire :", 
        "Source : ADII", "Source :", "Office des Changes", 
        "DESIGNATION DU PRODUIT :", "Codification", 
        "Désignation du Produit", "dans le Système Harmonisé", 
        "Unité", "de Quantité Normalisée",
        "Situation du :", "Situation pour l'année :",
        "Droits et Taxes à l'Import.", "Documents et Normes à l'Import.",
        "Accords et Conventions.", "Volume annuel d'Importation.",
        "Volume annuel d'Exportation.", "Principaux Pays Fournisseurs.",
        "Principaux Pays Clients.", "Classification Nationale des Echanges Commerciaux",
        "Classification Internationale des Echanges Commerciaux",
        "Opérateurs Economiques : Importateurs.", "Opérateurs Economiques : Exportateurs.",
        "Graphique et Tableau :", "Graphique et Tableaux :",
        "Merci de patienter quelques instants...",
        "Période statistique annuelle :", "Période statistique :",
        "Intercom :", "Coût Assurance Fret", "Franco à Bord",
        "Unité de mesure :", "KGS / Année", "KGS / PAYS", "KGS / Année",
        "KGS", "PAYS", "DATE", "Année", "Poids", "Pays", "Période",
        "Nouveau Produit Remarquable :", "Groupement d'utilisation :",
        "Nomenclature Marocaine des Produits \"NMP\"",
        "Nomenclature de la Comptabilité Nationale \"NCN\"",
        "Classification Type pour le Commerce International \"CTCI\"",
        "Vous êtes au niveau de la position tarifaire :",
        "Description du Produit Remarquable :",
        "Description du Nouveau Produit Remarquable :",
        "Entrée en vigueur le :", "LES DOCUMENTS EXIGIBLES",
        "N° document", "Document", "Emetteur", "Accords", "Liste",
        "DI ( en % )", "TPI ( en % )", "(*) Taux du Régime du Droit Commun",
        "Vous êtes sur la page", "sur", "au total", "Page Suivante",
        "La douane marocaine vous donne des droits d’opposition, de rectification et de suppression."
    ]
    
    clean_text = text
    # Case-insensitive replacement for some common ones
    for phrase in boilerplate:
        reg = re.compile(re.escape(phrase), re.IGNORECASE)
        clean_text = reg.sub("", clean_text)
        
    # Remove timestamps like 07/01/2026 23:00:56 or 20/01/2026 17:22:55
    clean_text = re.sub(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}", "", clean_text)
    # Remove dates like "vendredi 2 janvier 2015" (optional, might be useful)
    # clean_text = re.sub(r"(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+\d+\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}", "", clean_text, flags=re.IGNORECASE)
    
    # Remove excessive asterisks and dashes
    clean_text = re.sub(r"[*]{2,}", "", clean_text)
    clean_text = re.sub(r"[-]{3,}", "", clean_text)
    
    return clean_text_block(clean_text)

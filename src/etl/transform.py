import re
from ftfy import fix_text
from datetime import datetime

from cleaners import clean_text_block, parse_percentage
from schemas import HSCodeGold


def parse_hs_hierarchy(raw_text: str, hs_code: str) -> dict:
    """
    Parse HS hierarchy dynamically (4 / 6 / 8 / 10 digits).
    Missing levels are filled with 'NA'.
    """
    text = fix_text(raw_text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    result = {
        "parent_category": "NA",
        "sub_category": "NA",
        "product_name": "NA",
        "unit": None,
    }

    i = 0
    while i < len(lines):
        line = lines[i]

        # ---- 4 DIGIT (01.01) ----
        if re.fullmatch(r"\d{2}\.\d{2}", line):
            result["parent_category"] = line.replace(".", "")
            i += 2
            continue

        # ---- 6 DIGIT (0101.21) ----
        if re.fullmatch(r"\d{4}\.\d{2}", line):
            result["sub_category"] = line.replace(".", "")
            i += 1
            continue

        # ---- PRODUCT DESIGNATION (10-digit) ----
        if line.startswith("-"):
            result["product_name"] = line.lstrip("- ").strip()
            i += 1
            continue

        # ---- UNIT ----
        if line.isalpha() and len(line) <= 3:
            result["unit"] = line
            i += 1
            continue

        i += 1

    return result


def transform(raw: dict) -> dict:
    hs_code = raw["hs_code"]
    snapshot_date = raw["scraped_at"][:10]

    sections = raw["sections"]

    taxes = {}
    import_history = []
    export_history = []
    duty_history = []
    suppliers = []
    clients = []

    national_text = None
    international_text = None

    section_name = chapter_name = None
    parent_category = sub_category = None
    product_name = product_designation = unit = None
    legal_text = None

    for s in sections:
        name = s["section_name"]
        content = s["content"]

        # ---- POSITION TARIFAIRE ----
        if name in ["Position tarifaire", "Version papier"]:
            raw_text = content["raw_text"]
            product_designation = clean_text_block(raw_text)

            section_name = content["key_values"].get("SECTION")
            chapter_name = content["key_values"].get("CHAPITRE")

            hs_parts = parse_hs_hierarchy(raw_text, hs_code)

            parent_category = hs_parts["parent_category"]
            sub_category = hs_parts["sub_category"]
            product_name = hs_parts["product_name"]
            unit = hs_parts["unit"]

        # ---- TAXES ----
        elif name == "Droits et Taxes":
            kv = content["key_values"]
            taxes["import_duty_rate"] = parse_percentage(kv.get("- Droit d'Importation*"))
            taxes["parafiscal_tax_rate"] = parse_percentage(kv.get("- Taxe Parafiscale à l'Importation*"))
            taxes["vat_rate"] = parse_percentage(kv.get("- Taxe sur la Valeur Ajoutée à l'Import."))

        # ---- IMPORT HISTORY ----
        elif name == "Importations":
            year = None
            for l in content["raw_text"].splitlines():
                l = l.strip()
                if l.isdigit():
                    year = int(l)
                elif year and l.replace(" ", "").isdigit():
                    import_history.append({
                        "year": year,
                        "weight_kg": float(l.replace(" ", ""))
                    })

        # ---- EXPORT HISTORY ----
        elif name == "Exportations":
            year = None
            for l in content["raw_text"].splitlines():
                l = l.strip()
                if l.isdigit():
                    year = int(l)
                elif year and l.replace(" ", "").isdigit():
                    export_history.append({
                        "year": year,
                        "weight_kg": float(l.replace(" ", ""))
                    })

        # ---- SUPPLIERS / CLIENTS ----
        elif name == "Fournisseurs":
            suppliers = list(content["key_values"].keys())

        elif name == "Clients":
            clients = list(content["key_values"].keys())

        # ---- CLASSIFICATIONS ----
        elif name == "Nationale":
            national_text = clean_text_block(content["raw_text"])

        elif name == "Internationale":
            international_text = clean_text_block(content["raw_text"])

    gold = HSCodeGold(
        hs_code=hs_code,
        snapshot_date=snapshot_date,
        section_name=section_name,
        chapter_name=chapter_name,
        parent_category=parent_category,
        sub_category=sub_category,
        product_name=product_name,
        product_designation=product_designation,
        unit_of_measure=unit,
        import_duty_rate=taxes.get("import_duty_rate"),
        parafiscal_tax_rate=taxes.get("parafiscal_tax_rate"),
        vat_rate=taxes.get("vat_rate"),
        import_volume_total_kg=sum(i["weight_kg"] for i in import_history) if import_history else None,
        export_volume_total_kg=sum(i["weight_kg"] for i in export_history) if export_history else None,
        import_history=import_history or None,
        export_history=export_history or None,
        import_duty_history=duty_history or None,
        top_supplier_countries=suppliers or None,
        top_client_countries=clients or None,
        legal_text=legal_text,
        national_classification_text=national_text,
        international_classification_text=international_text,
        sources=[raw["main_content"]["metadata"]["source"]]
    )

    return gold.dict()

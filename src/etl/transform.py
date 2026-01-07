from cleaners import clean_text_block, parse_percentage, parse_french_date
from schemas import HSCodeGold
from datetime import datetime


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

    classification_text = []
    national_text = None
    international_text = None

    section_name = chapter_name = parent_category = sub_category = None
    product_name = product_designation = unit = None
    legal_text = None

    for s in sections:
        name = s["section_name"]
        content = s["content"]

        # ---- CLASSIFICATION ----
        if name in ["Position tarifaire", "Version papier"]:
            text = clean_text_block(content["raw_text"])
            classification_text.append(text)

            section_name = content["key_values"].get("SECTION")
            chapter_name = content["key_values"].get("CHAPITRE")
            product_designation = text

        # ---- TAXES ----
        elif name == "Droits et Taxes":
            kv = content["key_values"]
            taxes["import_duty_rate"] = parse_percentage(kv.get("- Droit d'Importation*"))
            taxes["parafiscal_tax_rate"] = parse_percentage(kv.get("- Taxe Parafiscale à l'Importation*"))
            taxes["vat_rate"] = parse_percentage(kv.get("- Taxe sur la Valeur Ajoutée à l'Import."))

        # ---- DUTY HISTORY ----
        elif name == "Historique Droit d'Importation":
            duty_history.append({
                "date": "2015-01-02",
                "rate": 2.5
            })

        # ---- IMPORTS ----
        elif name == "Importations":
            lines = content["raw_text"].splitlines()
            for l in lines:
                if l.strip().isdigit():
                    year = int(l.strip())
                elif " " in l and l.strip().replace(" ", "").isdigit():
                    import_history.append({
                        "year": year,
                        "weight_kg": float(l.replace(" ", ""))
                    })

        # ---- EXPORTS ----
        elif name == "Exportations":
            lines = content["raw_text"].splitlines()
            for l in lines:
                if l.strip().isdigit():
                    year = int(l.strip())
                elif " " in l and l.strip().replace(" ", "").isdigit():
                    export_history.append({
                        "year": year,
                        "weight_kg": float(l.replace(" ", ""))
                    })

        # ---- SUPPLIERS ----
        elif name == "Fournisseurs":
            suppliers = list(content["key_values"].keys())

        # ---- CLIENTS ----
        elif name == "Clients":
            clients = list(content["key_values"].keys())

        # ---- NATIONAL / INTERNATIONAL ----
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

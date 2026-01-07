from schemas import HSCodeGold, DutyHistory
from cleaners import clean_text_block, parse_percentage, parse_french_date
from hs_parser import parse_hs_hierarchy, parse_annual_volumes, parse_suppliers_or_clients
import re

def transform(raw: dict) -> dict:
    hs_code = raw["hs_code"]
    snapshot_date = raw["scraped_at"][:10]
    sections = raw.get("sections", [])

    taxes = {}
    import_history = []
    export_history = []
    duty_history = []
    suppliers = []
    clients = []

    national_text = None
    international_text = None
    legal_text = None
    section_name = chapter_name = None
    parent_category = sub_category = None
    product_name = product_designation = None
    unit = None
    sources = []

    for s in sections:
        name = s.get("section_name")
        content = s.get("content", {})
        raw_text = content.get("raw_text", "")

        if name in ["Position tarifaire", "Version papier"]:
            hs_parts = parse_hs_hierarchy(raw_text)
            parent_category = hs_parts["parent_category"]
            sub_category = hs_parts["sub_category"]
            product_name = hs_parts["product_name"]
            unit = hs_parts["unit"]
            section_name = content.get("key_values", {}).get("SECTION")
            chapter_name = content.get("key_values", {}).get("CHAPITRE")
            product_designation = clean_text_block(raw_text)

        elif name == "Droits et Taxes":
            kv = content.get("key_values", {})
            taxes["import_duty_rate"] = parse_percentage(kv.get("- Droit d'Importation*"))
            taxes["parafiscal_tax_rate"] = parse_percentage(kv.get("- Taxe Parafiscale à l'Importation*"))
            taxes["vat_rate"] = parse_percentage(kv.get("- Taxe sur la Valeur Ajoutée à l'Import."))

        elif name == "Importations":
            import_history = parse_annual_volumes(raw_text)
        elif name == "Exportations":
            export_history = parse_annual_volumes(raw_text)

        elif name == "Fournisseurs":
            suppliers = parse_suppliers_or_clients(raw_text)
        elif name == "Clients":
            clients = parse_suppliers_or_clients(raw_text)

        elif name == "Nationale":
            national_text = clean_text_block(raw_text)
        elif name == "Internationale":
            international_text = clean_text_block(raw_text)

        elif "Historique Droit d'Importation" in name:
            matches = re.findall(r"(\d{2}/\d{2}/\d{4})\s*.*?([\d,]+)\s?%", raw_text)
            for date_str, rate_str in matches:
                duty_history.append(DutyHistory(date=parse_french_date(date_str), rate=float(rate_str.replace(",", "."))))

        source = content.get("metadata", {}).get("source")
        if source and source not in sources:
            sources.append(source)

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
        import_volume_total_kg=sum(i.weight_kg for i in import_history) if import_history else None,
        export_volume_total_kg=sum(i.weight_kg for i in export_history) if export_history else None,
        import_history=import_history or None,
        export_history=export_history or None,
        import_duty_history=duty_history or None,
        top_supplier_countries=suppliers or None,
        top_client_countries=clients or None,
        national_classification_text=national_text,
        international_classification_text=international_text,
        legal_text=legal_text,
        sources=sources
    )

    return gold.dict()

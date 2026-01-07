from schemas import (
    HSProduct, Identity, HSLevels, ProductDetails, Classification, CodeLabel, HierarchyItem,
    Taxation, CurrentTaxation, TaxHistoryItem, TradeStatistics, TradeFlow, AnnualVolume, TradePeriod,
    Geography, GeoFlow, CountryFlow, LegalAndStatisticalTexts, LegalTextItem, DataAvailability, Lineage, SectionStats
)
from cleaners import clean_text_block, parse_percentage, parse_french_date, remove_adil_boilerplate
from hs_parser import parse_hs_hierarchy, parse_annual_volumes, parse_suppliers_or_clients
import re
from datetime import datetime

def transform(raw: dict) -> dict:
    hs_code = raw["hs_code"]
    snapshot_date = raw["scraped_at"][:10]
    scraped_at = raw["scraped_at"]
    sections = raw.get("sections", [])
    
    # --- 1. Identity & Levels ---
    # raw code: 0101291000
    hs2 = hs_code[:2]
    hs4 = hs_code[:4]
    hs6 = hs_code[:6]
    hs8 = hs_code[:8]
    hs10 = hs_code
    
    identity = Identity(
        hs_code=hs_code,
        hs_levels=HSLevels(hs2=hs2, hs4=hs4, hs6=hs6, hs8=hs8, hs10=hs10)
    )

    # --- Container Variables ---
    taxes_map = {}
    duty_history = []
    import_history = []
    export_history = []
    suppliers = []
    clients = []
    
    section_label = "NA"
    chapter_label = "NA"
    
    parent_category = None
    parent_description = None 
    sub_category = None
    sub_description = None
    product_name = None
    unit = None
    
    entry_into_force = None
    legal_text_blob = None # from "Description du Nouveau Produit Remarquable"
    
    national_text = None
    national_source = "NA"
    international_text = None
    international_source = "NA"
    
    # Metadata tracking
    data_avail = {
        "documents_and_norms": False,
        "agreements_and_conventions": False,
        "importers": False,
        "exporters": False
    }
    
    gold_docs = []
    gold_agreements = []
    
    all_sources = set()

    # --- 2. Extract Global Keys (Entry into force, etc) ---
    main_content = raw.get("main_content", {})
    for k, v in main_content.get("key_values", {}).items():
        if "Entrée en vigueur le" in k:
            entry_into_force = parse_french_date(v) # or raw string if preferred
        if "Description du Nouveau Produit Remarquable" in k:
            legal_text_blob = v
            
    # --- 3. Parse Sections ---
    for s in sections:
        name = s.get("section_name")
        content = s.get("content", {})
        raw_text = content.get("raw_text", "")
        
        # Track sources
        src = content.get("metadata", {}).get("source")
        if src: all_sources.add(src)

        if name in ["Position tarifaire", "Version papier"]:
            hs_parts = parse_hs_hierarchy(raw_text)
            parent_category = hs_parts["parent_category"]
            parent_description = hs_parts["parent_description"]
            sub_category = hs_parts["sub_category"]
            sub_description = hs_parts["sub_description"]
            product_name = hs_parts["product_name"]
            unit = hs_parts["unit"]
            
            section_label = content.get("key_values", {}).get("SECTION", "NA")
            chapter_label = content.get("key_values", {}).get("CHAPITRE", "NA")

        elif name == "Droits et Taxes":
            kv = content.get("key_values", {})
            taxes_map["import_duty"] = parse_percentage(kv.get("- Droit d'Importation*"))
            taxes_map["parafiscal"] = parse_percentage(kv.get("- Taxe Parafiscale à l'Importation*"))
            taxes_map["vat"] = parse_percentage(kv.get("- Taxe sur la Valeur Ajoutée à l'Import."))

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
            national_source = src or "NA"
        elif name == "Internationale":
            international_text = clean_text_block(raw_text)
            international_source = src or "NA"

        elif name == "Documents et Normes":
            if "non disponible" not in raw_text.lower():
                from hs_parser import parse_documents
                docs = parse_documents(raw_text)
                if docs: 
                    data_avail["documents_and_norms"] = True
                    gold_docs = docs

        elif name == "Accords et Convention":
             if "non disponible" not in raw_text.lower():
                 from hs_parser import parse_agreements
                 agrs = parse_agreements(raw_text)
                 if agrs: 
                     data_avail["agreements_and_conventions"] = True
                     gold_agreements = agrs
                 
        elif name == "Importateurs":
             if "non précisé" not in raw_text.lower():
                 data_avail["importers"] = True
                 
        elif name == "Exportateurs":
            if "non précisé" not in raw_text.lower():
                 data_avail["exporters"] = True

        elif "Historique Droit d'Importation" in name:
            # Target pattern: Date \n 02/01/2015 \n Taux \n 10 %
            matches = re.findall(r"(\d{2}/\d{2}/\d{4})\s+(?:Taux\s+)?(\d+(?:,\d+)?)\s?%", raw_text)
            for date_str, rate_str in matches:
                # Filter out metadata dates like "Situation du : 07/01/2026"
                d_str = parse_french_date(date_str)
                if d_str and d_str == snapshot_date:
                    continue
                
                if d_str:
                    duty_history.append(TaxHistoryItem(date=d_str, import_duty_rate_percent=float(rate_str.replace(",", "."))))

    # --- 4. Construct Sub-Objects ---
    
    # Product
    # Designation reconstruction for leaf 
    # e.g. "Reproducteurs de race pure" from `product_name` or `sub_description`
    # User example uses "Reproducteurs de race pure" which corresponds to sub-heading or product name
    
    # We will use product_name if available, else sub_description
    final_designation = product_name if product_name and product_name != "NA" else (sub_description or "NA")
    
    product = ProductDetails(
        designation=final_designation,
        description_remarkable=legal_text_blob,
        unit_of_measure=unit,
        entry_into_force_date=entry_into_force
    )
    
    # Classification
    # Needs explicit hierarchy list
    hierarchy = [
        HierarchyItem(level=2, code=hs2, label=chapter_label.split("-")[-1].strip(), present=True),
        HierarchyItem(level=4, code=hs4, label=parent_description or "NA", present=True),
        HierarchyItem(level=6, code=hs6, label=sub_description or "NA", present=True),
        # 8 and 10 usually hidden or inferred
        HierarchyItem(level=8, code=hs8, label=None, present=False),
        HierarchyItem(level=10, code=hs10, label=final_designation, present=True)
    ]
    
    classification = Classification(
        section=CodeLabel(code=section_label.split("-")[0].strip(), label=section_label.split("-")[-1].strip()),
        chapter=CodeLabel(code=chapter_label.split("-")[0].strip(), label=chapter_label.split("-")[-1].strip()),
        hs_hierarchy=hierarchy
    )
    
    # Taxation
    taxation = Taxation(
        current=CurrentTaxation(
            effective_date=snapshot_date,
            import_duty_rate_percent=taxes_map.get("import_duty"),
            parafiscal_tax_rate_percent=taxes_map.get("parafiscal"),
            vat_rate_percent=taxes_map.get("vat"),
            eligible_for_franchise=True if taxes_map.get("import_duty", 0) == 0 else False,
            source="ADII"
        ),
        history=duty_history
    )
    
    # Statistics
    stats_imports = None
    if import_history:
        stats_imports = TradeFlow(
            period=TradePeriod(from_year=min(h.year for h in import_history), to_year=max(h.year for h in import_history)),
            unit="KGS", incoterm="CIF", source="Office des Changes",
            annual_volumes=[AnnualVolume(year=h.year, weight_kg=h.weight_kg) for h in import_history]
        )
        
    stats_exports = None
    if export_history:
        stats_exports = TradeFlow(
            period=TradePeriod(from_year=min(h.year for h in export_history), to_year=max(h.year for h in export_history)),
            unit="KGS", incoterm="FOB", source="Office des Changes",
            annual_volumes=[AnnualVolume(year=h.year, weight_kg=h.weight_kg) for h in export_history]
        )
        
    trade_stats = TradeStatistics(imports=stats_imports, exports=stats_exports)
    
    # Geography
    geo_suppliers = None
    if suppliers:
        geo_suppliers = GeoFlow(
            year=2022, unit="KGS", incoterm="CIF", source="Office des Changes",
            countries=[CountryFlow(country=s["country"], weight_kg=s["weight_kg"]) for s in suppliers]
        )
    
    geo_clients = None
    if clients:
        geo_clients = GeoFlow(
            year=2022, unit="KGS", incoterm="FOB", source="Office des Changes",
            countries=[CountryFlow(country=c["country"], weight_kg=c["weight_kg"]) for c in clients]
        )
        
    geography = Geography(suppliers=geo_suppliers, clients=geo_clients)
    
    # Legal Texts
    legal_texts = LegalAndStatisticalTexts(
        national_classification=LegalTextItem(source=national_source, snapshot_date=snapshot_date, text=national_text) if national_text else None,
        international_classification=LegalTextItem(source=international_source, snapshot_date=snapshot_date, text=international_text) if international_text else None
    )
    
    # Lineage
    lineage = Lineage(
        scraped_at=scraped_at,
        scrape_status="success",
        sections=SectionStats(total=len(sections), successful=len(sections), failed=0),
        sources=list(all_sources)
    )
    
    # Data Availability
    da = DataAvailability(**data_avail)
    
    # Assemble
    gold = HSProduct(
        identity=identity,
        product=product,
        classification=classification,
        taxation=taxation,
        trade_statistics=trade_stats,
        geography=geography,
        legal_and_statistical_texts=legal_texts,
        documents=gold_docs or None,
        agreements=gold_agreements or None,
        data_availability=da,
        lineage=lineage
    )
    
    return gold.dict()

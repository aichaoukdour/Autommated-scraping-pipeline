from pydantic import BaseModel
from typing import List, Optional, Any

# --- Sub-models ---

class HSLevels(BaseModel):
    hs2: str
    hs4: str
    hs6: str
    hs8: str
    hs10: str

class Identity(BaseModel):
    hs_code: str
    hs_levels: HSLevels

class ProductDetails(BaseModel):
    designation: str
    description_remarkable: Optional[str]
    unit_of_measure: Optional[str]
    entry_into_force_date: Optional[str]

class CodeLabel(BaseModel):
    code: str
    label: str

class HierarchyItem(BaseModel):
    level: int
    code: str
    label: Optional[str]
    present: bool

class Classification(BaseModel):
    section: CodeLabel
    chapter: CodeLabel
    hs_hierarchy: List[HierarchyItem]

class CurrentTaxation(BaseModel):
    effective_date: str
    import_duty_rate_percent: Optional[float]
    parafiscal_tax_rate_percent: Optional[float]
    vat_rate_percent: Optional[float]
    eligible_for_franchise: bool = False # Default logic or derived
    source: str

class TaxHistoryItem(BaseModel):
    date: str
    import_duty_rate_percent: float
    source: str = "ADII"

class Taxation(BaseModel):
    current: CurrentTaxation
    history: List[TaxHistoryItem]

class AnnualVolume(BaseModel):
    year: int
    weight_kg: float

class TradePeriod(BaseModel):
    from_year: int
    to_year: int

class TradeFlow(BaseModel):
    period: TradePeriod
    unit: str
    incoterm: str
    annual_volumes: List[AnnualVolume]
    source: str

class TradeStatistics(BaseModel):
    imports: Optional[TradeFlow]
    exports: Optional[TradeFlow]

class CountryFlow(BaseModel):
    country: str
    weight_kg: float

class GeoFlow(BaseModel):
    year: int
    unit: str
    incoterm: str
    countries: List[CountryFlow]
    source: str

class Geography(BaseModel):
    suppliers: Optional[GeoFlow]
    clients: Optional[GeoFlow]

class LegalTextItem(BaseModel):
    source: str
    snapshot_date: str
    language: str = "fr"
    text: Optional[str]

class LegalAndStatisticalTexts(BaseModel):
    national_classification: Optional[LegalTextItem]
    international_classification: Optional[LegalTextItem]
    # We can add legal_text here if needed as a strictly legal doc?
    # User example put these under legal_and_statistical_texts

class DataAvailability(BaseModel):
    documents_and_norms: bool
    agreements_and_conventions: bool
    importers: bool
    exporters: bool

class SectionStats(BaseModel):
    total: int
    successful: int
    failed: int

class Lineage(BaseModel):
    scraped_at: str
    scrape_status: str
    sections: SectionStats
    sources: List[str]

class Document(BaseModel):
    code: str
    name: str
    issuer: str

class Agreement(BaseModel):
    country: str
    preference: str

# --- Root Model ---

class HSProduct(BaseModel):
    entity_type: str = "hs_product"
    identity: Identity
    product: ProductDetails
    classification: Classification
    taxation: Taxation
    trade_statistics: TradeStatistics
    geography: Geography
    legal_and_statistical_texts: LegalAndStatisticalTexts
    documents: Optional[List[Document]]
    agreements: Optional[List[Agreement]]
    data_availability: DataAvailability
    lineage: Lineage

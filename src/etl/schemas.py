from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class ImportExportHistory(BaseModel):
    year: int
    weight_kg: float


class DutyHistory(BaseModel):
    date: str
    rate: float


class HSCodeGold(BaseModel):
    hs_code: str
    snapshot_date: str

    section_name: Optional[str]
    chapter_name: Optional[str]
    parent_category: Optional[str]
    sub_category: Optional[str]
    product_name: Optional[str]
    product_designation: Optional[str]
    unit_of_measure: Optional[str]

    import_duty_rate: Optional[float]
    parafiscal_tax_rate: Optional[float]
    vat_rate: Optional[float]

    import_volume_total_kg: Optional[float]
    export_volume_total_kg: Optional[float]

    import_history: Optional[List[ImportExportHistory]]
    export_history: Optional[List[ImportExportHistory]]
    import_duty_history: Optional[List[DutyHistory]]

    top_supplier_countries: Optional[List[str]]
    top_client_countries: Optional[List[str]]

    legal_text: Optional[str]
    national_classification_text: Optional[str]
    international_classification_text: Optional[str]

    sources: List[str]

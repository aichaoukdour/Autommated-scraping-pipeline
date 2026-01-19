from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union

# --- Common Meta & Lineage ---

class Meta(BaseModel):
    note: Optional[str] = None
    source: Optional[str] = "ADII"
    parser_version: Optional[str] = None
    scraped_at: Optional[str] = None
    lang: Optional[str] = "fr"
    encoding_fixed: Optional[bool] = None
    original_label_raw: Optional[str] = None
    snapshot_date: Optional[str] = None
    edition: Optional[str] = None

class HttpInfo(BaseModel):
    status_code: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None

class PipelineInfo(BaseModel):
    scraper: Optional[str] = "selenium-scraper"
    parser_version: Optional[str] = "v1.0"
    schema_version: Optional[str] = "v1.0"

class QualityInfo(BaseModel):
    encoding_fixed: Optional[bool] = None
    missing_sections: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

class ErrorInfo(BaseModel):
    stage: str
    message: str
    raw: Optional[Dict[str, Any]] = None

class Lineage(BaseModel):
    scraped_at: Optional[str] = None
    status: Optional[str] = None  # success, failed, partial, stale
    url: Optional[str] = None
    http: HttpInfo = Field(default_factory=HttpInfo)
    pipeline: PipelineInfo = Field(default_factory=PipelineInfo)
    quality: QualityInfo = Field(default_factory=QualityInfo)
    sources: List[str] = Field(default_factory=list)
    errors: List[ErrorInfo] = Field(default_factory=list)

# --- Taxation ---

class TaxItem(BaseModel):
    code: str  # ex: "DI", "TPI", "TVA"
    label: Optional[str] = None
    raw: Optional[str] = None

class TaxationSection(BaseModel):
    taxes: List[TaxItem] = Field(default_factory=list)
    meta: Meta = Field(default_factory=Meta)

# --- Documents ---

class DocumentItem(BaseModel):
    code: str
    name: str
    issuer: str
    raw: Optional[str] = None

class DocumentsSection(BaseModel):
    documents: List[DocumentItem] = Field(default_factory=list)
    meta: Meta = Field(default_factory=Meta)

# --- Agreements (Accords) ---

class AccordItem(BaseModel):
    country: str
    Liste: Optional[str] = ""
    DI: str = "0%"
    TPI: str = "0%"
    raw: Optional[str] = None

class AccordConventionSection(BaseModel):
    accord_convention: List[AccordItem] = Field(default_factory=list)
    meta: Meta = Field(default_factory=Meta)

# --- History ---

class HistoryItem(BaseModel):
    date: str
    raw: Optional[str] = None

class HistorySection(BaseModel):
    items: List[HistoryItem] = Field(default_factory=list)
    meta: Meta = Field(default_factory=Meta)

# --- Other Sections (Generic or Specific) ---

class GenericSection(BaseModel):
    content: Dict[str, Any]
    meta: Meta = Field(default_factory=Meta)

# --- Root Model ---

class HSProduct(BaseModel):
    hs_code: str
    lineage: Lineage
    
    # Hierarchy Labels
    section_label: Optional[str] = None
    chapter_label: Optional[str] = None
    hs4_label: Optional[str] = None
    hs6_label: Optional[str] = None
    hs8_label: Optional[str] = None
    designation: Optional[str] = None
    
    # Sections
    taxation: Optional[TaxationSection] = None
    documents: Optional[DocumentsSection] = None
    accord_convention: Optional[AccordConventionSection] = None
    historique: Optional[HistorySection] = None
    
    # Generic bucket for other sections like classification, geography etc. if needed
    other_data: Optional[Dict[str, Any]] = Field(default_factory=dict)


"""
Pydantic models for API requests and responses
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

from ...domain.entities import ScrapedData, SectionData


class TariffCodeResponse(BaseModel):
    """Response model for tariff code data"""
    tariff_code: str
    product_description: Optional[str] = None
    effective_date: Optional[str] = None
    scraped_at: str
    section_count: int
    sections: List[str]
    
    @classmethod
    def from_scraped_data(cls, data: ScrapedData) -> "TariffCodeResponse":
        return cls(
            tariff_code=data.tariff_code_searched,
            product_description=data.basic_info.product_description,
            effective_date=data.basic_info.effective_date,
            scraped_at=data.scraped_at.isoformat(),
            section_count=len(data.sections),
            sections=list(data.sections.keys())
        )


class TariffCodeListResponse(BaseModel):
    """Response model for list of tariff codes"""
    codes: List[str]
    total: int
    skip: int
    limit: int


class SectionResponse(BaseModel):
    """Response model for section data"""
    section_name: str
    section_type: str
    tables: List[Dict[str, Any]] = Field(default_factory=list)
    lists: List[List[str]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    
    @classmethod
    def from_section_data(cls, section_name: str, section: SectionData) -> "SectionResponse":
        return cls(
            section_name=section_name,
            section_type=section.section_type.value,
            tables=section.structured_data.tables,
            lists=section.structured_data.lists,
            metadata=section.structured_data.metadata.to_dict(),
            error=section.error
        )


class ScrapeRequest(BaseModel):
    """Request model for triggering a scrape"""
    force: bool = Field(default=False, description="Force scrape even if cached")


class ScrapeResponse(BaseModel):
    """Response model for scrape task"""
    tariff_code: str
    task_id: str
    status: str
    message: str


class StatsResponse(BaseModel):
    """Response model for pipeline statistics"""
    total_records: int
    unique_codes: int
    latest_scrape: Optional[str] = None
    first_scrape: Optional[str] = None


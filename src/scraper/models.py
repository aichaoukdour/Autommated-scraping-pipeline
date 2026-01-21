from dataclasses import dataclass
from typing import Dict, List, Optional, Any

@dataclass
class ContentData:
    """Structured content data"""
    raw_text: str
    metadata: Dict[str, str]
    key_values: Dict[str, str]
    tables: List[Dict[str, Any]] 
    length: int

@dataclass
class SectionData:
    """Section scraping result"""
    section_name: str
    section_type: str
    content: ContentData
    scraped_at: str
    order: int
    status: Optional[str] = None

@dataclass
class ScrapeResult:
    """Complete scraping result for an HS code"""
    hs_code: str
    scraped_at: str
    scrape_status: str
    main_content: Dict[str, Any]
    sections: List[Dict[str, Any]]
    summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

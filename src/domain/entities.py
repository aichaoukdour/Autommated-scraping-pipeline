"""
Domain Entities - Core business objects
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum


class SectionType(str, Enum):
    """Types of sections that can be scraped"""
    POSITION_TARIFFAIRE = "Position tarifaire"
    DROITS_ET_TAXES = "Droits et taxes"
    DOCUMENTS = "Documents"
    ACCORDS = "Accords"
    HISTORIQUE = "Historique"
    OTHER = "Other"


@dataclass
class Metadata:
    """Metadata key-value pairs"""
    data: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
    
    def to_dict(self) -> Dict[str, Any]:
        return self.data


@dataclass
class BasicInfo:
    """Basic product information"""
    tariff_code: Optional[str] = None
    product_description: Optional[str] = None
    effective_date: Optional[str] = None
    metadata: Metadata = field(default_factory=Metadata)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'tariff_code': self.tariff_code,
            'product_description': self.product_description,
            'effective_date': self.effective_date,
            'metadata': self.metadata.to_dict()
        }


@dataclass
class StructuredData:
    """Structured data from a section"""
    metadata: Metadata = field(default_factory=Metadata)
    tables: List[Dict[str, Any]] = field(default_factory=list)
    lists: List[List[str]] = field(default_factory=list)
    section_specific: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'metadata': self.metadata.to_dict(),
            'tables': self.tables,
            'lists': self.lists,
            'section_specific': self.section_specific
        }


@dataclass
class SectionData:
    """Data extracted from a specific section"""
    section_name: str
    section_type: SectionType = SectionType.OTHER
    structured_data: StructuredData = field(default_factory=StructuredData)
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            'section_name': self.section_name,
            'section_type': self.section_type.value,
            'structured_data': self.structured_data.to_dict()
        }
        if self.error:
            result['error'] = self.error
        return result


@dataclass
class ScrapedData:
    """Complete scraped data for a tariff code"""
    tariff_code_searched: str
    basic_info: BasicInfo = field(default_factory=BasicInfo)
    sections: Dict[str, SectionData] = field(default_factory=dict)
    scraped_at: datetime = field(default_factory=datetime.now)
    scraping_duration_seconds: Optional[float] = None
    
    def add_section(self, section_data: SectionData) -> None:
        """Add a section to the scraped data"""
        self.sections[section_data.section_name] = section_data
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'tariff_code_searched': self.tariff_code_searched,
            'basic_info': self.basic_info.to_dict(),
            'sections': {name: section.to_dict() for name, section in self.sections.items()},
            'scraped_at': self.scraped_at.isoformat(),
            'scraping_duration_seconds': self.scraping_duration_seconds
        }


@dataclass
class TariffCode:
    """Tariff code entity"""
    value: str
    
    def __post_init__(self):
        """Validate tariff code format"""
        if not self.value:
            raise ValueError("Tariff code cannot be empty")
        # Remove any non-digit characters for validation
        clean_value = ''.join(c for c in self.value if c.isdigit())
        if len(clean_value) not in [8, 10]:
            raise ValueError(f"Invalid tariff code format: {self.value}")
    
    def __str__(self) -> str:
        return self.value
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, TariffCode):
            return False
        return self.value == other.value
    
    def __hash__(self) -> int:
        return hash(self.value)


"""
Cleaned Data Entities - Transformed and normalized data structures
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal


@dataclass
class CleanedTableRow:
    """A cleaned table row with typed values"""
    row_data: Dict[str, Any] = field(default_factory=dict)
    
    def get_numeric(self, key: str) -> Optional[Decimal]:
        """Get a numeric value from the row"""
        value = self.row_data.get(key)
        if value is None:
            return None
        try:
            # Remove commas and spaces, convert to Decimal
            clean_value = str(value).replace(',', '').replace(' ', '').strip()
            return Decimal(clean_value)
        except (ValueError, TypeError):
            return None
    
    def get_string(self, key: str) -> Optional[str]:
        """Get a cleaned string value"""
        value = self.row_data.get(key)
        if value is None:
            return None
        return str(value).strip()
    
    def get_date(self, key: str) -> Optional[datetime]:
        """Get a parsed date value"""
        value = self.row_data.get(key)
        if value is None:
            return None
        # Date parsing logic will be in the transformer
        return None  # Placeholder


@dataclass
class CleanedTable:
    """A cleaned table with normalized data"""
    name: str
    headers: List[str]
    rows: List[CleanedTableRow]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'headers': self.headers,
            'rows': [row.row_data for row in self.rows],
            'metadata': self.metadata
        }


@dataclass
class CleanedSection:
    """Cleaned section data"""
    section_name: str
    section_type: str
    tables: List[CleanedTable] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_errors: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'section_name': self.section_name,
            'section_type': self.section_type,
            'tables': [table.to_dict() for table in self.tables],
            'metadata': self.metadata,
            'raw_errors': self.raw_errors
        }


@dataclass
class CleanedData:
    """Cleaned and normalized scraped data"""
    tariff_code: str
    product_description: Optional[str] = None
    effective_date: Optional[datetime] = None
    sections: Dict[str, CleanedSection] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    cleaned_at: datetime = field(default_factory=datetime.now)
    source_version: Optional[int] = None
    source_scraped_at: Optional[datetime] = None
    
    def add_section(self, section: CleanedSection) -> None:
        """Add a cleaned section"""
        self.sections[section.section_name] = section
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'tariff_code': self.tariff_code,
            'product_description': self.product_description,
            'effective_date': self.effective_date.isoformat() if self.effective_date else None,
            'sections': {name: section.to_dict() for name, section in self.sections.items()},
            'metadata': self.metadata,
            'cleaned_at': self.cleaned_at.isoformat(),
            'source_version': self.source_version,
            'source_scraped_at': self.source_scraped_at.isoformat() if self.source_scraped_at else None
        }



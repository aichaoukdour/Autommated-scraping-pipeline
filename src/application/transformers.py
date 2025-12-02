"""
Data Transformation - Clean raw scraped data into normalized structures
Enhanced with missing value handling, column normalization, and data quality improvements
"""

import re
import logging
import unicodedata
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from decimal import Decimal, InvalidOperation

from ..domain.entities import ScrapedData
from ..domain.cleaned_entities import (
    CleanedData, CleanedSection, CleanedTable, CleanedTableRow
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform raw scraped data into cleaned, normalized format"""
    
    # French month names to numbers
    FRENCH_MONTHS = {
        'janvier': 1, 'février': 2, 'fév': 2, 'mars': 3, 'avril': 4,
        'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8, 'septembre': 9,
        'octobre': 10, 'novembre': 11, 'décembre': 12
    }
    
    # Missing value indicators
    MISSING_VALUES = {
        '', 'null', 'none', 'n/a', 'na', 'nil', '-', '--', '---',
        'n.d.', 'non disponible', 'non renseigné', 'non spécifié',
        'non applicable', 'non défini', 'vide', 'empty'
    }
    
    # Column name normalization mappings
    COLUMN_NORMALIZATIONS = {
        # Common variations
        'pays': 'country',
        'pays.': 'country',
        'country': 'country',
        'kgs': 'weight_kg',
        'kg': 'weight_kg',
        'poids': 'weight_kg',
        'date': 'date',
        'année': 'year',
        'annee': 'year',
        'année.': 'year',
        'valeur': 'value',
        'montant': 'amount',
        'quantité': 'quantity',
        'quantite': 'quantity',
        'tarif': 'tariff',
        'taux': 'rate',
        'pourcentage': 'percentage',
        '%': 'percentage',
        'code': 'code',
        'libellé': 'label',
        'libelle': 'label',
        'description': 'description',
        'nom': 'name',
        'type': 'type',
        'catégorie': 'category',
        'categorie': 'category',
    }
    
    def transform(self, scraped_data: ScrapedData) -> CleanedData:
        """Transform raw scraped data to cleaned data"""
        logger.info(f"Transforming data for {scraped_data.tariff_code_searched}")
        
        cleaned = CleanedData(
            tariff_code=scraped_data.tariff_code_searched,
            product_description=self._clean_text(scraped_data.basic_info.product_description),
            effective_date=self._parse_date(scraped_data.basic_info.effective_date),
            source_scraped_at=scraped_data.scraped_at,
            metadata=self._extract_metadata(scraped_data)
        )
        
        # Transform each section
        for section_name, section_data in scraped_data.sections.items():
            cleaned_section = self._transform_section(section_name, section_data)
            if cleaned_section:
                cleaned.add_section(cleaned_section)
        
        logger.info(f"Transformed {len(cleaned.sections)} sections")
        return cleaned
    
    def _transform_section(self, section_name: str, section_data) -> Optional[CleanedSection]:
        """Transform a section"""
        try:
            cleaned_section = CleanedSection(
                section_name=section_name,
                section_type=section_data.section_type.value if hasattr(section_data.section_type, 'value') else str(section_data.section_type),
                metadata=self._clean_metadata(section_data.structured_data.metadata.to_dict()),
                raw_errors=section_data.error
            )
            
            # Transform tables
            for table_idx, table in enumerate(section_data.structured_data.tables):
                cleaned_table = self._transform_table(table, section_name, table_idx)
                if cleaned_table:
                    cleaned_section.tables.append(cleaned_table)
            
            return cleaned_section
        except Exception as e:
            logger.error(f"Error transforming section {section_name}: {e}")
            return None
    
    def _normalize_column_name(self, header: str) -> str:
        """Normalize column names for consistency"""
        if not header:
            return "unnamed_column"
        
        # Clean the header
        cleaned = self._clean_text(header)
        if not cleaned:
            return "unnamed_column"
        
        # Convert to lowercase for matching
        lower_cleaned = cleaned.lower().strip()
        
        # Check normalization mappings
        for key, normalized in self.COLUMN_NORMALIZATIONS.items():
            if key in lower_cleaned:
                # Replace the key with normalized version
                normalized_header = re.sub(
                    re.escape(key), 
                    normalized, 
                    lower_cleaned, 
                    flags=re.IGNORECASE
                )
                # Capitalize first letter of each word
                return '_'.join(word.capitalize() for word in normalized_header.split())
        
        # Default: normalize to snake_case
        # Remove special characters, replace spaces with underscores
        normalized = re.sub(r'[^\w\s]', '', cleaned)
        normalized = re.sub(r'\s+', '_', normalized.strip())
        normalized = normalized.lower()
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        return normalized if normalized else "unnamed_column"
    
    def _is_missing_value(self, value: Any) -> bool:
        """Check if a value represents missing data"""
        if value is None:
            return True
        
        if isinstance(value, str):
            cleaned = value.strip().lower()
            return cleaned in self.MISSING_VALUES or len(cleaned) == 0
        
        # Check for empty collections
        if isinstance(value, (list, dict, set)):
            return len(value) == 0
        
        return False
    
    def _handle_missing_value(self, value: Any, header: str, data_type: Optional[str] = None) -> Any:
        """Handle missing values with appropriate strategy"""
        if not self._is_missing_value(value):
            return value
        
        # Strategy based on data type
        if data_type == 'numeric':
            return None  # Keep None for numeric missing values
        elif data_type == 'date':
            return None  # Keep None for date missing values
        elif data_type == 'boolean':
            return None
        else:
            # For text, return None (can be changed to empty string if preferred)
            return None
    
    def _infer_data_type(self, values: List[Any], header: str) -> str:
        """Infer data type from a column's values"""
        if not values:
            return 'text'
        
        # Count non-missing values
        non_missing = [v for v in values if not self._is_missing_value(v)]
        if not non_missing:
            return 'text'  # Default if all missing
        
        # Check for numeric
        numeric_count = 0
        for v in non_missing[:10]:  # Sample first 10
            if self._looks_like_number(str(v)):
                numeric_count += 1
        
        if numeric_count / len(non_missing[:10]) > 0.7:
            return 'numeric'
        
        # Check for dates
        date_count = 0
        for v in non_missing[:10]:
            if self._looks_like_date(str(v)):
                date_count += 1
        
        if date_count / len(non_missing[:10]) > 0.7:
            return 'date'
        
        # Check for boolean
        bool_values = {'true', 'false', 'oui', 'non', 'yes', 'no', '1', '0'}
        bool_count = sum(1 for v in non_missing[:10] if str(v).lower().strip() in bool_values)
        if bool_count / len(non_missing[:10]) > 0.7:
            return 'boolean'
        
        return 'text'
    
    def _transform_table(self, table: Dict[str, Any], section_name: str, table_idx: int) -> Optional[CleanedTable]:
        """Transform a table with enhanced data cleaning"""
        try:
            headers = table.get('headers', [])
            rows = table.get('rows', [])
            
            if not headers:
                logger.warning(f"Table {table_idx} has no headers")
                return None
            
            if not rows:
                logger.warning(f"Table {table_idx} has no rows")
                return None
            
            # Normalize headers
            normalized_headers = []
            header_mapping = {}  # Map original header to normalized
            for i, header in enumerate(headers):
                normalized = self._normalize_column_name(header) if header else f"col_{i}"
                normalized_headers.append(normalized)
                header_mapping[header] = normalized
            
            # Infer data types for each column
            column_types = {}
            for i, header in enumerate(headers):
                column_values = [row.get(header) for row in rows if isinstance(row, dict)]
                column_types[header] = self._infer_data_type(column_values, header)
            
            # Create cleaned table
            cleaned_table = CleanedTable(
                name=f"{section_name}_table_{table_idx + 1}",
                headers=normalized_headers,
                rows=[],
                metadata={
                    'table_index': table.get('table_index', table_idx + 1),
                    'original_headers': headers,
                    'column_types': {header_mapping.get(h, h): column_types.get(h, 'text') 
                                     for h in headers},
                    'missing_value_stats': {}
                }
            )
            
            # Clean each row
            missing_value_counts = {norm_h: 0 for norm_h in normalized_headers}
            valid_rows = 0
            
            for row_idx, row in enumerate(rows):
                if not isinstance(row, dict):
                    logger.warning(f"Row {row_idx} is not a dict: {type(row)}")
                    continue
                
                cleaned_row = CleanedTableRow()
                row_has_data = False
                
                for original_header, normalized_header in header_mapping.items():
                    raw_value = row.get(original_header)
                    data_type = column_types.get(original_header, 'text')
                    
                    # Handle missing values
                    if self._is_missing_value(raw_value):
                        missing_value_counts[normalized_header] += 1
                        cleaned_value = self._handle_missing_value(raw_value, normalized_header, data_type)
                    else:
                        cleaned_value = self._clean_cell_value(raw_value, normalized_header, data_type)
                        if cleaned_value is not None:
                            row_has_data = True
                    
                    cleaned_row.row_data[normalized_header] = cleaned_value
                
                # Only add row if it has at least some data
                if row_has_data:
                    cleaned_table.rows.append(cleaned_row)
                    valid_rows += 1
            
            # Update metadata with missing value statistics
            total_rows = len(rows)
            cleaned_table.metadata['missing_value_stats'] = {
                header: {
                    'count': missing_value_counts[header],
                    'percentage': round((missing_value_counts[header] / total_rows * 100), 2) if total_rows > 0 else 0
                }
                for header in normalized_headers
            }
            cleaned_table.metadata['valid_rows'] = valid_rows
            cleaned_table.metadata['total_rows'] = total_rows
            
            if len(cleaned_table.rows) == 0:
                logger.warning(f"Table {table_idx} has no valid rows after cleaning")
                return None
            
            logger.debug(f"Transformed table {table_idx}: {len(cleaned_table.rows)} valid rows "
                        f"({valid_rows}/{total_rows}), {len(normalized_headers)} columns")
            return cleaned_table
        except Exception as e:
            logger.error(f"Error transforming table: {e}", exc_info=True)
            return None
    
    def _clean_cell_value(self, value: Any, header: str, data_type: Optional[str] = None) -> Any:
        """Clean a cell value based on its content and inferred type"""
        if value is None:
            return None
        
        str_value = str(value).strip()
        
        # If data type is known, use it
        if data_type == 'numeric':
            parsed_num = self._parse_number(str_value)
            return parsed_num if parsed_num is not None else self._clean_text(str_value)
        
        if data_type == 'date':
            parsed_date = self._parse_date(str_value)
            return parsed_date.isoformat() if parsed_date else self._clean_text(str_value)
        
        if data_type == 'boolean':
            return self._parse_boolean(str_value)
        
        # Auto-detect type if not provided
        # Try to detect numeric values
        if self._looks_like_number(str_value):
            parsed_num = self._parse_number(str_value)
            if parsed_num is not None:
                return parsed_num
        
        # Try to detect dates
        if self._looks_like_date(str_value):
            parsed_date = self._parse_date(str_value)
            if parsed_date:
                return parsed_date.isoformat()
        
        # Return cleaned text
        return self._clean_text(str_value)
    
    def _parse_boolean(self, value: str) -> Optional[bool]:
        """Parse boolean values"""
        value_lower = value.lower().strip()
        true_values = {'true', 'oui', 'yes', '1', 'vrai', 'o'}
        false_values = {'false', 'non', 'no', '0', 'faux', 'n'}
        
        if value_lower in true_values:
            return True
        elif value_lower in false_values:
            return False
        return None
    
    def _looks_like_number(self, value: str) -> bool:
        """Check if a string looks like a number"""
        # Remove common formatting
        clean = value.replace(',', '').replace(' ', '').replace('.', '', 1).strip()
        # Check if it's all digits (possibly with one decimal point)
        return bool(re.match(r'^-?\d+\.?\d*$', clean))
    
    def _parse_number(self, value: str) -> Optional[Decimal]:
        """Parse a number string to Decimal"""
        try:
            # Remove commas and spaces
            clean = value.replace(',', '').replace(' ', '').strip()
            return Decimal(clean)
        except (ValueError, InvalidOperation):
            return None
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if a string looks like a date"""
        # Check for French date patterns
        date_patterns = [
            r'\d{1,2}\s+\w+\s+\d{4}',  # "1 juillet 2000"
            r'\d{1,2}/\d{1,2}/\d{4}',  # "01/07/2000"
            r'\d{4}-\d{2}-\d{2}',      # "2000-07-01"
        ]
        return any(re.search(pattern, value, re.IGNORECASE) for pattern in date_patterns)
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse a date string (handles French dates)"""
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        # Try French format: "1 juillet 2000"
        french_pattern = r'(\d{1,2})\s+(\w+)\s+(\d{4})'
        match = re.search(french_pattern, date_str, re.IGNORECASE)
        if match:
            day = int(match.group(1))
            month_name = match.group(2).lower()
            year = int(match.group(3))
            
            month = self.FRENCH_MONTHS.get(month_name)
            if month:
                try:
                    return datetime(year, month, day)
                except ValueError:
                    pass
        
        # Try standard formats
        formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean text: remove extra whitespace, normalize unicode, remove special chars"""
        if not text:
            return None
        
        # Convert to string
        cleaned = str(text)
        
        # Normalize unicode (NFD to NFC)
        try:
            cleaned = unicodedata.normalize('NFC', cleaned)
        except:
            pass
        
        # Remove null bytes and control characters (except newlines and tabs)
        cleaned = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', cleaned)
        
        # Normalize whitespace (replace multiple spaces/tabs/newlines with single space)
        cleaned = re.sub(r'[\s\t\n\r]+', ' ', cleaned)
        
        # Remove leading/trailing whitespace
        cleaned = cleaned.strip()
        
        # Remove zero-width characters
        cleaned = re.sub(r'[\u200B-\u200D\uFEFF]', '', cleaned)
        
        return cleaned if cleaned else None
    
    def _clean_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Clean metadata dictionary"""
        cleaned = {}
        for key, value in metadata.items():
            if key.startswith('_'):
                continue  # Skip internal metadata
            cleaned_key = self._clean_text(str(key))
            if isinstance(value, str):
                cleaned_value = self._clean_text(value)
            else:
                cleaned_value = value
            if cleaned_key:
                cleaned[cleaned_key] = cleaned_value
        return cleaned
    
    def _extract_metadata(self, scraped_data: ScrapedData) -> Dict[str, Any]:
        """Extract and clean metadata from scraped data"""
        metadata = {
            'scraping_duration_seconds': scraped_data.scraping_duration_seconds,
            'sections_count': len(scraped_data.sections),
        }
        
        # Add basic info metadata
        if scraped_data.basic_info.metadata:
            for key, value in scraped_data.basic_info.metadata.to_dict().items():
                if not key.startswith('_'):
                    metadata[f'basic_{key}'] = value
        
        return metadata


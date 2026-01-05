"""
ADIL ETL Pipeline v2 - Production Ready with Industry-Standard Libraries
Using: pandas, ftfy, pydantic, pandera for robust data processing
"""

import json
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, InvalidOperation

# Data processing libraries
import pandas as pd
import numpy as np

# Text cleaning and encoding
import ftfy  # Fix text encoding issues
from bs4 import BeautifulSoup  # HTML cleaning
import unicodedata  # Unicode normalization

# Data validation
from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
from pydantic.dataclasses import dataclass as pydantic_dataclass

# Schema validation for DataFrames
import pandera as pa
from pandera import Column, DataFrameSchema, Check

# Data quality
from dataclasses import dataclass, field
from collections import Counter


# ============================================================================
# STEP 1: CONFIGURATION & LOGGING SETUP
# ============================================================================

class Config:
    """Centralized configuration"""
    
    # Paths
    OUTPUT_DIR = Path("etl_output_v2")
    LOGS_DIR = OUTPUT_DIR / "logs"
    DATA_DIR = OUTPUT_DIR / "data"
    REPORTS_DIR = OUTPUT_DIR / "reports"
    
    # Files
    CLEAN_JSON = DATA_DIR / "adil_clean.json"
    CLEAN_CSV = DATA_DIR / "adil_clean.csv"
    CLEAN_PARQUET = DATA_DIR / "adil_clean.parquet"  # More efficient than CSV
    FAILED_JSON = DATA_DIR / "adil_failed.json"
    
    # Section-specific outputs
    TAXES_CSV = DATA_DIR / "taxes_extracted.csv"
    STATISTICS_PARQUET = DATA_DIR / "statistics_extracted.parquet"
    CLASSIFICATION_CSV = DATA_DIR / "classification_extracted.csv"
    
    # Reports
    SUMMARY_REPORT = REPORTS_DIR / "summary_report.json"
    DATA_QUALITY_REPORT = REPORTS_DIR / "data_quality_report.html"
    
    @classmethod
    def setup(cls):
        """Initialize directory structure and logging"""
        for dir_path in [cls.OUTPUT_DIR, cls.LOGS_DIR, cls.DATA_DIR, cls.REPORTS_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        log_file = cls.LOGS_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info("="*60)
        logger.info("ADIL ETL Pipeline v2 - Library-Based")
        logger.info(f"Libraries: pandas, ftfy, pydantic, pandera")
        logger.info(f"Output directory: {cls.OUTPUT_DIR}")
        logger.info("="*60)


# ============================================================================
# STEP 2: DATA MODELS WITH PYDANTIC (Auto-validation)
# ============================================================================

from pydantic import ConfigDict, field_validator

class TaxInfo(BaseModel):
    """Validated tax information using Pydantic v2"""
    model_config = ConfigDict(validate_assignment=True)
    
    import_duty: Optional[float] = Field(None, ge=0, le=100, description="Import duty percentage")
    import_duty_raw: Optional[str] = None
    parafiscal_tax: Optional[float] = Field(None, ge=0, le=100)
    parafiscal_tax_raw: Optional[str] = None
    vat: Optional[float] = Field(None, ge=0, le=100)
    vat_raw: Optional[str] = None
    has_franchises: bool = False


class StatisticsData(BaseModel):
    """Statistics with validation"""
    model_config = ConfigDict(validate_assignment=True)
    
    type: str = Field(..., pattern='^(imports|exports|suppliers|clients)$')
    period: Optional[str] = None
    unit: Optional[str] = None
    data_points: List[Dict] = Field(default_factory=list)
    total: Optional[float] = Field(None, ge=0)
    
    @field_validator('data_points')
    @classmethod
    def validate_data_points(cls, v):
        """Ensure data points have consistent structure"""
        for point in v:
            if not isinstance(point, dict):
                raise ValueError("Data points must be dictionaries")
            if 'value' in point and not isinstance(point['value'], (int, float)):
                raise ValueError("Value must be numeric")
        return v


class ClassificationInfo(BaseModel):
    """Product classification"""
    model_config = ConfigDict(validate_assignment=True)
    
    section: Optional[str] = Field(None, max_length=500)
    chapter: Optional[str] = Field(None, max_length=500)
    designation: Optional[str] = Field(None, max_length=1000)
    unit: Optional[str] = Field(None, max_length=100)
    product_description: Optional[str] = Field(None, max_length=2000)


class HSCodeRecord(BaseModel):
    """Main record with Pydantic validation"""
    model_config = ConfigDict(validate_assignment=True)
    
    # Core identification
    hs_code: str = Field(..., pattern=r'^\d{10}$', description="10-digit HS code")
    scraped_at: str
    scrape_status: str
    
    # Nested validated models
    classification: Optional[ClassificationInfo] = None
    taxes: Optional[TaxInfo] = None
    
    # Statistics
    imports_stats: Optional[StatisticsData] = None
    exports_stats: Optional[StatisticsData] = None
    suppliers: Optional[StatisticsData] = None
    clients: Optional[StatisticsData] = None
    
    # Documents
    documents_available: bool = False
    documents_content: Optional[str] = Field(None, max_length=10000)
    agreements_available: bool = False
    agreements_content: Optional[str] = Field(None, max_length=10000)
    
    # Classifications
    national_classification: Optional[Dict] = None
    international_classification: Optional[Dict] = None
    
    # Metadata
    processed_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    data_quality_score: float = Field(0.0, ge=0.0, le=1.0)
    validation_warnings: List[str] = Field(default_factory=list)


# ============================================================================
# STEP 3: TEXT CLEANING WITH LIBRARIES
# ============================================================================

class TextCleaner:
    """Clean text using industry-standard libraries"""
    
    @staticmethod
    def fix_encoding(text: str) -> str:
        """Fix encoding issues using ftfy library"""
        if not text:
            return ""
        # ftfy automatically fixes mojibake and encoding issues
        return ftfy.fix_text(text)
    
    @staticmethod
    def normalize_unicode(text: str) -> str:
        """Normalize unicode characters"""
        if not text:
            return ""
        # NFD = Canonical Decomposition, then remove combining characters
        # NFC = Canonical Composition (standard form)
        return unicodedata.normalize('NFC', text)
    
    @staticmethod
    def clean_html(text: str) -> str:
        """Remove HTML using BeautifulSoup"""
        if not text:
            return ""
        soup = BeautifulSoup(text, 'html.parser')
        return soup.get_text(separator=' ', strip=True)
    
    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalize whitespace using regex"""
        if not text:
            return ""
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Apply all cleaning operations"""
        if not text:
            return ""
        
        # Pipeline: encoding → unicode → html → whitespace
        text = TextCleaner.fix_encoding(text)
        text = TextCleaner.normalize_unicode(text)
        text = TextCleaner.clean_html(text)
        text = TextCleaner.normalize_whitespace(text)
        
        return text
    
    @staticmethod
    def extract_percentage(text: str) -> Optional[float]:
        """Extract percentage using decimal for precision"""
        if not text:
            return None
        
        # Match patterns: "2.5 %", "2,5%", "0.25%"
        pattern = r'(\d+(?:[.,]\d+)?)\s*%'
        match = re.search(pattern, text)
        
        if match:
            value_str = match.group(1).replace(',', '.')
            try:
                return float(Decimal(value_str))
            except (InvalidOperation, ValueError):
                return None
        return None
    
    @staticmethod
    def extract_numbers(text: str) -> List[float]:
        """Extract all numbers from text"""
        if not text:
            return []
        
        # Pattern for numbers with optional thousands separator
        pattern = r'\d+(?:[.,]\d+)?'
        matches = re.findall(pattern, text)
        
        numbers = []
        for match in matches:
            try:
                num = float(match.replace(',', '.'))
                numbers.append(num)
            except ValueError:
                continue
        
        return numbers


# ============================================================================
# STEP 4: EXTRACTORS USING PANDAS
# ============================================================================

class TaxExtractor:
    """Extract tax information"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cleaner = TextCleaner()
    
    def extract(self, section_content: Dict) -> Optional[TaxInfo]:
        """Extract tax information"""
        try:
            raw_text = section_content.get('raw_text', '')
            key_values = section_content.get('key_values', {})
            
            # Clean all text first
            cleaned_kv = {
                self.cleaner.clean_text(k): self.cleaner.clean_text(v) 
                for k, v in key_values.items()
            }
            
            tax_data = {
                'import_duty': None,
                'import_duty_raw': None,
                'parafiscal_tax': None,
                'parafiscal_tax_raw': None,
                'vat': None,
                'vat_raw': None,
                'has_franchises': False
            }
            
            # Extract using keyword matching
            for key, value in cleaned_kv.items():
                key_lower = key.lower()
                
                if 'droit' in key_lower and 'importation' in key_lower:
                    tax_data['import_duty_raw'] = value
                    tax_data['import_duty'] = self.cleaner.extract_percentage(value)
                
                elif 'parafiscale' in key_lower or 'tpi' in key_lower:
                    tax_data['parafiscal_tax_raw'] = value
                    tax_data['parafiscal_tax'] = self.cleaner.extract_percentage(value)
                
                elif 'valeur' in key_lower and 'ajoutée' in key_lower:
                    tax_data['vat_raw'] = value
                    tax_data['vat'] = self.cleaner.extract_percentage(value)
            
            # Check franchises
            if 'franchise' in raw_text.lower():
                tax_data['has_franchises'] = True
            
            # Validate with Pydantic
            return TaxInfo(**tax_data)
            
        except ValidationError as e:
            self.logger.error(f"Tax validation error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting taxes: {e}")
            return None


class StatisticsExtractor:
    """Extract statistics using pandas"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cleaner = TextCleaner()
    
    def extract_yearly_data(self, raw_text: str) -> List[Dict]:
        """Extract year/value pairs using pandas"""
        try:
            lines = raw_text.split('\n')
            
            # Find year and weight lines
            year_pattern = r'\b(20\d{2})\b'
            years = []
            weights = []
            
            for line in lines:
                # Extract years
                year_matches = re.findall(year_pattern, line)
                if year_matches and 'Année' in line:
                    years = [int(y) for y in year_matches]
                
                # Extract weights (numbers with spaces as thousands separator)
                if 'Poids' in line:
                    # Remove 'Poids' and extract numbers
                    weight_line = line.replace('Poids', '')
                    # Pattern: numbers with optional spaces
                    weight_pattern = r'(\d+(?:\s+\d+)*)'
                    weight_matches = re.findall(weight_pattern, weight_line)
                    
                    for w in weight_matches:
                        cleaned = w.replace(' ', '')
                        try:
                            weights.append(int(cleaned))
                        except ValueError:
                            continue
            
            # Create DataFrame
            if years and weights:
                # Ensure same length
                min_len = min(len(years), len(weights))
                df = pd.DataFrame({
                    'year': years[:min_len],
                    'value': weights[:min_len]
                })
                
                # Convert to list of dicts
                return df.to_dict('records')
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error extracting yearly data: {e}")
            return []
    
    def extract_country_data(self, raw_text: str) -> List[Dict]:
        """Extract country/value pairs"""
        try:
            lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
            
            countries = []
            weights = []
            
            for line in lines:
                # Skip header lines
                if any(keyword in line.lower() for keyword in ['graphique', 'tableau', 'pays', 'poids', 'adil']):
                    continue
                
                # Countries are typically uppercase
                if line.isupper() and len(line) > 2 and not line.isdigit():
                    countries.append(self.cleaner.clean_text(line))
                
                # Weights are pure numbers
                elif re.match(r'^[\d\s]+$', line):
                    weight_str = line.replace(' ', '')
                    try:
                        weights.append(int(weight_str))
                    except ValueError:
                        continue
            
            # Create DataFrame
            if countries and weights:
                min_len = min(len(countries), len(weights))
                df = pd.DataFrame({
                    'country': countries[:min_len],
                    'value': weights[:min_len]
                })
                
                return df.to_dict('records')
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error extracting country data: {e}")
            return []
    
    def extract(self, section: Dict, section_type: str) -> Optional[StatisticsData]:
        """Extract statistics"""
        try:
            content = section.get('content', {})
            raw_text = content.get('raw_text', '')
            metadata = content.get('metadata', {})
            
            # Extract data points
            if section_type in ['imports', 'exports']:
                data_points = self.extract_yearly_data(raw_text)
            elif section_type in ['suppliers', 'clients']:
                data_points = self.extract_country_data(raw_text)
            else:
                data_points = []
            
            # Calculate total
            total = None
            if data_points:
                values = [p['value'] for p in data_points if 'value' in p]
                total = float(sum(values)) if values else None
            
            stats_data = {
                'type': section_type,
                'period': metadata.get('period', ''),
                'unit': metadata.get('unit', ''),
                'data_points': data_points,
                'total': total
            }
            
            # Validate with Pydantic
            return StatisticsData(**stats_data)
            
        except ValidationError as e:
            self.logger.error(f"Statistics validation error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting statistics: {e}")
            return None


class ClassificationExtractor:
    """Extract classification information"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cleaner = TextCleaner()
    
    def extract(self, section_content: Dict) -> Optional[ClassificationInfo]:
        """Extract classification"""
        try:
            key_values = section_content.get('key_values', {})
            raw_text = section_content.get('raw_text', '')
            
            # Clean key-values
            cleaned_kv = {
                self.cleaner.clean_text(k): self.cleaner.clean_text(v) 
                for k, v in key_values.items()
            }
            
            classification_data = {
                'section': None,
                'chapter': None,
                'designation': None,
                'unit': None,
                'product_description': None
            }
            
            # Extract from key-values
            for key, value in cleaned_kv.items():
                key_lower = key.lower()
                
                if 'section' in key_lower:
                    classification_data['section'] = value
                elif 'chapitre' in key_lower:
                    classification_data['chapter'] = value
                elif 'unité' in key_lower:
                    classification_data['unit'] = value
            
            # Extract designation from text
            cleaned_text = self.cleaner.clean_text(raw_text)
            lines = cleaned_text.split('\n')
            
            for line in lines:
                if any(keyword in line.lower() for keyword in ['reproducteur', 'vivant', 'pur']):
                    classification_data['designation'] = line[:200]  # Limit length
                    break
            
            # Validate with Pydantic
            return ClassificationInfo(**classification_data)
            
        except ValidationError as e:
            self.logger.error(f"Classification validation error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting classification: {e}")
            return None


# ============================================================================
# STEP 5: MAIN TRANSFORMER
# ============================================================================

class HSCodeTransformer:
    """Main transformation orchestrator"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cleaner = TextCleaner()
        self.tax_extractor = TaxExtractor()
        self.stats_extractor = StatisticsExtractor()
        self.classification_extractor = ClassificationExtractor()
    
    def transform(self, raw_record: Dict) -> Optional[HSCodeRecord]:
        """Transform raw record to validated HSCodeRecord"""
        try:
            # Basic record data
            record_data = {
                'hs_code': raw_record.get('hs_code', ''),
                'scraped_at': raw_record.get('scraped_at', ''),
                'scrape_status': raw_record.get('scrape_status', 'unknown'),
                'classification': None,
                'taxes': None,
                'imports_stats': None,
                'exports_stats': None,
                'suppliers': None,
                'clients': None,
                'documents_available': False,
                'documents_content': None,
                'agreements_available': False,
                'agreements_content': None,
                'national_classification': None,
                'international_classification': None,
                'validation_warnings': []
            }
            
            # Process sections
            sections = raw_record.get('sections', [])
            
            for section in sections:
                section_name = section.get('section_name', '').lower()
                content = section.get('content', {})
                status = section.get('status', 'available')
                
                # Skip unavailable
                if status == 'not_available':
                    continue
                
                try:
                    # Route to appropriate extractor
                    if 'position tarifaire' in section_name:
                        record_data['classification'] = self.classification_extractor.extract(content)
                    
                    elif 'droits et taxes' in section_name:
                        record_data['taxes'] = self.tax_extractor.extract(content)
                    
                    elif 'importations' in section_name:
                        record_data['imports_stats'] = self.stats_extractor.extract(section, 'imports')
                    
                    elif 'exportations' in section_name:
                        record_data['exports_stats'] = self.stats_extractor.extract(section, 'exports')
                    
                    elif 'fournisseurs' in section_name:
                        record_data['suppliers'] = self.stats_extractor.extract(section, 'suppliers')
                    
                    elif 'clients' in section_name:
                        record_data['clients'] = self.stats_extractor.extract(section, 'clients')
                    
                    elif 'documents' in section_name:
                        raw = content.get('raw_text', '')
                        if raw and raw.strip() != 'N/A':
                            record_data['documents_available'] = True
                            record_data['documents_content'] = self.cleaner.clean_text(raw)[:10000]
                    
                    elif 'accords' in section_name or 'convention' in section_name:
                        raw = content.get('raw_text', '')
                        if raw and raw.strip() != 'N/A':
                            record_data['agreements_available'] = True
                            record_data['agreements_content'] = self.cleaner.clean_text(raw)[:10000]
                    
                    elif 'nationale' in section_name:
                        record_data['national_classification'] = {
                            'raw_text': self.cleaner.clean_text(content.get('raw_text', ''))[:5000],
                            'key_values': content.get('key_values', {})
                        }
                    
                    elif 'internationale' in section_name:
                        record_data['international_classification'] = {
                            'raw_text': self.cleaner.clean_text(content.get('raw_text', ''))[:5000],
                            'key_values': content.get('key_values', {})
                        }
                
                except Exception as e:
                    warning = f"Error processing '{section_name}': {str(e)}"
                    record_data['validation_warnings'].append(warning)
                    self.logger.warning(warning)
            
            # Calculate quality score
            record_data['data_quality_score'] = self._calculate_quality_score(record_data)
            
            # Create and validate with Pydantic
            return HSCodeRecord(**record_data)
            
        except ValidationError as e:
            self.logger.error(f"Validation error for {raw_record.get('hs_code')}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            return None
    
    def _calculate_quality_score(self, record_data: Dict) -> float:
        """Calculate data quality score"""
        score = 0.0
        
        # Core fields (40%)
        if record_data.get('hs_code'):
            score += 0.10
        if record_data.get('classification'):
            score += 0.15
        if record_data.get('taxes'):
            score += 0.15
        
        # Statistics (30%)
        if record_data.get('imports_stats') and record_data['imports_stats'].data_points:
            score += 0.10
        if record_data.get('exports_stats') and record_data['exports_stats'].data_points:
            score += 0.10
        if record_data.get('suppliers'):
            score += 0.05
        if record_data.get('clients'):
            score += 0.05
        
        # Classifications (20%)
        if record_data.get('national_classification'):
            score += 0.10
        if record_data.get('international_classification'):
            score += 0.10
        
        # Documents (10%)
        if record_data.get('documents_available'):
            score += 0.05
        if record_data.get('agreements_available'):
            score += 0.05
        
        return round(score, 2)


# ============================================================================
# STEP 6: CSV EXPORTER - Flatten nested data to table format
# ============================================================================

class CSVExporter:
    """Export records to flat CSV table"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def flatten_record(self, record: HSCodeRecord) -> Dict:
        """Flatten nested record to single-level dictionary for CSV - COMPLETE VERSION"""
        
        flat = {
            # ========== CORE IDENTIFICATION ==========
            'hs_code': record.hs_code,
            'scrape_status': record.scrape_status,
            'scraped_at': record.scraped_at,
            'processed_at': record.processed_at,
            'data_quality_score': record.data_quality_score,
            
            # ========== CLASSIFICATION ==========
            'section': record.classification.section if record.classification else None,
            'chapter': record.classification.chapter if record.classification else None,
            'product_designation': record.classification.designation if record.classification else None,
            'unit_of_measure': record.classification.unit if record.classification else None,
            'product_description': record.classification.product_description if record.classification else None,
            
            # ========== TAXES & DUTIES ==========
            'import_duty_pct': record.taxes.import_duty if record.taxes else None,
            'import_duty_raw': record.taxes.import_duty_raw if record.taxes else None,
            'parafiscal_tax_pct': record.taxes.parafiscal_tax if record.taxes else None,
            'parafiscal_tax_raw': record.taxes.parafiscal_tax_raw if record.taxes else None,
            'vat_pct': record.taxes.vat if record.taxes else None,
            'vat_raw': record.taxes.vat_raw if record.taxes else None,
            'total_tax_burden_pct': (
                (record.taxes.import_duty or 0) + 
                (record.taxes.parafiscal_tax or 0) + 
                (record.taxes.vat or 0)
            ) if record.taxes else None,
            'has_tax_franchises': record.taxes.has_franchises if record.taxes else False,
            
            # ========== IMPORT STATISTICS ==========
            'imports_total_value': record.imports_stats.total if record.imports_stats else None,
            'imports_unit': record.imports_stats.unit if record.imports_stats else None,
            'imports_period': record.imports_stats.period if record.imports_stats else None,
            'imports_years_available': len(record.imports_stats.data_points) if record.imports_stats else 0,
            
            # ========== EXPORT STATISTICS ==========
            'exports_total_value': record.exports_stats.total if record.exports_stats else None,
            'exports_unit': record.exports_stats.unit if record.exports_stats else None,
            'exports_period': record.exports_stats.period if record.exports_stats else None,
            'exports_years_available': len(record.exports_stats.data_points) if record.exports_stats else 0,
            
            # ========== TRADE BALANCE ==========
            'trade_balance': (
                (record.exports_stats.total or 0) - (record.imports_stats.total or 0)
            ) if (record.imports_stats or record.exports_stats) else None,
            
            # ========== SUPPLIERS ==========
            'suppliers_count': len(record.suppliers.data_points) if record.suppliers else 0,
            'suppliers_total_value': record.suppliers.total if record.suppliers else None,
            'top_supplier': record.suppliers.data_points[0].get('country') if (record.suppliers and record.suppliers.data_points) else None,
            'top_supplier_value': record.suppliers.data_points[0].get('value') if (record.suppliers and record.suppliers.data_points) else None,
            'suppliers_list': '; '.join([
                f"{p.get('country', 'N/A')} ({p.get('value', 0):,})"
                for p in (record.suppliers.data_points if record.suppliers else [])
            ]) or None,
            
            # ========== CLIENTS ==========
            'clients_count': len(record.clients.data_points) if record.clients else 0,
            'clients_total_value': record.clients.total if record.clients else None,
            'top_client': record.clients.data_points[0].get('country') if (record.clients and record.clients.data_points) else None,
            'top_client_value': record.clients.data_points[0].get('value') if (record.clients and record.clients.data_points) else None,
            'clients_list': '; '.join([
                f"{p.get('country', 'N/A')} ({p.get('value', 0):,})"
                for p in (record.clients.data_points if record.clients else [])
            ]) or None,
            
            # ========== YEARLY IMPORT DATA (Last 5 years) ==========
            'imports_2024': self._get_year_value(record.imports_stats, 2024),
            'imports_2023': self._get_year_value(record.imports_stats, 2023),
            'imports_2022': self._get_year_value(record.imports_stats, 2022),
            'imports_2021': self._get_year_value(record.imports_stats, 2021),
            'imports_2020': self._get_year_value(record.imports_stats, 2020),
            
            # ========== YEARLY EXPORT DATA (Last 5 years) ==========
            'exports_2024': self._get_year_value(record.exports_stats, 2024),
            'exports_2023': self._get_year_value(record.exports_stats, 2023),
            'exports_2022': self._get_year_value(record.exports_stats, 2022),
            'exports_2021': self._get_year_value(record.exports_stats, 2021),
            'exports_2020': self._get_year_value(record.exports_stats, 2020),
            
            # ========== GROWTH RATES ==========
            'imports_growth_2023_2024': self._calculate_growth(
                self._get_year_value(record.imports_stats, 2023),
                self._get_year_value(record.imports_stats, 2024)
            ),
            'exports_growth_2023_2024': self._calculate_growth(
                self._get_year_value(record.exports_stats, 2023),
                self._get_year_value(record.exports_stats, 2024)
            ),
            
            # ========== DOCUMENTS & AGREEMENTS ==========
            'documents_available': record.documents_available,
            'documents_content': record.documents_content[:500] if record.documents_content else None,  # First 500 chars
            'agreements_available': record.agreements_available,
            'agreements_content': record.agreements_content[:500] if record.agreements_content else None,
            
            # ========== NATIONAL CLASSIFICATION ==========
            'has_national_classification': record.national_classification is not None,
            'national_classification_text': (
                record.national_classification.get('raw_text', '')[:300] 
                if record.national_classification else None
            ),
            
            # ========== INTERNATIONAL CLASSIFICATION ==========
            'has_international_classification': record.international_classification is not None,
            'international_classification_text': (
                record.international_classification.get('raw_text', '')[:300] 
                if record.international_classification else None
            ),
            
            # ========== DATA QUALITY & WARNINGS ==========
            'warnings_count': len(record.validation_warnings),
            'warnings': '; '.join(record.validation_warnings) if record.validation_warnings else None,
            'has_complete_tax_info': all([
                record.taxes.import_duty is not None if record.taxes else False,
                record.taxes.vat is not None if record.taxes else False
            ]) if record.taxes else False,
            'has_trade_statistics': bool(record.imports_stats or record.exports_stats),
            'has_supplier_info': bool(record.suppliers and record.suppliers.data_points),
            'has_client_info': bool(record.clients and record.clients.data_points),
        }
        
        return flat
    
    def _get_year_value(self, stats: Optional[StatisticsData], year: int) -> Optional[float]:
        """Extract value for specific year from statistics"""
        if not stats or not stats.data_points:
            return None
        
        for point in stats.data_points:
            if point.get('year') == year:
                return float(point.get('value', 0))
        
        return None
    
    def _calculate_growth(self, old_value: Optional[float], new_value: Optional[float]) -> Optional[float]:
        """Calculate growth rate percentage"""
        if old_value is None or new_value is None or old_value == 0:
            return None
        
        growth = ((new_value - old_value) / old_value) * 100
        return round(growth, 2)
    
    def records_to_dataframe(self, records: List[HSCodeRecord]) -> pd.DataFrame:
        """Convert list of records to pandas DataFrame"""
        flattened = [self.flatten_record(record) for record in records]
        df = pd.DataFrame(flattened)
        
        # Set proper data types
        numeric_cols = [
            'data_quality_score',
            'tax_import_duty_pct', 'tax_parafiscal_pct', 'tax_vat_pct',
            'imports_total', 'exports_total', 'suppliers_total', 'clients_total',
            'imports_data_points_count', 'exports_data_points_count',
            'suppliers_count', 'clients_count', 'warnings_count'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def save_to_csv(self, records: List[HSCodeRecord], output_path: Path, 
                    include_index: bool = False) -> None:
        """Save ALL records to a SINGLE comprehensive CSV file"""
        try:
            df = self.records_to_dataframe(records)
            
            # Reorder columns for better readability
            priority_cols = [
                'hs_code',
                'product_designation',
                'section',
                'chapter',
                'import_duty_pct',
                'parafiscal_tax_pct',
                'vat_pct',
                'total_tax_burden_pct',
                'imports_total_value',
                'exports_total_value',
                'trade_balance',
                'top_supplier',
                'top_client'
            ]
            
            # Arrange columns: priority first, then others
            other_cols = [col for col in df.columns if col not in priority_cols]
            ordered_cols = [col for col in priority_cols if col in df.columns] + other_cols
            df = df[ordered_cols]
            
            # Save to CSV
            df.to_csv(
                output_path,
                index=include_index,
                encoding='utf-8-sig',  # Excel-compatible UTF-8
                float_format='%.2f'    # 2 decimal places for floats
            )
            
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"✓ COMPREHENSIVE CSV SAVED")
            self.logger.info(f"{'='*70}")
            self.logger.info(f"  File: {output_path}")
            self.logger.info(f"  Records: {len(df):,}")
            self.logger.info(f"  Columns: {len(df.columns)}")
            self.logger.info(f"  Size: {output_path.stat().st_size / 1024:.1f} KB")
            self.logger.info(f"{'='*70}\n")
            
            # Print column summary
            self.logger.info("Column Categories:")
            self.logger.info(f"  - Identification: hs_code, status, dates")
            self.logger.info(f"  - Classification: section, chapter, designation")
            self.logger.info(f"  - Taxes: import_duty, VAT, parafiscal, total")
            self.logger.info(f"  - Trade Stats: imports/exports (total + yearly)")
            self.logger.info(f"  - Partners: suppliers and clients (list + top)")
            self.logger.info(f"  - Documents: availability and content")
            self.logger.info(f"  - Quality: scores and warnings\n")
            
        except Exception as e:
            self.logger.error(f"Error saving CSV: {e}")
            raise


# ============================================================================
# STEP 7: FULL PIPELINE RUNNER
# ============================================================================

class ETLPipeline:
    """Complete ETL pipeline orchestrator"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.transformer = HSCodeTransformer()
        self.exporter = CSVExporter()
        
        self.successful_records: List[HSCodeRecord] = []
        self.failed_records: List[Dict] = []
    
    def process_file(self, input_file: Path) -> Dict:
        """Process complete JSON file"""
        
        self.logger.info("="*70)
        self.logger.info(f"Processing file: {input_file}")
        self.logger.info("="*70)
        
        # Load data
        with open(input_file, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        total = len(raw_data)
        self.logger.info(f"Loaded {total} records")
        
        # Process each record
        for i, raw_record in enumerate(raw_data, 1):
            hs_code = raw_record.get('hs_code', 'unknown')
            
            try:
                record = self.transformer.transform(raw_record)
                
                if record:
                    self.successful_records.append(record)
                    if i % 10 == 0:
                        self.logger.info(f"  Processed {i}/{total} ({i/total*100:.1f}%)")
                else:
                    self.failed_records.append({
                        'hs_code': hs_code,
                        'reason': 'Transformation returned None'
                    })
                    
            except Exception as e:
                self.logger.error(f"Error processing {hs_code}: {e}")
                self.failed_records.append({
                    'hs_code': hs_code,
                    'error': str(e)
                })
        
        self.logger.info(f"\n✓ Processing complete:")
        self.logger.info(f"  Successful: {len(self.successful_records)}")
        self.logger.info(f"  Failed: {len(self.failed_records)}")
        
        return {
            'total': total,
            'successful': len(self.successful_records),
            'failed': len(self.failed_records)
        }
    
    def save_outputs(self) -> None:
        """Save all outputs - SINGLE COMPREHENSIVE CSV"""
        
        self.logger.info("\n" + "="*70)
        self.logger.info("SAVING OUTPUTS")
        self.logger.info("="*70)
        
        if self.successful_records:
            # 1. Save SINGLE comprehensive CSV with ALL information
            self.exporter.save_to_csv(
                self.successful_records,
                Config.CLEAN_CSV
            )
            
            # 2. Save JSON backup (optional, for reference)
            json_data = [record.dict() for record in self.successful_records]
            with open(Config.CLEAN_JSON, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"✓ JSON backup: {Config.CLEAN_JSON}")
        
        # 3. Save failed records
        if self.failed_records:
            with open(Config.FAILED_JSON, 'w', encoding='utf-8') as f:
                json.dump(self.failed_records, f, indent=2, ensure_ascii=False)
            self.logger.info(f"✓ Failed records: {Config.FAILED_JSON}")
        
        self.logger.info("\n✓ All outputs saved successfully!")
    
    def generate_summary_report(self) -> Dict:
        """Generate summary statistics"""
        
        if not self.successful_records:
            return {}
        
        df = self.exporter.records_to_dataframe(self.successful_records)
        
        report = {
            'total_records': len(self.successful_records),
            'quality_scores': {
                'mean': float(df['data_quality_score'].mean()),
                'median': float(df['data_quality_score'].median()),
                'min': float(df['data_quality_score'].min()),
                'max': float(df['data_quality_score'].max())
            },
            'completeness': {
                'with_taxes': int(df['tax_import_duty_pct'].notna().sum()),
                'with_imports': int(df['imports_total'].notna().sum()),
                'with_exports': int(df['exports_total'].notna().sum()),
                'with_classification': int(df['classification_section'].notna().sum())
            },
            'tax_stats': {
                'avg_import_duty': float(df['tax_import_duty_pct'].mean()) if df['tax_import_duty_pct'].notna().any() else None,
                'avg_vat': float(df['tax_vat_pct'].mean()) if df['tax_vat_pct'].notna().any() else None,
                'with_franchises': int(df['tax_has_franchises'].sum())
            }
        }
        
        # Save report
        with open(Config.SUMMARY_REPORT, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"\n✓ Summary report: {Config.SUMMARY_REPORT}")
        
        return report
    
    def run(self, input_file: str) -> None:
        """Run complete pipeline"""
        
        Config.setup()
        
        input_path = Path(input_file)
        
        if not input_path.exists():
            self.logger.error(f"File not found: {input_file}")
            return
        
        # Process
        stats = self.process_file(input_path)
        
        # Save outputs
        self.save_outputs()
        
        # Generate report
        report = self.generate_summary_report()
        
        # Print summary
        print("\n" + "="*70)
        print("ETL PIPELINE COMPLETE")
        print("="*70)
        print(f"\nInput: {input_file}")
        print(f"Total records: {stats['total']}")
        print(f"Successful: {stats['successful']} ({stats['successful']/stats['total']*100:.1f}%)")
        print(f"Failed: {stats['failed']}")
        
        if report:
            print(f"\nData Quality:")
            print(f"  Average score: {report['quality_scores']['mean']:.2f}")
            print(f"  Range: {report['quality_scores']['min']:.2f} - {report['quality_scores']['max']:.2f}")
            
            print(f"\nCompleteness:")
            print(f"  With taxes: {report['completeness']['with_taxes']}")
            print(f"  With imports: {report['completeness']['with_imports']}")
            print(f"  With exports: {report['completeness']['with_exports']}")
        
        print(f"\nOutput files:")
        print(f"  📊 MAIN CSV (Complete): {Config.CLEAN_CSV}")
        print(f"     → Single table with ALL information")
        print(f"     → {stats['successful']:,} rows")
        print(f"     → ~70+ columns including:")
        print(f"        • HS Code + Product Designation")
        print(f"        • Classification (Section, Chapter)")
        print(f"        • Taxes (Import Duty, VAT, Parafiscal)")
        print(f"        • Trade Stats (Imports/Exports by year)")
        print(f"        • Suppliers & Clients (with values)")
        print(f"        • Documents & Agreements")
        print(f"        • Growth rates and quality scores")
        print(f"\n  📄 JSON Backup: {Config.CLEAN_JSON}")
        print(f"  📋 Summary Report: {Config.SUMMARY_REPORT}")
        if self.failed_records:
            print(f"  ⚠️  Failed Records: {Config.FAILED_JSON}")
        print("="*70 + "\n")


# ============================================================================
# STEP 8: TEST & RUN
# ============================================================================

def test_pipeline():
    """Test with sample data"""
    
    print("\n" + "="*70)
    print("TESTING ETL PIPELINE - CSV EXPORT")
    print("="*70 + "\n")
    
    Config.setup()
    
    # Sample data
    sample = {
        "hs_code": "0101210000",
        "scraped_at": "2026-01-03T12:24:54",
        "scrape_status": "success",
        "sections": [
            {
                "section_name": "Position tarifaire",
                "content": {
                    "raw_text": "SECTION : 01 - Animaux vivants",
                    "key_values": {
                        "SECTION": "01 - Animaux vivants",
                        "CHAPITRE": "01 - Chevaux"
                    }
                }
            },
            {
                "section_name": "Droits et Taxes",
                "content": {
                    "raw_text": "DI: 2.5%, TVA: 20%",
                    "key_values": {
                        "Droit d'Importation": "2.5 %",
                        "TVA": "20 %"
                    }
                }
            }
        ]
    }
    
    transformer = HSCodeTransformer()
    exporter = CSVExporter()
    
    record = transformer.transform(sample)
    
    if record:
        print("✓ Record transformed successfully\n")
        
        # Create DataFrame
        df = exporter.records_to_dataframe([record])
        
        print("CSV Preview:")
        print("-" * 70)
        print(df.to_string())
        print("-" * 70)
        
        # Save
        test_csv = Config.DATA_DIR / "test_output.csv"
        exporter.save_to_csv([record], test_csv)
        
        print(f"\n✓ Saved to: {test_csv}")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Run full pipeline on provided file
        input_file = sys.argv[1]
        pipeline = ETLPipeline()
        pipeline.run(input_file)
    else:
        # Run test
        test_pipeline()
        print("\nTo process your data file:")
        print("  python etl_v2.py your_data.json")

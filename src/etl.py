"""
Robust ETL Pipeline for Moroccan Customs ADIL Data
Production-ready implementation with validation, error handling, and scalability
"""

import json
import csv
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import hashlib
from collections import defaultdict


# ============================================================================
# CONFIGURATION
# ============================================================================

class ETLConfig:
    """Centralized configuration for the ETL pipeline"""
    
    # Output paths
    OUTPUT_DIR = Path("etl_output")
    CLEAN_DATA_JSON = OUTPUT_DIR / "adil_clean.json"
    CLEAN_DATA_CSV = OUTPUT_DIR / "adil_clean.csv"
    FAILED_DATA_JSON = OUTPUT_DIR / "adil_failed.json"
    LOG_FILE = OUTPUT_DIR / "etl_pipeline.log"
    SUMMARY_REPORT = OUTPUT_DIR / "etl_summary_report.json"
    CHECKPOINT_FILE = OUTPUT_DIR / "etl_checkpoint.json"
    
    # Processing settings
    BATCH_SIZE = 100
    MAX_WORKERS = 4
    ENABLE_CHECKPOINTING = True
    
    # Validation settings
    MIN_PRODUCT_DESC_LENGTH = 10
    MAX_TEXT_LENGTH = 50000
    # Fixed: Use French section names matching the actual website
    SECTION_NAMES = ["Droits et Taxes", "Documents", "Accords", "Historique"]
    
    @classmethod
    def setup(cls):
        """Initialize output directory and logging"""
        cls.OUTPUT_DIR.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(cls.LOG_FILE),
                logging.StreamHandler()
            ]
        )


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class HSCodeRecord:
    """Structured schema for HS code data"""
    hs_code: str
    product_description: str  # data[0]
    menu_text: str           # data[1]
    taxes: str               # data[2] - Droits et Taxes
    documents: str           # data[3] - Documents
    agreements: str          # data[4] - Accords
    history: str             # data[5] - Historique
    
    # Metadata
    processed_at: str
    data_quality_score: float
    validation_warnings: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    def to_csv_row(self) -> Dict:
        """Convert to CSV-compatible format"""
        data = self.to_dict()
        data['validation_warnings'] = "; ".join(data['validation_warnings'])
        return data


@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    score: float  # 0.0 to 1.0
    warnings: List[str]
    errors: List[str]


@dataclass
class ETLMetrics:
    """Pipeline execution metrics"""
    total_records: int = 0
    successful: int = 0
    failed: int = 0
    warnings: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def duration(self) -> float:
        """Calculate duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_dict(self) -> Dict:
        return {
            "total_records": self.total_records,
            "successful": self.successful,
            "failed": self.failed,
            "warnings": self.warnings,
            "success_rate": f"{(self.successful/self.total_records*100):.2f}%" if self.total_records > 0 else "0%",
            "duration_seconds": self.duration(),
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None
        }


# ============================================================================
# TEXT CLEANING & NORMALIZATION
# ============================================================================

class TextCleaner:
    """Advanced text cleaning and normalization"""
    
    @staticmethod
    def remove_html_tags(text: str) -> str:
        """Remove HTML tags using regex"""
        clean = re.sub(r'<[^>]+>', '', text)
        return clean
    
    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalize whitespace to single spaces"""
        return ' '.join(text.split())
    
    @staticmethod
    def normalize_encoding(text: str) -> str:
        """Fix common encoding issues"""
        replacements = {
            'â€™': "'",
            'â€œ': '"',
            'â€': '"',
            'Ã©': 'é',
            'Ã¨': 'è',
            'Ã§': 'ç',
            'Ã ': 'à'
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text
    
    @staticmethod
    def remove_extra_punctuation(text: str) -> str:
        """Remove excessive punctuation"""
        text = re.sub(r'\.{3,}', '...', text)
        text = re.sub(r'-{2,}', '-', text)
        text = re.sub(r'\s+([.,;:!?])', r'\1', text)
        return text

    @staticmethod
    def strip_adil_boilerplate(text: str) -> str:
        """Remove ADIL-specific header/footer boilerplate"""
        # Patterns to remove
        patterns = [
            r"ADiL Vous êtes au niveau de la position tarifaire\s*:\s*\d{2,4}(?:\.\d{2}){1,4}",
            r"Royaume du Maroc\s+Administration des Douanes\s+et Impôts Indirects.*?(?:Position tarifaire|Version papier)",
            r"ADiL\s+(?:Droits et Taxes|Documents et Normes|Accords et Conventions|Historique).*?Source\s*:\s*ADII",
            r"Situation du\s*:\s*\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}",
            r"Entrée en vigueur le\s*:\s*\w+\s+\d+\s+\w+\s+\d{4}",
            r"Graphique et Tableau\s*:\s*TAUX.*?(?=\d{2}/\d{2}/\d{4}|$)",
            r"\*+", # Remove decorative stars
            r"Source\s*:\s*ADII",
            r"Nouvelle recherche\.\.\.",
            r"Version papier",
            r"LES DOCUMENTS EXIGIBLES",
            r"Position tarifaire\s*:\s*\d{2,4}(?:\.\d{2}){1,4}",
            r"ADMINISTRATION DES DOUANES ET IMPOTS INDIRECTS",
            r"Royaume du Maroc"
        ]
        
        for pattern in patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
        
        # Specific cleanup for header debris that might be left on separate lines
        text = re.sub(r"ADiL\s*\n\s*(?:Droits|Documents|Accords|Historique).*?\n", "\n", text, flags=re.IGNORECASE | re.DOTALL)
        
        return text

    @staticmethod
    def extract_product_description(text: str) -> str:
        """Specifically extract info after 'Description du [Nouveau] Produit Remarquable'"""
        marker_pattern = r"Description du (?:Nouveau )?Produit Remarquable\s*:\s*\( Source\s*:\s*Office des Changes \)"
        match = re.search(marker_pattern, text, re.IGNORECASE)
        if match:
            # Get everything after the marker
            content = text[match.end():].strip()
            # Clean up potential leading/trailing garbage
            content = re.sub(r"^[:\s-]+", "", content)
            return content
        return text

    @staticmethod
    def deduplicate_lines(text: str) -> str:
        """Remove duplicate lines (common in tables like Accords)"""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        seen = set()
        unique_lines = []
        for line in lines:
            if line not in seen:
                unique_lines.append(line)
                seen.add(line)
        return "\n".join(unique_lines)

    @classmethod
    def clean(cls, text: str, field_type: str = "general") -> str:
        """Apply all cleaning operations with specialized logic per field"""
        if not text or not isinstance(text, str):
            return ""
        
        # Initial normalization (encoding, HTML)
        text = cls.normalize_encoding(text)
        text = cls.remove_html_tags(text)
        
        # Boilerplate removal
        text = cls.strip_adil_boilerplate(text)
        
        # Field-specific cleaning
        if field_type == "description":
            text = cls.extract_product_description(text)
        
        # Normalize HORIZONTAL whitespace only (preserve newlines)
        lines = [cls.normalize_whitespace(line) for line in text.split('\n')]
        text = '\n'.join(lines)
        
        # Deduplicate rows
        if field_type in ["accords", "documents"]:
            text = cls.deduplicate_lines(text)
            
        # Final cleanup
        text = cls.remove_extra_punctuation(text)
        return text.strip()


# ============================================================================
# DATA VALIDATORS
# ============================================================================

class DataValidator:
    """Comprehensive data validation"""
    
    @staticmethod
    def validate_hs_code(hs_code: str) -> Tuple[bool, str]:
        """Validate HS code format"""
        if not hs_code:
            return False, "HS code is empty"
        
        hs_code = str(hs_code).strip()
        
        # HS codes are typically 6-10 digits
        if not re.match(r'^\d{6,10}$', hs_code):
            return False, f"Invalid HS code format: {hs_code}"
        
        return True, ""
    
    @staticmethod
    def validate_text_field(text: str, field_name: str, 
                           min_length: int = 0, 
                           max_length: int = 50000) -> List[str]:
        """Validate text field with length constraints"""
        warnings = []
        
        if not text:
            warnings.append(f"{field_name} is empty")
        elif len(text) < min_length:
            warnings.append(f"{field_name} is too short ({len(text)} chars)")
        elif len(text) > max_length:
            warnings.append(f"{field_name} exceeds max length ({len(text)} chars)")
        
        return warnings
    
    @staticmethod
    def extract_percentages(text: str) -> List[float]:
        """Extract percentage values from text"""
        pattern = r'(\d+(?:\.\d+)?)\s*%'
        matches = re.findall(pattern, text)
        return [float(m) for m in matches]
    
    @staticmethod
    def extract_dates(text: str) -> List[str]:
        """Extract dates from text (multiple formats)"""
        patterns = [
            r'\d{4}-\d{2}-\d{2}',  # ISO format
            r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
            r'\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}'  # French
        ]
        
        dates = []
        for pattern in patterns:
            dates.extend(re.findall(pattern, text, re.IGNORECASE))
        
        return dates
    
    @classmethod
    def validate_record(cls, record: Dict) -> ValidationResult:
        """Comprehensive record validation"""
        warnings = []
        errors = []
        score = 1.0
        
        # Validate HS code
        is_valid, error_msg = cls.validate_hs_code(record.get('hs_code', ''))
        if not is_valid:
            errors.append(error_msg)
            return ValidationResult(False, 0.0, warnings, errors)
        
        # Validate data array
        data = record.get('data', [])
        if not isinstance(data, list) or len(data) < 6:
            errors.append(f"Invalid data array: expected 6 elements, got {len(data) if isinstance(data, list) else 'not a list'}")
            return ValidationResult(False, 0.0, warnings, errors)
        
        # Validate individual fields (matching scraper output order)
        field_checks = [
            (data[0], "product_description", ETLConfig.MIN_PRODUCT_DESC_LENGTH),
            (data[1], "menu_text", 0),
            (data[2], "taxes (Droits et Taxes)", 0),
            (data[3], "documents (Documents)", 0),
            (data[4], "agreements (Accords)", 0),
            (data[5], "history (Historique)", 0)
        ]
        
        for text, field_name, min_len in field_checks:
            field_warnings = cls.validate_text_field(text, field_name, min_len)
            warnings.extend(field_warnings)
            if field_warnings:
                score -= 0.1
        
        # Semantic validation
        if data[2]:  # Taxes section
            percentages = cls.extract_percentages(data[2])
            if not percentages:
                warnings.append("No percentage values found in taxes section")
                score -= 0.05
        
        if data[5]:  # History section
            dates = cls.extract_dates(data[5])
            if not dates:
                warnings.append("No dates found in history section")
                score -= 0.05
        
        score = max(0.0, min(1.0, score))
        is_valid = len(errors) == 0 and score >= 0.5
        
        return ValidationResult(is_valid, score, warnings, errors)


# ============================================================================
# ETL TRANSFORMER
# ============================================================================

class RobustETL:
    """Main ETL transformation engine"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cleaner = TextCleaner()
        self.validator = DataValidator()
        self.metrics = ETLMetrics()
    
    def extract(self, raw_record: Dict) -> Optional[Dict]:
        """Extract and validate raw data"""
        try:
            # Basic structure validation
            if not isinstance(raw_record, dict):
                self.logger.error(f"Invalid record type: {type(raw_record)}")
                return None
            
            if 'hs_code' not in raw_record or 'data' not in raw_record:
                self.logger.error(f"Missing required fields in record")
                return None
            
            return raw_record
            
        except Exception as e:
            self.logger.error(f"Extraction error: {e}")
            return None
    
    def transform(self, raw_record: Dict) -> Tuple[Optional[HSCodeRecord], ValidationResult]:
        """Transform raw data into structured record"""
        try:
            # Validate first
            validation = self.validator.validate_record(raw_record)
            
            if not validation.is_valid:
                self.logger.warning(f"Validation failed for {raw_record.get('hs_code')}: {validation.errors}")
                return None, validation
            
            # Extract and clean data
            hs_code = str(raw_record['hs_code']).strip()
            data = raw_record['data']
            
            # Create structured record with specialized cleaning for each field
            record = HSCodeRecord(
                hs_code=hs_code,
                product_description=self.cleaner.clean(data[0], "description"),
                menu_text=self.cleaner.clean(data[1], "general"),
                taxes=self.cleaner.clean(data[2], "general"),       # Droits et Taxes
                documents=self.cleaner.clean(data[3], "documents"),   # Documents
                agreements=self.cleaner.clean(data[4], "accords"),  # Accords
                history=self.cleaner.clean(data[5], "general"),     # Historique
                processed_at=datetime.now().isoformat(),
                data_quality_score=validation.score,
                validation_warnings=validation.warnings
            )
            
            return record, validation
            
        except Exception as e:
            self.logger.error(f"Transformation error for {raw_record.get('hs_code')}: {e}")
            validation = ValidationResult(False, 0.0, [], [str(e)])
            return None, validation
    
    def generate_summary(self, record: HSCodeRecord) -> Dict:
        """Generate summary report for a record"""
        summary = {
            "hs_code": record.hs_code,
            "product": record.product_description[:100] + "..." if len(record.product_description) > 100 else record.product_description,
            "key_taxes": self.validator.extract_percentages(record.taxes),
            "document_count": len(record.documents.split('\n')) if record.documents else 0,
            "agreement_count": len(record.agreements.split('\n')) if record.agreements else 0,
            "dates_in_history": self.validator.extract_dates(record.history),
            "quality_score": record.data_quality_score,
            "has_warnings": len(record.validation_warnings) > 0
        }
        return summary


# ============================================================================
# DATA LOADER
# ============================================================================

class DataLoader:
    """Handle data persistence with incremental updates"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_existing_data(self, filepath: Path) -> Dict[str, Dict]:
        """Load existing data indexed by HS code"""
        if not filepath.exists():
            return {}
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Index by HS code
            indexed = {record['hs_code']: record for record in data}
            self.logger.info(f"Loaded {len(indexed)} existing records from {filepath}")
            return indexed
            
        except Exception as e:
            self.logger.error(f"Error loading existing data: {e}")
            return {}
    
    def save_json(self, records: List[HSCodeRecord], filepath: Path, mode: str = 'incremental'):
        """Save records to JSON with incremental support"""
        try:
            if mode == 'incremental':
                existing = self.load_existing_data(filepath)
                
                # Update with new records
                for record in records:
                    existing[record.hs_code] = record.to_dict()
                
                records_to_save = list(existing.values())
            else:
                records_to_save = [r.to_dict() for r in records]
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(records_to_save, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved {len(records_to_save)} records to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving JSON: {e}")
            raise
    
    def save_csv(self, records: List[HSCodeRecord], filepath: Path, mode: str = 'incremental'):
        """Save records to CSV with incremental support"""
        try:
            if not records:
                return
            
            # Determine write mode
            file_exists = filepath.exists()
            write_mode = 'a' if (mode == 'incremental' and file_exists) else 'w'
            
            with open(filepath, write_mode, newline='', encoding='utf-8') as f:
                fieldnames = list(records[0].to_csv_row().keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if write_mode == 'w':
                    writer.writeheader()
                
                for record in records:
                    writer.writerow(record.to_csv_row())
            
            self.logger.info(f"Saved {len(records)} records to {filepath} (mode: {write_mode})")
            
        except Exception as e:
            self.logger.error(f"Error saving CSV: {e}")
            raise
    
    def save_failed_records(self, failed_records: List[Dict], filepath: Path):
        """Save failed records for debugging"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(failed_records, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved {len(failed_records)} failed records to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving failed records: {e}")


# ============================================================================
# CHECKPOINT MANAGER
# ============================================================================

class CheckpointManager:
    """Manage processing checkpoints for resumability"""
    
    def __init__(self, checkpoint_file: Path):
        self.checkpoint_file = checkpoint_file
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def save_checkpoint(self, processed_codes: List[str], batch_num: int):
        """Save current progress"""
        try:
            checkpoint = {
                "timestamp": datetime.now().isoformat(),
                "batch_num": batch_num,
                "processed_codes": processed_codes,
                "count": len(processed_codes)
            }
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2)
            
            self.logger.info(f"Checkpoint saved: {len(processed_codes)} codes processed")
            
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[Dict]:
        """Load last checkpoint"""
        if not self.checkpoint_file.exists():
            return None
        
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
            
            self.logger.info(f"Checkpoint loaded: {checkpoint['count']} codes already processed")
            return checkpoint
            
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
            return None
    
    def clear_checkpoint(self):
        """Clear checkpoint file"""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            self.logger.info("Checkpoint cleared")


# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================

class ETLPipeline:
    """Orchestrate the complete ETL process"""
    
    def __init__(self, config: ETLConfig = None):
        self.config = config or ETLConfig()
        self.config.setup()
        
        self.etl = RobustETL()
        self.loader = DataLoader(self.config)
        self.checkpoint_mgr = CheckpointManager(self.config.CHECKPOINT_FILE)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.successful_records: List[HSCodeRecord] = []
        self.failed_records: List[Dict] = []
        self.summaries: List[Dict] = []
    
    def process_single_record(self, raw_record: Dict) -> Optional[HSCodeRecord]:
        """Process a single record through ETL"""
        try:
            # Extract
            extracted = self.etl.extract(raw_record)
            if not extracted:
                self.failed_records.append({
                    "record": raw_record,
                    "reason": "Extraction failed",
                    "timestamp": datetime.now().isoformat()
                })
                return None
            
            # Transform
            record, validation = self.etl.transform(extracted)
            
            if record:
                # Generate summary
                summary = self.etl.generate_summary(record)
                self.summaries.append(summary)
                return record
            else:
                self.failed_records.append({
                    "record": raw_record,
                    "validation_errors": validation.errors,
                    "timestamp": datetime.now().isoformat()
                })
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing record {raw_record.get('hs_code')}: {e}")
            self.failed_records.append({
                "record": raw_record,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            return None
    
    def process_batch(self, records: List[Dict], batch_num: int = 0) -> List[HSCodeRecord]:
        """Process a batch of records"""
        self.logger.info(f"Processing batch {batch_num} with {len(records)} records")
        
        processed = []
        for record in records:
            result = self.process_single_record(record)
            if result:
                processed.append(result)
        
        # Save checkpoint
        if self.config.ENABLE_CHECKPOINTING:
            processed_codes = [r.hs_code for r in self.successful_records + processed]
            self.checkpoint_mgr.save_checkpoint(processed_codes, batch_num)
        
        return processed
    
    def process_parallel(self, records: List[Dict], max_workers: int = None) -> List[HSCodeRecord]:
        """Process records in parallel"""
        max_workers = max_workers or self.config.MAX_WORKERS
        self.logger.info(f"Processing {len(records)} records with {max_workers} workers")
        
        processed = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_record = {
                executor.submit(self.process_single_record, record): record 
                for record in records
            }
            
            for future in as_completed(future_to_record):
                try:
                    result = future.result()
                    if result:
                        processed.append(result)
                except Exception as e:
                    record = future_to_record[future]
                    self.logger.error(f"Parallel processing error for {record.get('hs_code')}: {e}")
        
        return processed
    
    def run(self, input_data: List[Dict], 
            mode: str = 'batch',
            save_mode: str = 'overwrite',
            resume: bool = True) -> ETLMetrics:
        """
        Run the complete ETL pipeline
        
        Args:
            input_data: List of raw records
            mode: 'batch' or 'parallel'
            save_mode: 'incremental' or 'overwrite'
            resume: Whether to resume from checkpoint
        """
        self.logger.info("="*60)
        self.logger.info("Starting ETL Pipeline")
        self.logger.info("="*60)
        
        metrics = ETLMetrics()
        metrics.start_time = datetime.now()
        metrics.total_records = len(input_data)
        
        # Check for checkpoint
        processed_codes = set()
        if resume:
            checkpoint = self.checkpoint_mgr.load_checkpoint()
            if checkpoint:
                processed_codes = set(checkpoint['processed_codes'])
                input_data = [r for r in input_data if r.get('hs_code') not in processed_codes]
                self.logger.info(f"Resuming: {len(input_data)} records remaining")
        
        # Process data
        if mode == 'parallel':
            processed = self.process_parallel(input_data)
            self.successful_records.extend(processed)
        else:
            # Batch processing
            for i in range(0, len(input_data), self.config.BATCH_SIZE):
                batch = input_data[i:i + self.config.BATCH_SIZE]
                processed = self.process_batch(batch, i // self.config.BATCH_SIZE)
                self.successful_records.extend(processed)
                
                # Periodic save
                if len(self.successful_records) % 50 == 0:
                    self.loader.save_json(self.successful_records, self.config.CLEAN_DATA_JSON, save_mode)
        
        # Final save
        self.logger.info("Saving final results...")
        self.loader.save_json(self.successful_records, self.config.CLEAN_DATA_JSON, save_mode)
        self.loader.save_csv(self.successful_records, self.config.CLEAN_DATA_CSV, save_mode)
        
        if self.failed_records:
            self.loader.save_failed_records(self.failed_records, self.config.FAILED_DATA_JSON)
        
        # Save summary report
        if self.summaries:
            with open(self.config.SUMMARY_REPORT, 'w', encoding='utf-8') as f:
                json.dump(self.summaries, f, ensure_ascii=False, indent=2)
        
        # Clear checkpoint on success
        if self.config.ENABLE_CHECKPOINTING:
            self.checkpoint_mgr.clear_checkpoint()
        
        # Update metrics
        metrics.end_time = datetime.now()
        metrics.successful = len(self.successful_records)
        metrics.failed = len(self.failed_records)
        metrics.warnings = sum(1 for r in self.successful_records if r.validation_warnings)
        
        # Log summary
        self.logger.info("="*60)
        self.logger.info("ETL Pipeline Complete")
        self.logger.info(f"Total: {metrics.total_records}")
        self.logger.info(f"Successful: {metrics.successful}")
        self.logger.info(f"Failed: {metrics.failed}")
        self.logger.info(f"Warnings: {metrics.warnings}")
        self.logger.info(f"Duration: {metrics.duration():.2f}s")
        self.logger.info(f"Success Rate: {(metrics.successful/metrics.total_records*100):.2f}%")
        self.logger.info("="*60)
        
        # Save metrics
        with open(self.config.OUTPUT_DIR / "etl_metrics.json", 'w') as f:
            json.dump(metrics.to_dict(), f, indent=2)
        
        return metrics


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def process_with_validation(raw_record: Dict, etl: RobustETL = None) -> Dict:
    """
    Process a single record and return structured data
    For integration with scraper
    """
    if etl is None:
        etl = RobustETL()
    
    extracted = etl.extract(raw_record)
    if not extracted:
        return raw_record
    
    record, validation = etl.transform(extracted)
    
    if record:
        return record.to_dict()
    else:
        return {
            **raw_record,
            "validation_failed": True,
            "errors": validation.errors
        }


def run_etl_pipeline(input_file: str, 
                     mode: str = 'batch',
                     max_workers: int = 4,
                     resume: bool = True):
    """
    Run ETL pipeline on a JSON file
    
    Args:
        input_file: Path to input JSON file
        mode: 'batch' or 'parallel'
        max_workers: Number of parallel workers
        resume: Resume from checkpoint
    """
    # Load input data
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Setup config
    config = ETLConfig()
    config.MAX_WORKERS = max_workers
    
    # Run pipeline
    pipeline = ETLPipeline(config)
    metrics = pipeline.run(data, mode=mode, resume=resume)
    
    print("\n" + "="*60)
    print("ETL PIPELINE SUMMARY")
    print("="*60)
    print(f"Input file: {input_file}")
    print(f"Total records: {metrics.total_records}")
    print(f"Successful: {metrics.successful} ({metrics.successful/metrics.total_records*100:.1f}%)")
    print(f"Failed: {metrics.failed}")
    print(f"Duration: {metrics.duration():.2f} seconds")
    print(f"\nOutput files:")
    print(f"  - Clean JSON: {config.CLEAN_DATA_JSON}")
    print(f"  - Clean CSV: {config.CLEAN_DATA_CSV}")
    if metrics.failed > 0:
        print(f"  - Failed records: {config.FAILED_DATA_JSON}")
    print(f"  - Summary report: {config.SUMMARY_REPORT}")
    print(f"  - Log file: {config.LOG_FILE}")
    print("="*60)
    
    return metrics


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import sys
    
    print("ETL Pipeline for ADIL Scraper")
    print("="*60)
    print("This module processes scraped ADIL data.")
    print("="*60)
    print("\nUsage:")
    print("  python etl.py <input_file.json>")
    print("\nExample:")
    print("  python etl.py etl_output/scraped_raw.json")
    print("="*60)
    
    # If a file is provided as argument, process it
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        print(f"\nProcessing file: {input_file}")
        
        try:
            metrics = run_etl_pipeline(
                input_file,
                mode='parallel',
                max_workers=4,
                resume=True
            )
        except FileNotFoundError:
            print(f"Error: File '{input_file}' not found!")
        except Exception as e:
            print(f"Error processing file: {e}")
    else:
        print("\n⚠ No input file specified!")
        print("Please provide a JSON file to process.")

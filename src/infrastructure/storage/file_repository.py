"""
File repository implementations
"""

import json
import csv
import os
import re
from typing import Optional, List
from datetime import datetime

from ...domain.entities import ScrapedData
from ...domain.repositories import FileRepository


class JsonFileRepository(FileRepository):
    """JSON-based file repository implementation"""
    
    def save(self, data: ScrapedData, filepath: str) -> bool:
        """Save scraped data to JSON file"""
        try:
            os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data.to_dict(), f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False
    
    def load(self, filepath: str) -> Optional[ScrapedData]:
        """Load scraped data from JSON file"""
        try:
            if not os.path.exists(filepath):
                return None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data_dict = json.load(f)
            
            # Reconstruct ScrapedData from dict
            # This is a simplified version - you might want to use a proper deserializer
            from ...domain.entities import BasicInfo, SectionData, StructuredData, Metadata
            
            basic_info_dict = data_dict.get('basic_info', {})
            basic_info = BasicInfo(
                tariff_code=basic_info_dict.get('tariff_code'),
                product_description=basic_info_dict.get('product_description'),
                effective_date=basic_info_dict.get('effective_date'),
                metadata=Metadata(basic_info_dict.get('metadata', {}))
            )
            
            scraped_data = ScrapedData(
                tariff_code_searched=data_dict.get('tariff_code_searched'),
                basic_info=basic_info
            )
            
            # Load sections
            sections_dict = data_dict.get('sections', {})
            for section_name, section_dict in sections_dict.items():
                structured_dict = section_dict.get('structured_data', {})
                structured_data = StructuredData(
                    metadata=Metadata(structured_dict.get('metadata', {})),
                    tables=structured_dict.get('tables', []),
                    lists=structured_dict.get('lists', []),
                    section_specific=structured_dict.get('section_specific', {})
                )
                
                section_data = SectionData(
                    section_name=section_name,
                    structured_data=structured_data,
                    error=section_dict.get('error')
                )
                scraped_data.add_section(section_data)
            
            return scraped_data
            
        except Exception as e:
            print(f"Error loading file: {e}")
            return None
    
    def load_tariff_codes(self, filepath: str) -> List[str]:
        """Load tariff codes from file"""
        codes = []
        
        if not os.path.exists(filepath):
            return codes
        
        try:
            if filepath.lower().endswith('.csv'):
                with open(filepath, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        for cell in row:
                            code = cell.strip()
                            if code and code.isdigit():
                                codes.append(code)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        code = line.strip()
                        code = re.sub(r'[,\s]+', '', code)
                        if code and (code.isdigit() or re.match(r'^\d{10}$', code)):
                            codes.append(code)
        except Exception as e:
            print(f"Error reading file: {e}")
            return codes
        
        # Remove duplicates
        seen = set()
        unique_codes = []
        for code in codes:
            if code not in seen:
                seen.add(code)
                unique_codes.append(code)
        
        return unique_codes
    
    def save_all(self, data_list: List[ScrapedData], filepath: str) -> bool:
        """Save multiple scraped data to a single JSON file"""
        try:
            os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump([data.to_dict() for data in data_list], f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False


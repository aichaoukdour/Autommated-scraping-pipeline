"""
Output Formatters
"""

import json
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Any


class OutputFormatter(ABC):
    """Abstract base class for output formatters"""
    
    @abstractmethod
    def save_single_result(self, data: Dict[str, Any], filepath: str) -> bool:
        """Save a single scraping result"""
        pass
    
    @abstractmethod
    def save_multiple_results(self, data_list: List[Dict[str, Any]], filepath: str) -> bool:
        """Save multiple scraping results"""
        pass


class JsonOutputFormatter(OutputFormatter):
    """JSON output formatter"""
    
    def save_single_result(self, data: Dict[str, Any], filepath: str) -> bool:
        """Save single result to JSON file"""
        try:
            os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False
    
    def save_multiple_results(self, data_list: List[Dict[str, Any]], filepath: str) -> bool:
        """Save multiple results to JSON file"""
        try:
            os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data_list, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False


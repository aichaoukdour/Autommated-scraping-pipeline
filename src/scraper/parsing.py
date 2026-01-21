import re
from typing import Dict, List, Optional, Any
from collections import defaultdict
from bs4 import BeautifulSoup, Tag
from .models import ContentData

class TextProcessor:
    """Extracts structured data from HTML content using BeautifulSoup"""
    
    KEY_VALUE_PATTERN = re.compile(r'([^:]+?)\s*(?:\([^)]*\))?\s*:\s*([^\n]+)')
    
    METADATA_PATTERNS = {
        'position': r'Position tarifaire\s*:?\s*([^\n<]+)',
        'source': r'Source\s*:?\s*([^\n<]+)',
        'date': r'Situation du\s*:?\s*([^\n<]+)',
        'unit': r'Unité.*?:?\s*([^\n<]+)'
    }
    
    SECTION_TYPE_KEYWORDS = {
        'statistics': [r'\d{4}.*\d{4}', r'importation', 'exportation', 'statistique'],
        'financial': [r'\d+\.?\d*\s*%', 'droit', 'taxe', 'tva'],
        'geography': ['pays', 'country', 'ue', 'agadir', 'turquie'],
        'regulatory': ['accord', 'restriction', 'prohibition', 'document', 'norme']
    }

    @classmethod
    def process_content(cls, html_content: str) -> ContentData:
        """Process HTML into structured content using BeautifulSoup"""
        if not html_content:
            return cls._empty_content()

        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements to clean up text extraction
        for script in soup(["script", "style"]):
            script.decompose()

        # Get clean text
        clean_text = soup.get_text(separator="\n", strip=True)
        
        return ContentData(
            raw_text=clean_text,
            metadata=cls.extract_metadata(clean_text),
            key_values=cls.extract_key_value_pairs(clean_text),
            tables=cls.extract_html_tables(soup),  # Pass soup object directly
            length=len(clean_text)
        )

    @staticmethod
    def _empty_content() -> ContentData:
        return ContentData("", {}, {}, [], 0)

    @classmethod
    def extract_html_tables(cls, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extracts data from actual HTML <table> tags.
        Filters out layout tables (tables used for visual structure only).
        """
        tables_data = []
        html_tables = soup.find_all("table")

        for table in html_tables:
            # Skip tables inside other tables (nested) to avoid duplication
            if table.find_parent("table"):
                continue

            parsed_table = cls._parse_html_table(table)
            if parsed_table:
                tables_data.append(parsed_table)
        
        return tables_data

    @classmethod
    def _parse_html_table(cls, table_tag: Tag) -> Optional[Dict[str, Any]]:
        """Parses a single BeautifulSoup table tag"""
        rows = table_tag.find_all("tr")
        if not rows or len(rows) < 2:
            return None

        # Strategy: Look for the best candidate for a header row
        header_row = None
        data_start_index = 0

        # Try to find a row with <th> tags
        for i, row in enumerate(rows[:3]): # Check first 3 rows
            if row.find("th"):
                header_row = row
                data_start_index = i + 1
                break
        
        # Fallback: First row with meaningful text if no <th> found
        if not header_row:
            header_row = rows[0]
            data_start_index = 1

        # Extract headers
        headers = [cls._clean_cell(cell.get_text()) for cell in header_row.find_all(["th", "td"])]
        
        # Filter out purely empty header lists
        if not any(h for h in headers if h):
            return None

        data_rows = []
        for row in rows[data_start_index:]:
            cells = row.find_all(["td", "th"])
            
            if len(cells) == 0:
                continue

            row_data = {}
            has_data = False
            
            # Safe looping using zip
            for idx, cell in enumerate(cells):
                if idx < len(headers) and headers[idx]:
                    val = cls._normalize_cell(cell.get_text())
                    row_data[headers[idx]] = val
                    if val:
                        has_data = True
            
            if has_data:
                data_rows.append(row_data)

        if not data_rows:
            return None

        return {
            "headers": [h for h in headers if h],
            "rows": data_rows,
            "row_count": len(data_rows)
        }

    @staticmethod
    def _clean_cell(text: str) -> str:
        """Clean header text"""
        text = text.replace('\xa0', ' ').replace('\n', ' ')
        return " ".join(text.strip().split())

    @staticmethod
    def _normalize_cell(value: str) -> Any:
        v = value.strip()
        if not v: return ""
        
        # Percentages
        if v.endswith("%"):
            try:
                return float(v.replace("%", "").replace(",", ".")) / 100
            except ValueError:
                return v
        
        return v

    @classmethod
    def extract_key_value_pairs(cls, text: str) -> Dict[str, str]:
        pairs = {}
        for key, value in cls.KEY_VALUE_PATTERN.findall(text):
            clean_key = key.strip()
            clean_value = value.strip()
            if clean_key and clean_value and len(clean_value) < 200:
                pairs[clean_key] = clean_value
        return pairs
    
    @classmethod
    def extract_metadata(cls, text: str) -> Dict[str, str]:
        metadata = {}
        for key, pattern in cls.METADATA_PATTERNS.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                metadata[key] = match.group(1).strip()
        return metadata

    @classmethod
    def detect_section_type(cls, section_name: str, content: str) -> str:
        clean_content = re.sub(r'<[^>]+>', ' ', content).lower()
        section_name_lower = section_name.lower()
        
        for s_type, keywords in cls.SECTION_TYPE_KEYWORDS.items():
            if any(k in section_name_lower for k in keywords if isinstance(k, str) and not k.startswith('\\')):
                return s_type

        scores = defaultdict(int)
        for section_type, patterns in cls.SECTION_TYPE_KEYWORDS.items():
            for pattern in patterns:
                if re.search(pattern, clean_content):
                    scores[section_type] += 1
        
        if not scores:
            return 'general_info'
        return max(scores.items(), key=lambda x: x[1])[0]

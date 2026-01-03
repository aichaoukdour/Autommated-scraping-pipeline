"""
Moroccan Customs ADIL Scraper
Scrapes HS code data from douane.gov.ma with dynamic content detection
"""

import json
import csv
import time
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class DataProcessor:
    """Extracts structured data from raw text"""
    
    @staticmethod
    def extract_key_value_pairs(text):
        pairs = {}
        pattern = r'([^:]+?)\s*(?:\([^)]*\))?\s*:\s*([^\n]+)'
        
        for key, value in re.findall(pattern, text):
            clean_key = key.strip()
            clean_value = value.strip()
            if clean_key and clean_value:
                pairs[clean_key] = clean_value
        
        return pairs
    
    @staticmethod
    def extract_tables(text):
        tables = []
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        year_pattern = r'\b(19|20)\d{2}\b'
        current_table = []
        
        for line in lines:
            if re.search(year_pattern, line):
                current_table.append(line)
            elif current_table and any(c.isdigit() for c in line):
                current_table.append(line)
            else:
                if len(current_table) > 1:
                    tables.append('\n'.join(current_table))
                current_table = []
        
        if len(current_table) > 1:
            tables.append('\n'.join(current_table))
        
        return tables
    
    @staticmethod
    def detect_section_type(section_name, content):
        content_lower = content.lower()
        scores = defaultdict(int)
        
        # Score based on content patterns
        if re.search(r'\d{4}.*\d{4}', content):
            scores['statistics'] += 3
        if re.search(r'\d+[,\s]\d+', content):
            scores['statistics'] += 2
        if re.search(r'\d+\.?\d*\s*%', content):
            scores['financial'] += 3
        
        country_keywords = ['pays', 'country', 'france', 'espagne', 'allemagne']
        if any(kw in content_lower for kw in country_keywords):
            scores['geography'] += 2
        
        if re.search(r'section|chapitre|branche|division', content_lower):
            scores['classification'] += 3
        
        legal_terms = ['accord', 'treaty', 'restriction', 'prohibition', 'document']
        if any(term in content_lower for term in legal_terms):
            scores['regulatory'] += 2
        
        return max(scores.items(), key=lambda x: x[1])[0] if scores else 'general'
    
    @staticmethod
    def extract_metadata(text):
        metadata = {}
        patterns = {
            'position': r'Position tarifaire\s*:?\s*([^\n]+)',
            'source': r'Source\s*:?\s*([^\n]+)',
            'date': r'Situation du\s*:?\s*([^\n]+)',
            'period': r'Période.*?:?\s*([^\n]+)',
            'intercom': r'Intercom\s*:?\s*([^\n]+)',
            'unit': r'Unité.*?:?\s*([^\n]+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                metadata[key] = match.group(1).strip()
        
        return metadata


class ADILScraper:
    BASE_URL = "https://www.douane.gov.ma/adil/c_bas_test_1.asp"
    MAX_RETRIES = 3
    WAIT_TIMEOUT = 10
    
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, self.WAIT_TIMEOUT)
        self.processor = DataProcessor()
        
        # Hide webdriver detection
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
        })
    
    def scrape_hs_code(self, hs_code):
        thread_id = threading.current_thread().name
        print(f"[{thread_id}] Scraping: {hs_code}")
        
        try:
            # Submit search form
            self.driver.get(self.BASE_URL)
            time.sleep(3)
            
            input_field = self.driver.find_element(By.NAME, "lposition")
            input_field.send_keys(hs_code)
            self.driver.find_element(By.NAME, "submit").click()
            print(f"[{thread_id}]   Form submitted")
            time.sleep(4)
            
            # Build result structure
            result = {
                "hs_code": hs_code,
                "scraped_at": datetime.now().isoformat(),
                "scrape_status": "success",
                "main_content": {},
                "sections": []
            }
            
            self._scrape_main_content(result, thread_id)
            self._scrape_all_sections(result, thread_id)
            self._add_summary_stats(result, thread_id)
            
            return result
            
        except Exception as e:
            print(f"[{thread_id}]   Fatal error: {e}")
            return {
                "hs_code": hs_code,
                "scraped_at": datetime.now().isoformat(),
                "scrape_status": "error",
                "error": str(e)
            }
    
    def _scrape_main_content(self, result, thread_id):
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(2)
            
            body = self.driver.find_element(By.TAG_NAME, "body")
            main_text = body.text.strip()
            
            result["main_content"] = {
                "raw_text": main_text,
                "metadata": self.processor.extract_metadata(main_text),
                "key_values": self.processor.extract_key_value_pairs(main_text),
                "length": len(main_text)
            }
            
            print(f"[{thread_id}]   ✓ Main content processed")
        except Exception as e:
            print(f"[{thread_id}]   ✗ Main content: {e}")
    
    def _scrape_all_sections(self, result, thread_id):
        try:
            self.driver.switch_to.default_content()
            self.driver.switch_to.frame(1)
            
            links = self.driver.find_elements(By.TAG_NAME, "a")
            section_links = self._get_section_links(links)
            
            print(f"[{thread_id}]   Found {len(section_links)} sections")
            
            for idx, link_info in enumerate(section_links):
                self._scrape_section_with_retry(result, link_info, idx, len(section_links), thread_id)
                
        except Exception as e:
            print(f"[{thread_id}]   ✗ Sidebar error: {e}")
            result["scrape_status"] = "partial"
    
    def _get_section_links(self, links):
        section_links = []
        seen = set()
        skip_keywords = ['nouvelle recherche', 'recherche', 'retour', 'accueil', 'home']
        
        for link in links:
            try:
                text = link.text.strip()
                href = link.get_attribute("href")
                
                if text and text not in seen and not any(kw in text.lower() for kw in skip_keywords):
                    section_links.append({"name": text, "href": href})
                    seen.add(text)
            except:
                continue
        
        return section_links
    
    def _scrape_section_with_retry(self, result, link_info, idx, total, thread_id):
        section_name = link_info["name"]
        
        for attempt in range(self.MAX_RETRIES):
            try:
                self._scrape_section(result, section_name, idx, total, thread_id)
                return
            except (NoSuchElementException, StaleElementReferenceException, TimeoutException):
                if attempt < self.MAX_RETRIES - 1:
                    print(f"[{thread_id}]   ⟳ [{idx+1}/{total}] Retry {attempt+1} for {section_name}")
                    time.sleep(2)
                    self.driver.switch_to.default_content()
                else:
                    self._add_unavailable_section(result, section_name, idx, thread_id, total)
            except Exception as e:
                print(f"[{thread_id}]   ✗ [{idx+1}/{total}] Error: {str(e)[:100]}")
                break
    
    def _scrape_section(self, result, section_name, idx, total, thread_id):
        # Navigate to section
        self.driver.switch_to.default_content()
        time.sleep(1)
        self.driver.switch_to.frame(1)
        
        xpath = f'//a[normalize-space(text())="{section_name}"]' if "'" in section_name else f"//a[normalize-space(text())='{section_name}']"
        link = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        self.driver.execute_script("arguments[0].scrollIntoView(true);", link)
        time.sleep(0.5)
        link.click()
        time.sleep(2)
        
        # Extract content
        self.driver.switch_to.default_content()
        self.driver.switch_to.frame(2)
        body = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        content = body.text.strip() if body.text.strip() and len(body.text.strip()) >= 10 else "N/A"
        
        section_data = self._build_section_data(section_name, content, idx)
        result["sections"].append(section_data)
        self._log_section(section_name, section_data, idx, total, thread_id)
    
    def _build_section_data(self, section_name, content, idx):
        is_empty = content == "N/A"
        
        return {
            "section_name": section_name,
            "section_type": "empty" if is_empty else self.processor.detect_section_type(section_name, content),
            "content": {
                "raw_text": content,
                "metadata": {} if is_empty else self.processor.extract_metadata(content),
                "key_values": {} if is_empty else self.processor.extract_key_value_pairs(content),
                "tables": [] if is_empty else self.processor.extract_tables(content),
                "length": len(content)
            },
            "scraped_at": datetime.now().isoformat(),
            "order": idx
        }
    
    def _add_unavailable_section(self, result, section_name, idx, thread_id, total):
        result["sections"].append({
            "section_name": section_name,
            "section_type": "unavailable",
            "content": {"raw_text": "N/A", "metadata": {}, "key_values": {}, "tables": [], "length": 0},
            "scraped_at": datetime.now().isoformat(),
            "order": idx,
            "status": "not_available"
        })
        print(f"[{thread_id}]   N/A [{idx+1}/{total}] {section_name}")
    
    def _log_section(self, section_name, section_data, idx, total, thread_id):
        is_na = section_data["content"]["raw_text"] == "N/A"
        status = "N/A" if is_na else "✓"
        print(f"[{thread_id}]   {status} [{idx+1}/{total}] {section_name}")
        
        if not is_na:
            kv_count = len(section_data["content"]["key_values"])
            table_count = len(section_data["content"]["tables"])
            print(f"[{thread_id}]      Type: {section_data['section_type']} | KV: {kv_count} | Tables: {table_count}")
    
    def _add_summary_stats(self, result, thread_id):
        result["summary"] = {
            "total_sections": len(result["sections"]),
            "successful_sections": sum(1 for s in result["sections"] if "error" not in s),
            "failed_sections": sum(1 for s in result["sections"] if "error" in s),
            "section_types": {}
        }
        
        for section in result["sections"]:
            if "section_type" in section:
                stype = section["section_type"]
                result["summary"]["section_types"][stype] = result["summary"]["section_types"].get(stype, 0) + 1
        
        print(f"[{thread_id}]   Complete: {result['summary']['total_sections']} sections")
        print(f"[{thread_id}]   Types: {result['summary']['section_types']}")
    
    def close(self):
        self.driver.quit()


def scrape_single_code(hs_code):
    scraper = ADILScraper()
    try:
        return scraper.scrape_hs_code(hs_code)
    finally:
        scraper.close()


def process_csv(csv_path, max_workers=3, limit=None):
    hs_codes = _read_codes_from_csv(csv_path, limit)
    
    print(f"\n{'='*60}")
    print(f"Processing {len(hs_codes)} HS codes with {max_workers} workers")
    print(f"{'='*60}\n")
    
    results = _run_parallel_scraping(hs_codes, max_workers)
    _save_results(results, max_workers)
    
    return results


def _read_codes_from_csv(csv_path, limit):
    hs_codes = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            hs_codes.append(row['hs_code'].strip())
            if limit and len(hs_codes) >= limit:
                break
    return hs_codes


def _run_parallel_scraping(hs_codes, max_workers):
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {executor.submit(scrape_single_code, code): code for code in hs_codes}
        
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                result = future.result()
                results.append(result)
                print(f"\n{'='*60}")
                print(f"✓ Completed: {code} ({len(results)}/{len(hs_codes)})")
                print(f"{'='*60}\n")
            except Exception as e:
                print(f"\n✗ Failed: {code} - {e}\n")
    
    return results


def _save_results(results, max_workers):
    with open("adil_detailed.json", 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    summary = _build_summary(results, max_workers)
    
    with open("adil_summary.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def _build_summary(results, max_workers):
    total_sections = sum(r.get("summary", {}).get("total_sections", 0) for r in results)
    section_types = defaultdict(int)
    
    for result in results:
        if "summary" in result and "section_types" in result["summary"]:
            for stype, count in result["summary"]["section_types"].items():
                section_types[stype] += count
    
    return {
        "pipeline_metadata": {
            "run_timestamp": datetime.now().isoformat(),
            "total_codes_processed": len(results),
            "max_workers": max_workers
        },
        "status_breakdown": {
            "success": sum(1 for r in results if r.get("scrape_status") == "success"),
            "partial": sum(1 for r in results if r.get("scrape_status") == "partial"),
            "error": sum(1 for r in results if r.get("scrape_status") == "error")
        },
        "section_analytics": {
            "total_sections_scraped": total_sections,
            "avg_sections_per_code": round(total_sections / len(results), 2) if results else 0,
            "section_type_distribution": dict(section_types)
        },
        "results": results
    }


if __name__ == "__main__":
    MAX_WORKERS = 1
    LIMIT = 1
    
    csv_paths = ["../Code Sh Import - Feuil.csv"]
    
    for path in csv_paths:
        if os.path.exists(path):
            print(f"Found CSV: {path}")
            process_csv(path, max_workers=MAX_WORKERS, limit=LIMIT)
            break
    else:
        print("Error: CSV file not found!")
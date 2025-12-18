"""
Moroccan Customs ADIL Scraper - Parallelized Version
"""

import json
import csv
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import threading
from etl import run_etl_pipeline


class ADILScraper:
    BASE_URL = "https://www.douane.gov.ma/adil/c_bas_test_1.asp"
    
    SECTIONS = [
        "Droits et Taxes",
        "Documents", 
        "Accords",
        "Historique"
    ]
    
    def __init__(self):
        """Initialize a browser instance per thread"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=options)
        
        # Remove webdriver detection
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                window.chrome = {
                    runtime: {}
                };
            '''
        })
    
    def _clean_text(self, html):
        """Extract clean text from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script', 'style']):
            tag.decompose()
        return ' '.join(soup.get_text(separator=' ', strip=True).split())
    
    def scrape_hs_code(self, hs_code):
        """Scrape data for one HS code"""
        thread_id = threading.current_thread().name
        print(f"[{thread_id}] Scraping: {hs_code}")
        
        try:
            # Load page
            self.driver.get(self.BASE_URL)
            time.sleep(3)
            
            # Find input and submit
            input_field = self.driver.find_element(By.NAME, "lposition")
            input_field.send_keys(hs_code)
            self.driver.find_element(By.NAME, "submit").click()
            print(f"[{thread_id}]   Form submitted")
            time.sleep(4)
            
            # Switch to default content to access frames
            self.driver.switch_to.default_content()
            
            data = []
            
            # STEP 1: Get main content (product name/description from Frame 2)
            try:
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame(2)
                
                body = self.driver.find_element(By.TAG_NAME, "body")
                main_content = body.text.strip()
                
                data.append(main_content)
                print(f"[{thread_id}]   ✓ Main Content: {len(main_content)} chars")
            except Exception as e:
                print(f"[{thread_id}]   ✗ Main Content: {e}")
                data.append("")
            
            # STEP 2: Get sidebar menu text (Frame 1)
            try:
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame(1)
                
                body = self.driver.find_element(By.TAG_NAME, "body")
                menu_text = body.text.strip()
                
                data.append(menu_text)
                print(f"[{thread_id}]   ✓ Sidebar Menu: {len(menu_text)} chars")
            except Exception as e:
                print(f"[{thread_id}]   ✗ Sidebar Menu: {e}")
                data.append("")
            
            # STEP 3: Click each section link and extract content
            for idx, section_name in enumerate(self.SECTIONS):
                try:
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(1)
                    
                    link = self.driver.find_element(By.XPATH, f"//*[contains(text(), '{section_name}')]")
                    link.click()
                    time.sleep(3)
                    
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(2)
                    
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    text = body.text.strip()
                    
                    data.append(text)
                    print(f"[{thread_id}]   ✓ {section_name}: {len(text)} chars")
                    
                except Exception as e:
                    print(f"[{thread_id}]   ✗ {section_name}: {e}")
                    data.append("")
            
            # Pad to ensure we have 6 elements
            while len(data) < 6:
                data.append("")
            
            return {"hs_code": hs_code, "data": data}
            
        except Exception as e:
            print(f"[{thread_id}]   Error: {e}")
            return {"hs_code": hs_code, "data": ["", "", "", "", "", ""]}
    
    def close(self):
        """Close the browser"""
        self.driver.quit()


def scrape_single_code(hs_code):
    """Worker function that creates its own scraper instance"""
    scraper = ADILScraper()
    try:
        return scraper.scrape_hs_code(hs_code)
    finally:
        scraper.close()


def process_csv_parallel(csv_path, max_workers=3, limit=None):
    """
    Process CSV file with parallel workers
    
    Args:
        csv_path: Path to CSV file
        max_workers: Number of parallel browser instances (default: 3)
        limit: Number of codes to process (None for all)
    """
    # Read HS codes
    hs_codes = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            hs_codes.append(row['hs_code'].strip())
            if limit and len(hs_codes) >= limit:
                break
    
    print(f"Processing {len(hs_codes)} HS codes with {max_workers} workers...")
    
    results = []
    
    # Process in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_code = {
            executor.submit(scrape_single_code, code): code 
            for code in hs_codes
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                result = future.result()
                results.append(result)
                print(f"\n✓ Completed: {code} ({len(results)}/{len(hs_codes)})")
            except Exception as e:
                print(f"\n✗ Failed: {code} - {e}")
                results.append({"hs_code": code, "data": ["", "", "", "", "", ""], "error": str(e)})
    
    # Save raw results for ETL processing
    raw_output = "src/adil_results_raw.json"
    with open(raw_output, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\nSaved raw data to: {raw_output}")
    print(f"Running ETL Pipeline...")
    
    # Run the robust ETL pipeline
    try:
        metrics = run_etl_pipeline(
            raw_output,
            mode='parallel',
            max_workers=max_workers,
            resume=False  # Start fresh for this run
        )
        
        print(f"\n{'='*60}")
        print(f"✓ SCRAPING & ETL DONE")
        print(f"✓ Total records: {metrics.total_records}")
        print(f"✓ Successful: {metrics.successful}")
        print(f"✓ Output directory: etl_output/")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error running ETL pipeline: {e}")
    
    return results


if __name__ == "__main__":
    import os
    
    # Configuration
    MAX_WORKERS = 3  # Number of parallel browsers
    LIMIT = 3        # Process only first 3 codes (set to None for all)
    
    # Find CSV
    csv_paths = [
        "Code Sh Import - Feuil.csv",
        "hs_codes.csv",
        "../Code Sh Import - Feuil.csv"
    ]
    
    for path in csv_paths:
        if os.path.exists(path):
            print(f"Found CSV: {path}")
            process_csv_parallel(path, max_workers=MAX_WORKERS, limit=LIMIT)
            break
    else:
        print("Error: CSV file not found!")
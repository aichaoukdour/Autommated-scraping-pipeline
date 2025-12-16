"""
Moroccan Customs ADIL Scraper - Simplified
"""

import json
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup


class ADILScraper:
    BASE_URL = "https://www.douane.gov.ma/adil/c_bas_test_1.asp"
    
    # The 4 sections to scrape
    SECTIONS = [
        "Droits et Taxes",
        "Documents", 
        "Accords",
        "Historique"
    ]
    
    def __init__(self):
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
        print(f"\nScraping: {hs_code}")
        
        try:
            # Load page
            self.driver.get(self.BASE_URL)
            time.sleep(3)
            
            # Find input and submit
            input_field = self.driver.find_element(By.NAME, "lposition")
            input_field.send_keys(hs_code)
            self.driver.find_element(By.NAME, "submit").click()
            print(f"  Form submitted")
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
                main_content = ' '.join(main_content.split())
                
                data.append(main_content)
                print(f"  ✓ Main Content (Product): {len(main_content)} chars")
                
                with open("debug_main_content.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
            except Exception as e:
                print(f"  ✗ Main Content: {e}")
                data.append("")
            
            # STEP 2: Get sidebar menu text (Frame 1)
            try:
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame(1)
                
                body = self.driver.find_element(By.TAG_NAME, "body")
                menu_text = body.text.strip()
                menu_text = ' '.join(menu_text.split())
                
                data.append(menu_text)
                print(f"  ✓ Sidebar Menu: {len(menu_text)} chars")
            except Exception as e:
                print(f"  ✗ Sidebar Menu: {e}")
                data.append("")
            
            # STEP 3: Click each section link and extract content
            for idx, section_name in enumerate(self.SECTIONS):
                try:
                    # Go to sidebar frame (Frame 1)
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(1)
                    
                    # Find and click the section link
                    link = self.driver.find_element(By.XPATH, f"//*[contains(text(), '{section_name}')]")
                    link.click()
                    time.sleep(3)
                    
                    # Switch to content frame (Frame 2)
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame(2)
                    
                    # Extract all visible text from the page
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    text = body.text.strip()
                    text = ' '.join(text.split())
                    
                    data.append(text)
                    print(f"  ✓ {section_name}: {len(text)} chars")
                    
                except Exception as e:
                    print(f"  ✗ {section_name}: {e}")
                    data.append("")
            
            # Pad to ensure we have 6 elements
            while len(data) < 6:
                data.append("")
            
            return {"hs_code": hs_code, "data": data}
            
        except Exception as e:
            print(f"  Error: {e}")
            return {"hs_code": hs_code, "data": ["", "", "", "", "", ""]}
    
    
    def process_csv(self, csv_path):
        """Process CSV file"""
        results = []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                hs_code = row['hs_code'].strip()
                raw_result = self.scrape_hs_code(hs_code)
                
                # ETL Integration
                from etl import clean_and_structure_data
                structured_data = clean_and_structure_data(raw_result['data'])
                # Ensure hs_code is preserved in the structured output
                structured_data['hs_code'] = hs_code
                
                results.append(structured_data)
                time.sleep(1)
        
        # Save structured data
        with open("adil_data_clean.json", 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Done: {len(results)} codes processed and cleaned.")
    
    def close(self):
        self.driver.quit()


if __name__ == "__main__":
    import os
    
    # Find CSV
    for path in ["hs_codes.csv", "../hs_codes.csv", "../Code Sh Import - Feuil.csv"]:
        if os.path.exists(path):
            scraper = ADILScraper()
            try:
                scraper.process_csv(path)
            finally:
                scraper.close()
            break
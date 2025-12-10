"""
HTTP-based scraper for ADIL (Moroccan Customs) website.
Extracts duty rates for HS codes without using a browser.
"""
import requests
import urllib3
import pandas as pd
import json
import logging
import re
import time
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_URL = "https://www.douane.gov.ma/adil/"
POST_URL = BASE_URL + "rsearch1.asp"
INFO2_URL = BASE_URL + "info_2.asp"
INFO1_URL = BASE_URL + "info_1.asp"

INPUT_CSV = "Code Sh Import - Feuille 1.csv"
OUTPUT_CSV = "products_data.csv"
OUTPUT_JSON = "products_data.json"
SAMPLE_SIZE = 10  # Set to None to process all codes

# Setup logging
logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded"
}


def pad_hs_code(hs_code: str) -> str:
    """Pad HS code to 10 digits if needed.
    
    First pads to 6 digits on the left (with zeros) to handle 
    codes that lost leading zeros when read as integers,
    then pads to 10 digits on the right with zeros.
    """
    hs_code = str(hs_code).strip()
    # First ensure we have at least 6 digits (left-pad with zeros)
    if len(hs_code) < 6:
        hs_code = hs_code.zfill(6)
    # Then pad to 10 digits on the right
    if len(hs_code) < 10:
        hs_code = hs_code.ljust(10, '0')
    return hs_code


def extract_rate_from_html(html_text: str, pattern_name: str) -> str:
    """Extract rate value from HTML using multiple patterns."""
    patterns = {
        "di": [
            r"Droit d'Importation[^:]*:[^<]*<span[^>]*>\s*([\d,\.]+)\s*%",
            r"DI[^:]*:\s*<span[^>]*>\s*([\d,\.]+)\s*%",
            r">\s*([\d,\.]+)\s*%\s*</span>\s*</span>\s*\n\s*</td></tr>\s*<tr[^>]*>\s*<td[^>]*>.*?TPI",
        ],
        "tpi": [
            r"TPI[^:]*:[^<]*<span[^>]*>.*?<b[^>]*>\s*([\d,\.]+)\s*%",
            r"Taxe Parafiscale[^:]*:[^<]*<span[^>]*>.*?<b[^>]*>\s*([\d,\.]+)\s*%",
            r"TPI[^<]*</span>\s*</span>\s*</p>\s*</div></td>\s*</tr>\s*<tr[^>]*>\s*<td[^>]*>.*?([\d,\.]+)\s*%",
        ],
        "tva": [
            r"TVA[^:]*:[^<]*<span[^>]*>.*?<b[^>]*>\s*([\d,\.]+)\s*%",
            r"Valeur Ajout[^:]*:[^<]*<span[^>]*>.*?<b[^>]*>\s*([\d,\.]+)\s*%",
        ]
    }
    
    for pattern in patterns.get(pattern_name, []):
        match = re.search(pattern, html_text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip() + "%"
    return "N/A"


def scrape_hs_code(session: requests.Session, hs_code: str) -> dict:
    """Scrape duty rates for a single HS code."""
    hs_code_padded = pad_hs_code(hs_code)
    logging.info(f"Processing HS code: {hs_code} -> padded: {hs_code_padded}")
    
    result = {
        "hs_code_input": hs_code,
        "description": "N/A",
        "di_rate": "N/A",
        "tpi_rate": "N/A",
        "tva_rate": "N/A"
    }
    
    try:
        # Step 1: POST to initialize session with HS code
        data = {
            "lposition": hs_code_padded,
            "submit": "Trouver..."
        }
        response = session.post(POST_URL, headers=HEADERS, data=data, verify=False, timeout=30)
        
        if response.status_code != 200:
            logging.warning(f"POST failed for {hs_code}: status {response.status_code}")
            return result
        
        # Step 2: Fetch info_1.asp for description
        response_info1 = session.get(
            INFO1_URL, 
            params={"pos": hs_code_padded}, 
            headers=HEADERS, 
            verify=False, 
            timeout=30
        )
        
        if response_info1.status_code == 200:
            soup1 = BeautifulSoup(response_info1.text, 'html.parser')
            # Look for description in the page
            desc_elements = soup1.find_all('td', class_='t')
            for elem in desc_elements:
                text = elem.get_text(strip=True)
                if text and len(text) > 5 and "Position" not in text:
                    result["description"] = text
                    break
        
        # Step 3: Fetch info_2.asp for duty rates
        response_info2 = session.get(
            INFO2_URL, 
            params={"pos": hs_code_padded}, 
            headers=HEADERS, 
            verify=False, 
            timeout=30
        )
        
        if response_info2.status_code == 200:
            html_text = response_info2.text
            
            # Extract rates using pattern matching
            result["di_rate"] = extract_rate_from_html(html_text, "di")
            result["tpi_rate"] = extract_rate_from_html(html_text, "tpi")
            result["tva_rate"] = extract_rate_from_html(html_text, "tva")
        
        logging.info(f"Scraped {hs_code}: DI={result['di_rate']}, TPI={result['tpi_rate']}, TVA={result['tva_rate']}")
        
    except Exception as e:
        logging.error(f"Error scraping {hs_code}: {e}")
    
    return result


def main():
    """Main function to run the scraper."""
    print("=" * 50)
    print("ADIL HTTP Scraper (No Browser)")
    print("=" * 50)
    
    # Read HS codes from CSV
    try:
        df = pd.read_csv(INPUT_CSV, dtype=str)  # Read as string to preserve leading zeros
        hs_codes = df.iloc[:, 0].tolist()
        print(f"Loaded {len(hs_codes)} HS codes from {INPUT_CSV}")
        logging.info(f"Loaded {len(hs_codes)} HS codes from {INPUT_CSV}")
    except Exception as e:
        print(f"Error reading {INPUT_CSV}: {e}")
        logging.error(f"Error reading {INPUT_CSV}: {e}")
        return
    
    # Create session
    session = requests.Session()
    results = []
    
    # Scrape each HS code
    codes_to_process = hs_codes[:SAMPLE_SIZE] if SAMPLE_SIZE else hs_codes
    print(f"Processing {len(codes_to_process)} codes (SAMPLE_SIZE={SAMPLE_SIZE})")
    
    for i, hs_code in enumerate(codes_to_process):
        print(f"[{i+1}/{len(codes_to_process)}] Scraping {hs_code}...")
        result = scrape_hs_code(session, hs_code)
        results.append(result)
        
        # Small delay to be polite
        time.sleep(0.5)
    
    # Save results
    if results:
        # Save CSV
        df_results = pd.DataFrame(results)
        df_results.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"Saved {len(results)} records to {OUTPUT_CSV}")
        logging.info(f"Saved {len(results)} records to {OUTPUT_CSV}")
        
        # Save JSON
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(f"Saved {len(results)} records to {OUTPUT_JSON}")
        logging.info(f"Saved {len(results)} records to {OUTPUT_JSON}")
    else:
        print("No data extracted.")
        logging.warning("No data extracted.")
    
    print("=" * 50)
    print("Scraping complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()

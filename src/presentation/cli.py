"""
Command Line Interface
"""

import argparse
from typing import List, Optional
import os

from ..application.use_cases import ScrapeTariffCodeUseCase, ScrapeMultipleTariffCodesUseCase
from ..application.dto import ScrapeTariffCodeRequest, ScrapeMultipleTariffCodesRequest
from ..infrastructure.scraping.playwright_repository import PlaywrightScrapingRepository
from ..infrastructure.storage.file_repository import JsonFileRepository
from ..infrastructure.storage.change_detection_repository import FileChangeDetectionRepository
from ..domain.value_objects import ScrapingConfiguration
from .formatters import OutputFormatter, JsonOutputFormatter


def create_cli_app():
    """Create and configure CLI application"""
    parser = argparse.ArgumentParser(
        description='ADiL Customs Website Scraper - Enterprise Architecture',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape single code
  python -m src.main --code 0804100000
  
  # Scrape from file
  python -m src.main --file codes.txt
  
  # Scrape multiple codes
  python -m src.main --codes 0804100000 0201100000
  
  # Headless mode
  python -m src.main --file codes.txt --headless
        """
    )
    
    parser.add_argument('--code', type=str, help='Single tariff code to scrape')
    parser.add_argument('--codes', nargs='+', help='Multiple tariff codes to scrape')
    parser.add_argument('--file', type=str, help='Path to file containing tariff codes')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--output-dir', type=str, default='.', help='Directory to save output files')
    parser.add_argument('--combined', action='store_true', help='Save all results in one combined JSON file')
    
    args = parser.parse_args()
    
    # Collect tariff codes
    tariff_codes = []
    
    if args.code:
        tariff_codes.append(args.code)
    
    if args.codes:
        tariff_codes.extend(args.codes)
    
    if args.file:
        file_repo = JsonFileRepository()
        file_codes = file_repo.load_tariff_codes(args.file)
        if file_codes:
            tariff_codes.extend(file_codes)
            print(f"[OK] Loaded {len(file_codes)} codes from {args.file}")
        else:
            print(f"[X] No valid codes found in {args.file}")
            return
    
    if not tariff_codes:
        print("="*70)
        print("ADiL COMPLETE DATA SCRAPER - ENTERPRISE ARCHITECTURE")
        print("="*70)
        print("\n[X] No tariff codes provided!")
        print("\nPlease provide codes using one of these methods:")
        print("  --code CODE          : Single tariff code")
        print("  --codes CODE1 CODE2  : Multiple codes")
        print("  --file PATH          : File with codes (CSV or TXT)")
        print("\nUse --help for more information.")
        return
    
    # Remove duplicates
    tariff_codes = list(dict.fromkeys(tariff_codes))
    
    print("="*70)
    print("ADiL COMPLETE DATA SCRAPER - ENTERPRISE ARCHITECTURE")
    print("="*70)
    print(f"\n[OK] Found {len(tariff_codes)} unique tariff code(s) to scrape")
    print(f"     Codes: {', '.join(tariff_codes[:5])}{'...' if len(tariff_codes) > 5 else ''}")
    
    # Create output directory
    if args.output_dir and not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # Setup dependency injection
    config = ScrapingConfiguration(headless=args.headless)
    scraping_repo = PlaywrightScrapingRepository(config)
    file_repo = JsonFileRepository()
    change_detection_repo = FileChangeDetectionRepository()
    
    # Create use cases
    scrape_use_case = ScrapeTariffCodeUseCase(scraping_repo, change_detection_repo)
    scrape_multiple_use_case = ScrapeMultipleTariffCodesUseCase(scraping_repo, change_detection_repo)
    
    # Execute based on number of codes
    formatter = JsonOutputFormatter()
    
    if len(tariff_codes) == 1:
        # Single code
        request = ScrapeTariffCodeRequest(
            tariff_code=tariff_codes[0],
            headless=args.headless
        )
        response = scrape_use_case.execute(request)
        
        if response.result.success:
            output_file = os.path.join(args.output_dir, f"adil_complete_{tariff_codes[0]}.json")
            formatter.save_single_result(response.result.data, output_file)
            print(f"\n[OK] Complete data saved to: {output_file}")
            
            if response.warnings:
                print("\n⚠️  Warnings:")
                for warning in response.warnings:
                    print(f"  • {warning}")
        else:
            print(f"\n[X] Failed to scrape: {response.result.error}")
    else:
        # Multiple codes
        request = ScrapeMultipleTariffCodesRequest(
            tariff_codes=tariff_codes,
            headless=args.headless
        )
        response = scrape_multiple_use_case.execute(request)
        
        if args.combined:
            # Save all in one file
            output_file = os.path.join(args.output_dir, 'adil_complete_all.json')
            successful_results = [r.data for r in response.results if r.success]
            formatter.save_multiple_results(successful_results, output_file)
            print(f"\n{'='*70}")
            print(f"SUMMARY")
            print(f"{'='*70}")
            print(f"[OK] Scraped {response.success_count}/{response.total_count} tariff codes")
            print(f"[OK] Combined results saved to: {output_file}")
        else:
            # Save individual files
            for result in response.results:
                if result.success:
                    output_file = os.path.join(args.output_dir, f"adil_complete_{result.tariff_code}.json")
                    formatter.save_single_result(result.data, output_file)
            
            print(f"\n{'='*70}")
            print(f"SUMMARY")
            print(f"{'='*70}")
            print(f"[OK] Successfully scraped {response.success_count}/{response.total_count} tariff codes")
            print(f"[OK] Individual files saved to: {args.output_dir}")
        
        if response.changes_detected:
            print("\n⚠️  WEBSITE CHANGES DETECTED:")
            for change in response.changes_detected:
                print(f"  • {change}")


if __name__ == "__main__":
    create_cli_app()


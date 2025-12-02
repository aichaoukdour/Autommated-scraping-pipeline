"""
Use Cases - Application-specific business logic
"""

from typing import Optional, List
import time
from datetime import datetime

from ..domain.entities import TariffCode, ScrapedData
from ..domain.repositories import ScrapingRepository, ChangeDetectionRepository
from .dto import (
    ScrapeTariffCodeRequest,
    ScrapeTariffCodeResponse,
    ScrapingResult,
    ScrapeMultipleTariffCodesRequest,
    ScrapeMultipleTariffCodesResponse,
    ChangeDetectionReport
)
from .services import ChangeDetectionService


class ScrapeTariffCodeUseCase:
    """Use case for scraping a single tariff code"""
    
    def __init__(
        self,
        scraping_repository: ScrapingRepository,
        change_detection_repository: Optional[ChangeDetectionRepository] = None
    ):
        self.scraping_repository = scraping_repository
        self.change_detection_repository = change_detection_repository
        self.change_detection_service = ChangeDetectionService(change_detection_repository) if change_detection_repository else None
    
    def execute(self, request: ScrapeTariffCodeRequest) -> ScrapeTariffCodeResponse:
        """Execute the scraping use case"""
        start_time = time.time()
        
        try:
            # Convert string to domain entity
            tariff_code = TariffCode(request.tariff_code)
            
            # Perform scraping
            scraped_data = self.scraping_repository.scrape(tariff_code)
            
            duration = time.time() - start_time
            
            if scraped_data:
                scraped_data.scraping_duration_seconds = duration
                result = ScrapingResult(
                    success=True,
                    tariff_code=request.tariff_code,
                    data=scraped_data.to_dict(),
                    duration_seconds=duration
                )
                
                # Handle change detection if enabled
                warnings = []
                if request.monitor_changes and self.change_detection_service:
                    strategy_log = getattr(self.scraping_repository, 'get_strategy_log', lambda: {})()
                    report = self.change_detection_service.check_changes(strategy_log)
                    if report and report.changes:
                        warnings.extend(report.changes)
                
                return ScrapeTariffCodeResponse(
                    result=result,
                    warnings=warnings
                )
            else:
                return ScrapeTariffCodeResponse(
                    result=ScrapingResult(
                        success=False,
                        tariff_code=request.tariff_code,
                        error="Scraping returned no data",
                        duration_seconds=duration
                    )
                )
        
        except ValueError as e:
            return ScrapeTariffCodeResponse(
                result=ScrapingResult(
                    success=False,
                    tariff_code=request.tariff_code,
                    error=f"Invalid tariff code: {str(e)}",
                    duration_seconds=time.time() - start_time
                )
            )
        except Exception as e:
            return ScrapeTariffCodeResponse(
                result=ScrapingResult(
                    success=False,
                    tariff_code=request.tariff_code,
                    error=f"Unexpected error: {str(e)}",
                    duration_seconds=time.time() - start_time
                )
            )


class ScrapeMultipleTariffCodesUseCase:
    """Use case for scraping multiple tariff codes"""
    
    def __init__(
        self,
        scraping_repository: ScrapingRepository,
        change_detection_repository: Optional[ChangeDetectionRepository] = None
    ):
        self.scraping_repository = scraping_repository
        self.change_detection_repository = change_detection_repository
        self.change_detection_service = ChangeDetectionService(change_detection_repository) if change_detection_repository else None
    
    def execute(self, request: ScrapeMultipleTariffCodesRequest) -> ScrapeMultipleTariffCodesResponse:
        """Execute the multiple scraping use case"""
        results = []
        
        # Load previous strategy log for change detection
        if request.monitor_changes and self.change_detection_service:
            self.change_detection_service.load_previous_log()
        
        # Convert to domain entities
        tariff_codes = []
        for code_str in request.tariff_codes:
            try:
                tariff_codes.append(TariffCode(code_str))
            except ValueError:
                results.append(ScrapingResult(
                    success=False,
                    tariff_code=code_str,
                    error=f"Invalid tariff code format: {code_str}"
                ))
        
        # Scrape each code
        for i, tariff_code in enumerate(tariff_codes, 1):
            start_time = time.time()
            
            try:
                scraped_data = self.scraping_repository.scrape(tariff_code)
                duration = time.time() - start_time
                
                if scraped_data:
                    scraped_data.scraping_duration_seconds = duration
                    results.append(ScrapingResult(
                        success=True,
                        tariff_code=str(tariff_code),
                        data=scraped_data.to_dict(),
                        duration_seconds=duration
                    ))
                else:
                    results.append(ScrapingResult(
                        success=False,
                        tariff_code=str(tariff_code),
                        error="Scraping returned no data",
                        duration_seconds=duration
                    ))
            
            except Exception as e:
                results.append(ScrapingResult(
                    success=False,
                    tariff_code=str(tariff_code),
                    error=str(e),
                    duration_seconds=time.time() - start_time
                ))
            
            # Delay between requests (except for last one)
            if i < len(tariff_codes) and request.delay_between_requests > 0:
                time.sleep(request.delay_between_requests)
        
        # Detect and report changes
        changes_detected = []
        if request.monitor_changes and self.change_detection_service:
            strategy_log = getattr(self.scraping_repository, 'get_strategy_log', lambda: {})()
            report = self.change_detection_service.detect_and_report(strategy_log)
            if report:
                changes_detected = report.changes
        
        success_count = sum(1 for r in results if r.success)
        failed_count = len(results) - success_count
        
        return ScrapeMultipleTariffCodesResponse(
            results=results,
            total_count=len(results),
            success_count=success_count,
            failed_count=failed_count,
            changes_detected=changes_detected
        )


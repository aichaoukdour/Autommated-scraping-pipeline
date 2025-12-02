"""
Real-Time Scraping Service
Continuous monitoring and automatic scraping
"""

import time
import logging
import signal
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from ...domain.entities import TariffCode, ScrapedData
from ...domain.repositories import ScrapingRepository
from ...domain.value_objects import MonitoringConfiguration
from ...application.services import DataChangeDetectionService
from ..storage.cached_repository import CachedRepository

logger = logging.getLogger(__name__)


class RealTimeScrapingService:
    """Real-time scraping service that runs continuously"""
    
    def __init__(
        self,
        scraping_repository: ScrapingRepository,
        storage_repository: CachedRepository,
        config: MonitoringConfiguration
    ):
        """
        Initialize real-time scraping service
        
        Args:
            scraping_repository: Repository for scraping operations
            storage_repository: Repository for storage (cache + database)
            config: Monitoring configuration
        """
        self.scraper = scraping_repository
        self.storage = storage_repository
        self.config = config
        self.running = False
        self.start_time: Optional[datetime] = None
        self.cycle_count = 0
        
        # Change detection service
        self.change_detector = DataChangeDetectionService()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Real-time scraping service initialized (interval: {config.interval_minutes} minutes)")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
    
    def start(self):
        """Start the monitoring service"""
        if self.running:
            logger.warning("Service is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        logger.info("=" * 70)
        logger.info("Real-Time Scraping Service Started")
        logger.info(f"Interval: {self.config.interval_minutes} minutes")
        logger.info(f"Cache TTL: {self.config.cache_ttl_seconds} seconds")
        logger.info(f"Change Detection: {'Enabled' if self.config.enable_change_detection else 'Disabled'}")
        logger.info("=" * 70)
        
        try:
            while self.running:
                cycle_start = time.time()
                self.cycle_count += 1
                
                logger.info(f"\n{'=' * 70}")
                logger.info(f"Starting monitoring cycle #{self.cycle_count}")
                logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'=' * 70}\n")
                
                # Execute one monitoring cycle
                results = self._monitoring_cycle()
                
                # Log cycle results
                cycle_duration = time.time() - cycle_start
                self._log_cycle_results(results, cycle_duration)
                
                # Update monitoring status
                self._update_monitoring_status(results, cycle_duration)
                
                # Calculate sleep time
                sleep_time = max(0, (self.config.interval_minutes * 60) - cycle_duration)
                
                if self.running and sleep_time > 0:
                    logger.info(f"\nCycle completed. Next cycle in {sleep_time / 60:.1f} minutes...")
                    logger.info("-" * 70)
                    
                    # Sleep in small chunks to allow for graceful shutdown
                    elapsed = 0
                    while self.running and elapsed < sleep_time:
                        time.sleep(min(5, sleep_time - elapsed))
                        elapsed += 5
                
        except KeyboardInterrupt:
            logger.info("\nReceived keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error in monitoring loop: {e}", exc_info=True)
        finally:
            self.stop()
    
    def stop(self):
        """Stop the monitoring service"""
        if not self.running:
            return
        
        logger.info("\n" + "=" * 70)
        logger.info("Stopping Real-Time Scraping Service...")
        logger.info("=" * 70)
        
        self.running = False
        
        if self.start_time:
            uptime = datetime.now() - self.start_time
            logger.info(f"Total uptime: {uptime}")
            logger.info(f"Total cycles: {self.cycle_count}")
        
        logger.info("Service stopped gracefully")
    
    def _monitoring_cycle(self) -> Dict[str, Any]:
        """Execute one complete monitoring cycle"""
        results = {
            "processed": 0,
            "cached_hits": 0,
            "scraped": 0,
            "changes_detected": 0,
            "errors": 0,
            "skipped": 0,
            "details": []
        }
        
        # Get list of monitored codes
        monitored_codes = self._get_monitored_codes()
        
        if not monitored_codes:
            logger.warning("No tariff codes to monitor")
            return results
        
        logger.info(f"Monitoring {len(monitored_codes)} tariff code(s)")
        
        # Process each code
        for tariff_code in monitored_codes:
            if not self.running:
                break
            
            try:
                result = self._process_tariff_code(tariff_code)
                results["processed"] += 1
                
                if result["from_cache"]:
                    results["cached_hits"] += 1
                elif result["scraped"]:
                    results["scraped"] += 1
                
                if result["changed"]:
                    results["changes_detected"] += 1
                
                if result["skipped"]:
                    results["skipped"] += 1
                
                if result["error"]:
                    results["errors"] += 1
                
                results["details"].append(result)
                
            except Exception as e:
                logger.error(f"Error processing {tariff_code}: {e}", exc_info=True)
                results["errors"] += 1
                results["processed"] += 1
        
        return results
    
    def _process_tariff_code(self, tariff_code: str) -> Dict[str, Any]:
        """Process a single tariff code"""
        result = {
            "tariff_code": tariff_code,
            "from_cache": False,
            "scraped": False,
            "changed": False,
            "skipped": False,
            "error": None,
            "duration_ms": 0,
            "message": ""
        }
        
        start_time = time.time()
        
        try:
            # Check cache first
            cached_data = self.storage.cache.get(tariff_code)
            cached_metadata = self.storage.cache.get_metadata(tariff_code)
            
            # Decide if we should skip scraping
            if cached_data and cached_metadata:
                # Check if cache is still valid
                cache_age = self._get_cache_age(cached_metadata)
                if cache_age < self.config.cache_ttl_seconds:
                    result["from_cache"] = True
                    result["skipped"] = True
                    result["message"] = f"Cache hit (age: {cache_age}s)"
                    logger.info(f"  [{tariff_code}] {result['message']}")
                    self.storage.log_monitoring_activity(
                        tariff_code, "cached_hit", "success",
                        int((time.time() - start_time) * 1000),
                        result["message"]
                    )
                    return result
            
            # Scrape fresh data
            logger.info(f"  [{tariff_code}] Scraping...")
            scraped_data = self.scraper.scrape(TariffCode(tariff_code))
            
            if not scraped_data:
                result["error"] = "Scraping returned no data"
                result["message"] = result["error"]
                logger.warning(f"  [{tariff_code}] {result['error']}")
                self.storage.log_monitoring_activity(
                    tariff_code, "scraped", "failed",
                    int((time.time() - start_time) * 1000),
                    result["error"]
                )
                return result
            
            result["scraped"] = True
            
            # Load previous version from database
            previous_data = self.storage.db.load_latest(tariff_code)
            
            # Check for changes
            if self.config.enable_change_detection and previous_data:
                change_info = self.change_detector.detect_changes(previous_data, scraped_data)
                
                if change_info["has_changes"]:
                    result["changed"] = True
                    result["message"] = f"Changes detected: {change_info['summary']}"
                    
                    # Save new version (with deduplication)
                    if self.config.save_only_on_change or True:  # Always save for now
                        saved = self.storage.save(scraped_data)
                        if not saved:
                            # Duplicate detected
                            result["skipped"] = True
                            result["changed"] = False
                            result["message"] = "Duplicate data detected (hash match) - no changes"
                            logger.info(f"  [{tariff_code}] {result['message']}")
                        else:
                            # Log changes
                            self.storage.log_change(
                                tariff_code=tariff_code,
                                change_type="updated" if previous_data else "created",
                                old_data=previous_data,
                                new_data=scraped_data,
                                changes_summary=change_info
                            )
                            logger.info(f"  [{tariff_code}] ✓ {result['message']}")
                    
                else:
                    result["message"] = "No changes detected"
                    # Still save if not save_only_on_change (with deduplication)
                    if not self.config.save_only_on_change:
                        saved = self.storage.save(scraped_data)
                        if not saved:
                            result["skipped"] = True
                            result["message"] = "Duplicate data detected (hash match)"
                            logger.info(f"  [{tariff_code}] {result['message']}")
                        else:
                            logger.info(f"  [{tariff_code}] {result['message']}")
                    else:
                        result["skipped"] = True
                        logger.info(f"  [{tariff_code}] {result['message']} (not saved - save_only_on_change enabled)")
            else:
                # First time scraping this code or change detection disabled
                saved = self.storage.save(scraped_data)
                if not saved:
                    result["skipped"] = True
                    result["changed"] = False
                    result["message"] = "Duplicate data detected (hash match)"
                    logger.info(f"  [{tariff_code}] {result['message']}")
                else:
                    result["changed"] = True
                    result["message"] = "New record created" if not previous_data else "Saved (change detection disabled)"
                    logger.info(f"  [{tariff_code}] ✓ {result['message']}")
            
            # Log activity
            self.storage.log_monitoring_activity(
                tariff_code, "scraped",
                "success" if scraped_data else "failed",
                int((time.time() - start_time) * 1000),
                result["message"]
            )
            
        except Exception as e:
            result["error"] = str(e)
            result["message"] = f"Error: {str(e)}"
            logger.error(f"  [{tariff_code}] {result['error']}", exc_info=True)
            self.storage.log_monitoring_activity(
                tariff_code, "error", "failed",
                int((time.time() - start_time) * 1000),
                result["error"]
            )
        
        result["duration_ms"] = int((time.time() - start_time) * 1000)
        return result
    
    def _get_monitored_codes(self) -> List[str]:
        """Get list of tariff codes to monitor"""
        try:
            codes = self.storage.load_tariff_codes("")
            return codes
        except Exception as e:
            logger.error(f"Error loading monitored codes: {e}")
            return []
    
    def _get_cache_age(self, metadata: Dict[str, Any]) -> float:
        """Get age of cached data in seconds"""
        try:
            scraped_at_str = metadata.get("scraped_at")
            if scraped_at_str:
                scraped_at = datetime.fromisoformat(scraped_at_str.replace('Z', '+00:00'))
                age = (datetime.now() - scraped_at.replace(tzinfo=None)).total_seconds()
                return max(0, age)
        except Exception:
            pass
        return float('inf')  # Invalid metadata = cache expired
    
    def _log_cycle_results(self, results: Dict[str, Any], duration: float):
        """Log cycle results"""
        logger.info("\n" + "=" * 70)
        logger.info("Cycle Results Summary")
        logger.info("=" * 70)
        logger.info(f"Processed: {results['processed']}")
        logger.info(f"Cache Hits: {results['cached_hits']}")
        logger.info(f"Scraped: {results['scraped']}")
        logger.info(f"Changes Detected: {results['changes_detected']}")
        logger.info(f"Skipped: {results['skipped']}")
        logger.info(f"Errors: {results['errors']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        
        if results['cached_hits'] > 0:
            cache_hit_rate = (results['cached_hits'] / results['processed']) * 100
            logger.info(f"Cache Hit Rate: {cache_hit_rate:.1f}%")
    
    def _update_monitoring_status(self, results: Dict[str, Any], duration: float):
        """Update monitoring status in Redis"""
        try:
            status = {
                "running": self.running,
                "last_cycle": datetime.now().isoformat(),
                "cycle_count": self.cycle_count,
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
                "last_cycle_duration_seconds": duration,
                "last_cycle_results": {
                    "processed": results["processed"],
                    "cached_hits": results["cached_hits"],
                    "scraped": results["scraped"],
                    "changes_detected": results["changes_detected"],
                    "errors": results["errors"]
                }
            }
            self.storage.cache.set_monitoring_status(status)
        except Exception as e:
            logger.error(f"Error updating monitoring status: {e}")


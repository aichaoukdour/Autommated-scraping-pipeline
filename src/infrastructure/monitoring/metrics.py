"""
Prometheus metrics for monitoring
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import logging

logger = logging.getLogger(__name__)

# Scraping metrics
scraping_requests_total = Counter(
    'scraping_requests_total',
    'Total number of scraping requests',
    ['status']  # success, failed, skipped
)

scraping_duration_seconds = Histogram(
    'scraping_duration_seconds',
    'Time spent scraping a tariff code',
    buckets=[1, 5, 10, 30, 60, 120, 300]
)

# Cache metrics
cache_hits_total = Counter(
    'cache_hits_total',
    'Total number of cache hits'
)

cache_misses_total = Counter(
    'cache_misses_total',
    'Total number of cache misses'
)

# Database metrics
database_operations_total = Counter(
    'database_operations_total',
    'Total number of database operations',
    ['operation']  # save, load, delete
)

# Task queue metrics
celery_tasks_total = Counter(
    'celery_tasks_total',
    'Total number of Celery tasks',
    ['task_name', 'status']  # scrape_tariff_code, success/failed
)

celery_task_duration_seconds = Histogram(
    'celery_task_duration_seconds',
    'Time spent processing Celery tasks',
    ['task_name']
)

# Active monitoring
monitored_codes_gauge = Gauge(
    'monitored_codes_count',
    'Number of tariff codes being monitored'
)

# Data metrics
total_tariff_codes_gauge = Gauge(
    'total_tariff_codes',
    'Total number of unique tariff codes in database'
)

total_scraped_records_gauge = Gauge(
    'total_scraped_records',
    'Total number of scraped records'
)


def start_metrics_server(port: int = 8000):
    """Start Prometheus metrics HTTP server"""
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}", exc_info=True)


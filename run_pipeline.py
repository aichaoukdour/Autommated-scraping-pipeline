"""
Centralized Pipeline Runner - Runs the complete data pipeline
"""

import os
import sys
import subprocess
import time
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PipelineRunner:
    """Centralized pipeline runner"""
    
    def __init__(self):
        """Initialize pipeline runner"""
        self.setup_environment()
        self.ensure_logs_directory()
    
    def setup_environment(self):
        """Set up environment variables"""
        env_vars = {
            'DATABASE_URL': os.getenv('DATABASE_URL', 'postgresql://aicha:aicha1234@127.0.0.1:5433/adil_scraper'),
            'REDIS_URL': os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0'),
            'MONITORING_INTERVAL_MINUTES': os.getenv('MONITORING_INTERVAL_MINUTES', '5'),
            'CACHE_TTL_SECONDS': os.getenv('CACHE_TTL_SECONDS', '300'),
            'PLAYWRIGHT_HEADLESS': os.getenv('PLAYWRIGHT_HEADLESS', 'false'),
            'LOG_LEVEL': os.getenv('LOG_LEVEL', 'INFO')
        }
        
        for key, value in env_vars.items():
            os.environ[key] = value
        
        logger.info("Environment variables set")
    
    def ensure_logs_directory(self):
        """Ensure logs directory exists"""
        Path('logs').mkdir(exist_ok=True)
    
    def check_docker_installed(self) -> bool:
        """Check if Docker is installed and running"""
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                logger.info(f"Docker found: {result.stdout.strip()}")
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        logger.error("Docker not found or not running")
        return False
    
    def start_docker_services(self) -> bool:
        """Start Docker services (PostgreSQL and Redis)"""
        logger.info("Starting Docker services...")
        
        try:
            # Check if services are already running
            result = subprocess.run(['docker-compose', 'ps'], 
                                  capture_output=True, text=True, timeout=10)
            
            # Start services
            result = subprocess.run(['docker-compose', 'up', '-d', 'postgres', 'redis'],
                                  capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                logger.info("Docker services started successfully")
                # Wait for services to be ready
                self.wait_for_services()
                return True
            else:
                logger.error(f"Failed to start Docker services: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error starting Docker services: {e}")
            return False
    
    def wait_for_services(self, max_wait=30):
        """Wait for Docker services to be ready"""
        logger.info("Waiting for services to be ready...")
        
        import psycopg2
        import redis
        
        for i in range(max_wait):
            try:
                # Test PostgreSQL
                conn = psycopg2.connect(os.environ['DATABASE_URL'])
                conn.close()
                
                # Test Redis
                r = redis.from_url(os.environ['REDIS_URL'])
                r.ping()
                
                logger.info("Services are ready!")
                return True
                
            except Exception:
                if i < max_wait - 1:
                    time.sleep(1)
                else:
                    logger.warning("Services may not be fully ready, continuing anyway...")
                    return False
        
        return False
    
    def check_database_schema(self) -> bool:
        """Check if database schema exists"""
        try:
            import psycopg2
            conn = psycopg2.connect(os.environ['DATABASE_URL'])
            cur = conn.cursor()
            
            # Check if tables exist
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('scraped_data', 'cleaned_data', 'monitoring_log')
            """)
            
            tables = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            
            if len(tables) >= 3:
                logger.info(f"Database schema OK: {len(tables)} tables found")
                return True
            else:
                logger.warning(f"Database schema incomplete: {len(tables)} tables found")
                return False
                
        except Exception as e:
            logger.error(f"Error checking database schema: {e}")
            return False
    
    def add_tariff_code(self, code: str, interval: int = 5) -> bool:
        """Add a tariff code to monitoring"""
        try:
            from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository
            db_repo = PostgreSQLRepository()
            result = db_repo.add_monitored_code(
                tariff_code=code,
                interval_minutes=interval,
                priority=0
            )
            if result:
                logger.info(f"Added tariff code {code} to monitoring (interval: {interval} min)")
            return result
        except Exception as e:
            logger.error(f"Error adding tariff code: {e}")
            return False
    
    def run_etl(self, codes: list = None) -> int:
        """Run ETL pipeline"""
        logger.info("Running ETL pipeline...")
        
        try:
            from src.application.etl_pipeline import ETLPipeline
            
            pipeline = ETLPipeline()
            success_count = pipeline.transform_all(codes)
            
            logger.info(f"ETL completed: {success_count} codes transformed")
            return success_count
            
        except Exception as e:
            logger.error(f"Error running ETL: {e}")
            return 0
    
    def start_monitoring(self, background: bool = False):
        """Start monitoring service"""
        logger.info("Starting monitoring service...")
        
        if background:
            # Run in background
            if sys.platform == 'win32':
                subprocess.Popen([sys.executable, '-m', 'src.monitoring.service'],
                              creationflags=subprocess.CREATE_NEW_CONSOLE)
            else:
                subprocess.Popen([sys.executable, '-m', 'src.monitoring.service'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logger.info("Monitoring service started in background")
        else:
            # Run in foreground
            logger.info("Starting monitoring service (press Ctrl+C to stop)...")
            try:
                from src.monitoring.service import main
                main()
            except KeyboardInterrupt:
                logger.info("Monitoring service stopped by user")
            except Exception as e:
                logger.error(f"Error in monitoring service: {e}")
    
    def show_status(self):
        """Show pipeline status"""
        logger.info("=" * 70)
        logger.info("Pipeline Status")
        logger.info("=" * 70)
        
        # Check Docker services
        try:
            result = subprocess.run(['docker-compose', 'ps'], 
                                  capture_output=True, text=True, timeout=10)
            logger.info("\nDocker Services:")
            logger.info(result.stdout)
        except:
            logger.warning("Could not check Docker services")
        
        # Check database
        try:
            import psycopg2
            conn = psycopg2.connect(os.environ['DATABASE_URL'])
            cur = conn.cursor()
            
            cur.execute("SELECT COUNT(*) FROM scraped_data")
            raw_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM cleaned_data")
            cleaned_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM monitored_codes WHERE enabled = true")
            monitored_count = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            logger.info(f"\nDatabase Status:")
            logger.info(f"  Raw data records: {raw_count}")
            logger.info(f"  Cleaned data records: {cleaned_count}")
            logger.info(f"  Monitored codes: {monitored_count}")
            
        except Exception as e:
            logger.warning(f"Could not check database: {e}")
        
        logger.info("=" * 70)


def main():
    """Main pipeline runner"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run the complete data pipeline')
    parser.add_argument('--mode', choices=['full', 'start', 'monitor', 'etl', 'status'],
                       default='full', help='Pipeline mode')
    parser.add_argument('--code', help='Tariff code to add/monitor')
    parser.add_argument('--interval', type=int, default=5, help='Monitoring interval (minutes)')
    parser.add_argument('--background', action='store_true', help='Run monitoring in background')
    parser.add_argument('--skip-docker', action='store_true', help='Skip Docker service startup')
    parser.add_argument('--skip-etl', action='store_true', help='Skip ETL transformation')
    
    args = parser.parse_args()
    
    runner = PipelineRunner()
    
    if args.mode == 'status':
        runner.show_status()
        return
    
    # Check Docker
    if not args.skip_docker and args.mode in ['full', 'start', 'monitor']:
        if not runner.check_docker_installed():
            logger.error("Docker is required but not found. Install Docker or use --skip-docker")
            sys.exit(1)
        
        if not runner.start_docker_services():
            logger.error("Failed to start Docker services")
            sys.exit(1)
        
        runner.check_database_schema()
    
    # Add tariff code if provided
    if args.code:
        runner.add_tariff_code(args.code, args.interval)
    
    # Run ETL
    if not args.skip_etl and args.mode in ['full', 'etl']:
        codes = [args.code] if args.code else None
        runner.run_etl(codes)
    
    # Start monitoring
    if args.mode in ['full', 'start', 'monitor']:
        runner.start_monitoring(background=args.background)
    
    logger.info("Pipeline execution completed")


if __name__ == "__main__":
    main()


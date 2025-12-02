"""
CLI for managing real-time monitoring service
"""

import argparse
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.infrastructure.storage.postgresql_repository import PostgreSQLRepository
from src.infrastructure.storage.redis_cache_repository import RedisCacheRepository


def add_code_command(args):
    """Add a tariff code to monitoring"""
    db_repo = PostgreSQLRepository()
    
    success = db_repo.add_monitored_code(
        tariff_code=args.code,
        interval_minutes=args.interval,
        priority=args.priority
    )
    
    if success:
        print(f"[OK] Added {args.code} to monitoring")
        print(f"  Interval: {args.interval} minutes")
        print(f"  Priority: {args.priority}")
    else:
        print(f"[ERROR] Failed to add {args.code}")
        sys.exit(1)


def list_codes_command(args):
    """List monitored tariff codes"""
    db_repo = PostgreSQLRepository()
    
    try:
        codes = db_repo.load_tariff_codes("")
        if codes:
            print(f"\nMonitored Tariff Codes ({len(codes)}):")
            print("-" * 50)
            for i, code in enumerate(codes, 1):
                print(f"  {i}. {code}")
        else:
            print("No tariff codes being monitored")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def status_command(args):
    """Check monitoring service status"""
    cache_repo = RedisCacheRepository()
    
    status = cache_repo.get_monitoring_status()
    
    if status:
        print("\nMonitoring Service Status:")
        print("-" * 50)
        print(f"Running: {status.get('running', 'Unknown')}")
        print(f"Last Cycle: {status.get('last_cycle', 'Never')}")
        print(f"Total Cycles: {status.get('cycle_count', 0)}")
        
        if 'last_cycle_results' in status:
            results = status['last_cycle_results']
            print(f"\nLast Cycle Results:")
            print(f"  Processed: {results.get('processed', 0)}")
            print(f"  Cache Hits: {results.get('cached_hits', 0)}")
            print(f"  Scraped: {results.get('scraped', 0)}")
            print(f"  Changes: {results.get('changes_detected', 0)}")
            print(f"  Errors: {results.get('errors', 0)}")
    else:
        print("Service status not available (service may not be running)")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='ADiL Real-Time Monitoring CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Add code command
    add_parser = subparsers.add_parser('add', help='Add tariff code to monitoring')
    add_parser.add_argument('code', help='Tariff code to monitor')
    add_parser.add_argument('--interval', type=int, default=60, help='Check interval in minutes (default: 60)')
    add_parser.add_argument('--priority', type=int, default=0, help='Priority (higher = checked first, default: 0)')
    add_parser.set_defaults(func=add_code_command)
    
    # List codes command
    list_parser = subparsers.add_parser('list', help='List monitored tariff codes')
    list_parser.set_defaults(func=list_codes_command)
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Check monitoring service status')
    status_parser.set_defaults(func=status_command)
    
    args = parser.parse_args()
    
    if args.command:
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()


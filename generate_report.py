import psycopg2
from src.scraper.config import ScraperConfig, logger
import sys

def generate_health_report():
    config = ScraperConfig()
    try:
        conn = psycopg2.connect(config.db_dsn)
        cur = conn.cursor()
        
        # 1. Overall Stats
        cur.execute("SELECT count(*), count(CASE WHEN status = 'SUCCESS' THEN 1 END) FROM audit_logs")
        total, success = cur.fetchone()
        
        if total == 0:
            print("\n📊 NO AUDIT DATA FOUND.")
            return

        success_rate = (success / total) * 100
        
        # 2. Performance Stats
        cur.execute("SELECT AVG(duration_ms) FROM audit_logs WHERE status = 'SUCCESS'")
        avg_time = cur.fetchone()[0] or 0
        
        # 3. Error Breakdown
        cur.execute("""
            SELECT status, count(*), SUBSTRING(message, 1, 100) as msg 
            FROM audit_logs 
            WHERE status != 'SUCCESS' 
            GROUP BY status, msg 
            ORDER BY count(*) DESC 
            LIMIT 5
        """)
        errors = cur.fetchall()

        print("\n" + "="*40)
        print("🚀 PIPELINE HEALTH REPORT")
        print("="*40)
        print(f"Total Processed : {total}")
        print(f"Success Rate    : {success_rate:.2f}% ({success}/{total})")
        print(f"Avg Speed       : {avg_time/1000:.2f} seconds per code")
        print(f"Estimated 13k   : {(avg_time/1000 * 13000)/3600:.1f} hours total")
        
        if errors:
            print("\n❌ TOP ERRORS:")
            for status, count, msg in errors:
                print(f"- [{status}] {count} times: {msg}...")
        
        print("="*40 + "\n")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to generate report: {e}")

if __name__ == "__main__":
    generate_health_report()

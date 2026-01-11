import psycopg2
from psycopg2.extras import DictCursor

DSN = "dbname=hs user=postgres password=postgres host=localhost port=5433"

def show_health_dashboard():
    print("\n" + "="*50)
    print("      🏥 PIPELINE HEALTH DASHBOARD")
    print("="*50)
    
    try:
        conn = psycopg2.connect(DSN)
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 1. Overall Stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'SUCCESS') as success,
                    COUNT(*) FILTER (WHERE status = 'FAILED') as failed,
                    COUNT(*) FILTER (WHERE status = 'VALIDATION_ERROR') as val_error,
                    AVG(duration_ms)::INTEGER as avg_duration
                FROM audit_logs
            """)
            stats = cur.fetchone()
            
            if stats['total'] == 0:
                print("⚠️ No audit logs found. Run the pipeline first!")
                return

            print(f"📊 Total Processed: {stats['total']}")
            print(f"✅ Successes:      {stats['success']}")
            print(f"❌ Failures:       {stats['failed']}")
            print(f"🔍 Validation Errs: {stats['val_error']}")
            print(f"⏱️ Avg Duration:    {stats['avg_duration']}ms")
            print("-" * 50)

            # 2. Recent Failures
            cur.execute("""
                SELECT hs10, status, message, timestamp 
                FROM audit_logs 
                WHERE status != 'SUCCESS' 
                ORDER BY timestamp DESC 
                LIMIT 5
            """)
            failures = cur.fetchall()
            
            if failures:
                print("\n🚨 RECENT FAILURES/ERRORS:")
                for f in failures:
                    print(f"[{f['timestamp'].strftime('%H:%M:%S')}] {f['hs10']} - {f['status']}")
                    print(f"   Error: {f['message'][:100]}...")
            else:
                print("\n🌈 No recent failures. System is healthy!")
            
    except Exception as e:
        print(f"❌ Error querying health dashboard: {e}")
    finally:
        if conn:
            conn.close()
    
    print("="*50 + "\n")

if __name__ == "__main__":
    show_health_dashboard()

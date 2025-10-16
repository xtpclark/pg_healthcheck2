#!/usr/bin/env python3
"""
Cleanup script for report versions using existing schema.
Keeps:
- All pinned versions
- Last 10 versions per report
- Versions newer than 30 days
"""
import psycopg2
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import load_trends_config

def cleanup_old_versions():
    """Clean up old report versions according to retention policy."""
    config = load_trends_config()
    db_settings = config.get('database')
    
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        
        # Delete old unpinned versions for generated reports
        cursor.execute("""
            DELETE FROM report_versions
            WHERE auto_cleanup = TRUE 
              AND is_pinned = FALSE
              AND version_timestamp < NOW() - INTERVAL '30 days'
              AND generated_report_id IS NOT NULL
              AND id NOT IN (
                  SELECT id FROM (
                      SELECT id, ROW_NUMBER() OVER (
                          PARTITION BY generated_report_id 
                          ORDER BY version_number DESC
                      ) as rn
                      FROM report_versions
                      WHERE generated_report_id IS NOT NULL
                  ) sub WHERE rn <= 10
              );
        """)
        
        deleted_generated = cursor.rowcount
        
        # Delete old unpinned versions for uploaded reports
        cursor.execute("""
            DELETE FROM report_versions
            WHERE auto_cleanup = TRUE 
              AND is_pinned = FALSE
              AND version_timestamp < NOW() - INTERVAL '30 days'
              AND uploaded_report_id IS NOT NULL
              AND id NOT IN (
                  SELECT id FROM (
                      SELECT id, ROW_NUMBER() OVER (
                          PARTITION BY uploaded_report_id 
                          ORDER BY version_number DESC
                      ) as rn
                      FROM report_versions
                      WHERE uploaded_report_id IS NOT NULL
                  ) sub WHERE rn <= 10
              );
        """)
        
        deleted_uploaded = cursor.rowcount
        deleted_count = deleted_generated + deleted_uploaded
        
        conn.commit()
        
        # Log results
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Cleanup completed:")
        print(f"  - Generated reports: {deleted_generated} versions deleted")
        print(f"  - Uploaded reports: {deleted_uploaded} versions deleted")
        print(f"  - Total: {deleted_count} versions deleted")
        
        # Get statistics
        cursor.execute("SELECT COUNT(*) FROM report_versions;")
        total_versions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM report_versions WHERE is_pinned = TRUE;")
        pinned_versions = cursor.fetchone()[0]
        
        print(f"[{timestamp}] Statistics:")
        print(f"  - Total versions remaining: {total_versions}")
        print(f"  - Pinned versions: {pinned_versions}")
        
        return deleted_count
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] Database error during cleanup: {e}", file=sys.stderr)
        return -1
    except Exception as e:
        print(f"[ERROR] Unexpected error during cleanup: {e}", file=sys.stderr)
        return -1
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    result = cleanup_old_versions()
    sys.exit(0 if result >= 0 else 1)

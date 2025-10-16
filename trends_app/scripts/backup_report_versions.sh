#!/bin/bash
# Backup report_versions table before cleanup

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/path/to/backups"
BACKUP_FILE="$BACKUP_DIR/report_versions_$TIMESTAMP.sql"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Backup the table
pg_dump -h localhost -U your_db_user -d your_db_name -t report_versions > "$BACKUP_FILE"

# Compress the backup
gzip "$BACKUP_FILE"

# Keep only last 30 days of backups
find "$BACKUP_DIR" -name "report_versions_*.sql.gz" -mtime +30 -delete

echo "Backup created: ${BACKUP_FILE}.gz"

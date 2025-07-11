#!/bin/bash
# Setup Cron Jobs for PostgreSQL Health Check Trend Collection
# This script helps set up automated trend data collection

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="config/config.yaml"
TREND_CONFIG_FILE="config/trend_config.yaml"
FREQUENCY="daily"
USER=$(whoami)
ENVIRONMENT=""

# Help function
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -c, --config FILE     Main configuration file (default: config/config.yaml)"
    echo "  -t, --trend-config FILE Trend configuration file (default: config/trend_config.yaml)"
    echo "  -e, --environment ENV Environment: development, staging, production"
    echo "  -f, --frequency TYPE  Collection frequency: hourly, daily, weekly (default: daily)"
    echo "  -u, --user USER       User to run cron job (default: current user)"
    echo "  -d, --dry-run         Show what would be added to crontab without adding it"
    echo "  -h, --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Daily collection at 2 AM"
    echo "  $0 -f hourly                          # Hourly collection"
    echo "  $0 -f weekly                          # Weekly collection on Sunday at 2 AM"
    echo "  $0 -e production                      # Use production environment"
    echo "  $0 -c /path/to/config.yaml           # Use custom config file"
    echo "  $0 -t /path/to/trend_config.yaml     # Use custom trend config"
    echo "  $0 -d                                 # Show cron entry without adding it"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -t|--trend-config)
            TREND_CONFIG_FILE="$2"
            shift 2
            ;;
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -f|--frequency)
            FREQUENCY="$2"
            shift 2
            ;;
        -u|--user)
            USER="$2"
            shift 2
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Validate frequency
case $FREQUENCY in
    hourly|daily|weekly)
        ;;
    *)
        echo -e "${RED}Error: Invalid frequency '$FREQUENCY'. Use: hourly, daily, weekly${NC}"
        exit 1
        ;;
esac

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo -e "${RED}Error: Configuration file '$CONFIG_FILE' not found${NC}"
    exit 1
fi

# Check if trend_collector.py exists
if [[ ! -f "$SCRIPT_DIR/trend_collector.py" ]]; then
    echo -e "${RED}Error: trend_collector.py not found in $SCRIPT_DIR${NC}"
    exit 1
fi

# Check if trend config file exists
if [[ ! -f "$TREND_CONFIG_FILE" ]]; then
    echo -e "${YELLOW}Warning: Trend config file '$TREND_CONFIG_FILE' not found${NC}"
    echo "You may want to create this file for better trend collection configuration"
fi

# Generate cron schedule based on frequency
case $FREQUENCY in
    hourly)
        CRON_SCHEDULE="0 * * * *"
        DESCRIPTION="hourly"
        ;;
    daily)
        CRON_SCHEDULE="0 2 * * *"
        DESCRIPTION="daily at 2 AM"
        ;;
    weekly)
        CRON_SCHEDULE="0 2 * * 0"
        DESCRIPTION="weekly on Sunday at 2 AM"
        ;;
esac

# Create log directory
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# Create the cron entry
if [[ -n "$ENVIRONMENT" ]]; then
    CRON_ENTRY="$CRON_SCHEDULE cd $SCRIPT_DIR && python3 trend_collector.py $CONFIG_FILE --trend-config $TREND_CONFIG_FILE --environment $ENVIRONMENT >> $LOG_DIR/trend_collection.log 2>&1"
else
    CRON_ENTRY="$CRON_SCHEDULE cd $SCRIPT_DIR && python3 trend_collector.py $CONFIG_FILE --trend-config $TREND_CONFIG_FILE >> $LOG_DIR/trend_collection.log 2>&1"
fi

# Add comment for identification
CRON_COMMENT="# PostgreSQL Health Check Trend Collection ($DESCRIPTION)"

echo -e "${BLUE}PostgreSQL Health Check Trend Collection Setup${NC}"
echo "=================================================="
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Script Directory: $SCRIPT_DIR"
echo "  Config File: $CONFIG_FILE"
echo "  Trend Config File: $TREND_CONFIG_FILE"
if [[ -n "$ENVIRONMENT" ]]; then
    echo "  Environment: $ENVIRONMENT"
fi
echo "  Frequency: $FREQUENCY ($DESCRIPTION)"
echo "  User: $USER"
echo "  Log File: $LOG_DIR/trend_collection.log"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    echo -e "${YELLOW}DRY RUN - Cron entry that would be added:${NC}"
    echo ""
    echo "$CRON_COMMENT"
    echo "$CRON_ENTRY"
    echo ""
    echo -e "${GREEN}To add this cron job, run without --dry-run${NC}"
    exit 0
fi

# Check if cron entry already exists
if crontab -l 2>/dev/null | grep -q "trend_collector.py"; then
    echo -e "${YELLOW}Warning: Existing trend collection cron job found${NC}"
    echo ""
    echo "Current cron entries:"
    crontab -l 2>/dev/null | grep -A1 -B1 "trend_collector.py" || true
    echo ""
    read -p "Do you want to replace existing entries? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Cron setup cancelled${NC}"
        exit 0
    fi
fi

# Add cron entry
echo -e "${BLUE}Adding cron job...${NC}"

# Get current crontab
CURRENT_CRONTAB=$(crontab -l 2>/dev/null || echo "")

# Remove existing trend collection entries
CLEAN_CRONTAB=$(echo "$CURRENT_CRONTAB" | grep -v "trend_collector.py" | grep -v "PostgreSQL Health Check Trend Collection")

# Add new entry
NEW_CRONTAB="$CLEAN_CRONTAB

$CRON_COMMENT
$CRON_ENTRY"

# Install new crontab
echo "$NEW_CRONTAB" | crontab -

echo -e "${GREEN}✓ Cron job added successfully!${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Verify trend storage is enabled in $CONFIG_FILE"
echo "2. Test the collection manually: python3 trend_collector.py $CONFIG_FILE"
echo "3. Monitor logs: tail -f $LOG_DIR/trend_collection.log"
echo "4. View cron jobs: crontab -l"
echo ""

# Show current cron entries
echo -e "${YELLOW}Current cron entries:${NC}"
crontab -l | grep -A1 -B1 "trend_collector.py" || echo "No trend collection jobs found"

echo ""
echo -e "${GREEN}Setup complete! Trend data will be collected $DESCRIPTION.${NC}" 
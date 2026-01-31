# Server Deployment Guide

This guide explains how to deploy the property scraping system to run on a server with database storage and scheduled execution.

## Overview

The system has been updated to use a proper database (SQLite) instead of CSV files, while maintaining Google Sheets integration for visualization with your conditional formatting.

**Architecture:**
- **SQLite Database**: Stores all property listings, jobs, and rental data
- **Scheduled Scraper**: Runs automatically (e.g., daily) via cron
- **Google Sheets Sync**: Automatically syncs new listings to your Google Sheet
- **CSV Files**: Still generated for backward compatibility (optional)

## Files Overview

### New Files
- `db.py` - Database operations and schema
- `run_eiendom_db.py` - Database-enabled version of the scraper
- `sync_to_sheets.py` - Syncs database data to Google Sheets
- `scheduler.py` - Main scheduler script for automated runs

### Modified Workflow
1. Scraper extracts data and stores in database
2. Database tracks active/inactive listings
3. Sync script pushes only new listings to Google Sheets
4. Google Sheets maintains your conditional formatting

## Server Setup

### 1. Prerequisites

```bash
# Python 3.12 or higher
python3 --version

# Git (to clone/pull updates)
git --version
```

### 2. Install on Server

```bash
# Clone the repository (or pull updates)
cd /home/yourusername/
git clone <your-repo-url> skannonser
cd skannonser

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r main/config/requirements.txt
```

### 3. Configure Credentials

```bash
cd main/config

# Add your Google API credentials
# Copy credentials.json to main/config/credentials.json
# Copy token.json to main/config/token.json (or authenticate once)

# Set Google Maps API key in config.py
nano config.py
# Update GOOGLE_MAPS_API_KEY with your key
```

### 4. Initialize Database

```bash
# Create the database (first time only)
python db.py

# This creates properties.db in the project root
# Location: /home/yourusername/skannonser/properties.db
```

### 5. Test the Setup

```bash
# Test scraping with database
python run_eiendom_db.py

# Test syncing to Google Sheets
python sync_to_sheets.py

# Test full workflow
python scheduler.py eiendom
```

## Scheduling with Cron

### Daily Execution

Add to crontab to run daily at 6 AM:

```bash
# Edit crontab
crontab -e

# Add this line (adjust paths to match your setup):
0 6 * * * cd /home/yourusername/skannonser && /home/yourusername/skannonser/.venv/bin/python main/scheduler.py eiendom >> /home/yourusername/skannonser/logs/scraper.log 2>&1
```

### Create Logs Directory

```bash
mkdir -p /home/yourusername/skannonser/logs
```

### Cron Schedule Examples

```bash
# Every day at 6 AM
0 6 * * * /path/to/command

# Every 6 hours
0 */6 * * * /path/to/command

# Every Monday at 8 AM
0 8 * * 1 /path/to/command

# Twice daily (6 AM and 6 PM)
0 6,18 * * * /path/to/command
```

### View Cron Logs

```bash
# View recent logs
tail -f /home/yourusername/skannonser/logs/scraper.log

# View last 100 lines
tail -n 100 /home/yourusername/skannonser/logs/scraper.log
```

## Docker Deployment (Alternative)

If you prefer Docker:

### Create Dockerfile

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY main/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Run scheduler
CMD ["python", "main/scheduler.py", "eiendom"]
```

### Docker Compose

```yaml
version: '3.8'

services:
  scraper:
    build: .
    volumes:
      - ./properties.db:/app/properties.db
      - ./main/credentials.json:/app/main/credentials.json
      - ./main/token.json:/app/main/token.json
    environment:
      - TZ=Europe/Oslo
    restart: unless-stopped
```

### Run with Docker

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f

# Run manually
docker-compose run scraper python main/scheduler.py eiendom
```

## Cloud Deployment Options

### 1. DigitalOcean Droplet

**Cost**: $4-6/month for basic droplet

```bash
# After SSH into droplet:
sudo apt update
sudo apt install python3 python3-pip python3-venv git
# Follow "Server Setup" steps above
```

### 2. AWS EC2

**Cost**: Free tier eligible (t2.micro)

- Launch Ubuntu instance
- Configure security groups
- Follow server setup steps

### 3. Google Cloud (Cloud Run + Cloud Scheduler)

**Cost**: Pay per execution (~$1-5/month)

```bash
# Deploy as Cloud Run service
gcloud run deploy scraper \
  --source . \
  --region us-central1 \
  --no-allow-unauthenticated

# Schedule with Cloud Scheduler
gcloud scheduler jobs create http scraper-daily \
  --schedule="0 6 * * *" \
  --uri="https://scraper-xxx.run.app" \
  --http-method=POST
```

### 4. Heroku

**Cost**: Free for basic dyno (limited hours)

```bash
# Create Procfile
echo "worker: python main/scheduler.py eiendom" > Procfile

# Deploy
heroku create
git push heroku main
heroku ps:scale worker=1
```

## Database Management

### View Database Stats

```bash
python -c "from main.db import PropertyDatabase; db = PropertyDatabase(); print(db.get_stats('eiendom'))"
```

### Backup Database

```bash
# Create backup
cp main/database/properties.db backups/properties_$(date +%Y%m%d).db

# Automated daily backup (add to crontab)
0 2 * * * cp /home/yourusername/skannonser/main/database/properties.db /home/yourusername/skannonser/backups/properties_$(date +\%Y\%m\%d).db
```

### Query Database Directly

```bash
# Open SQLite shell
sqlite3 main/database/properties.db

# Example queries
SELECT COUNT(*) FROM eiendom WHERE is_active = 1;
SELECT * FROM eiendom ORDER BY scraped_at DESC LIMIT 10;
.quit
```

### Export Database to CSV

```python
# export_db.py
from main.db import PropertyDatabase
import pandas as pd

db = PropertyDatabase()
df = db.get_active_listings('eiendom')
df.to_csv('eiendom_export.csv', index=False)
```

## Google Sheets Integration

### How It Works

1. **Incremental Sync** (default): Only new listings are added to Google Sheets
2. **Full Sync** (manual): Replaces entire sheet with database contents

### Sync Commands

```bash
# Sync only new listings (safe, recommended)
python sync_to_sheets.py

# Full sync (overwrites sheet - use with caution)
python sync_to_sheets.py --full

# Sync to different sheet
python sync_to_sheets.py --sheet "MySheet"
```

### Preserve Conditional Formatting

Your conditional formatting in Google Sheets is preserved because:
- New rows are appended (not overwritten)
- Column structure remains the same
- Formulas in Sheets are not affected

**Tip**: Set up conditional formatting rules on the entire column, not just specific rows, so new data automatically gets formatted.

## Monitoring & Maintenance

### Check if Scraper is Running

```bash
# View cron jobs
crontab -l

# Check recent log entries
tail -f logs/scraper.log

# Check database stats
python db.py
```

### Email Notifications on Failure

Add to crontab for email on errors:

```bash
# Set MAILTO at top of crontab
MAILTO=your-email@example.com

0 6 * * * cd /path/to/skannonser && ./venv/bin/python main/scheduler.py eiendom
```

### Monitoring Script

Create `monitor.py`:

```python
#!/usr/bin/env python3
from main.db import PropertyDatabase
from datetime import datetime, timedelta
import sys

db = PropertyDatabase()
stats = db.get_stats('eiendom')

# Check if any listings were updated in last 24 hours
# (indicates scraper is working)

if stats['active'] == 0:
    print("WARNING: No active listings found!")
    sys.exit(1)

print(f"✓ Database healthy: {stats['active']} active listings")
sys.exit(0)
```

## Troubleshooting

### Issue: Cron job not running

```bash
# Check cron service
sudo service cron status

# Check cron logs
grep CRON /var/log/syslog

# Test command manually
cd /path/to/skannonser && .venv/bin/python main/scheduler.py eiendom
```

### Issue: Google Sheets authentication fails

```bash
# Re-authenticate
rm main/config/token.json
python sync_to_sheets.py
# Follow authentication prompts
```

### Issue: Database locked

```bash
# Check for running processes
ps aux | grep python

# If stuck, restart
pkill -f scheduler.py
```

### Issue: Disk space

```bash
# Check disk usage
df -h

# Clean old logs
find logs/ -name "*.log" -mtime +30 -delete

# Clean old database backups
find backups/ -name "*.db" -mtime +90 -delete
```

## Migration from CSV

If you have existing CSV data:

### Import Existing Data

```python
# import_csv.py
from main.db import PropertyDatabase
import pandas as pd

db = PropertyDatabase()

# Import eiendom data
df = pd.read_csv('data/eiendom/AB_processed.csv')
db.insert_or_update_eiendom(df)

print("Import complete!")
```

### Run Migration

```bash
python import_csv.py
```

## Environment Variables (Optional)

For better security, use environment variables:

```bash
# Add to ~/.bashrc or ~/.profile
export GOOGLE_MAPS_API_KEY="your-key-here"
export SPREADSHEET_ID="your-sheet-id"
```

Update `config.py`:

```python
import os
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', 'default-key')
```

## Performance Tips

### Database Optimization

```sql
-- Run occasionally to optimize database
VACUUM;
ANALYZE;
```

### Reduce API Calls

```python
# In config.py - add rate limiting if needed
SCRAPE_DELAY = 0.5  # seconds between requests
```

## Scaling Up

### Multiple Categories

Run different scrapers at different times:

```bash
# Crontab with staggered schedules
0 6 * * * /path/scheduler.py eiendom
0 7 * * * /path/scheduler.py rental  
0 8 * * * /path/scheduler.py jobs
```

### PostgreSQL (for production)

To use PostgreSQL instead of SQLite:

1. Install PostgreSQL
2. Update `db.py` to use `psycopg2`
3. Change connection string

```python
# PostgreSQL example (requires modification of db.py)
import psycopg2
conn = psycopg2.connect(
    dbname="properties",
    user="youruser",
    password="yourpass",
    host="localhost"
)
```

## Support & Updates

### Update Code

```bash
cd /home/yourusername/skannonser
git pull origin main
source .venv/bin/activate
pip install -r main/requirements.txt --upgrade
```

### Rollback

```bash
# If something breaks after update
git log  # Find previous commit
git checkout <commit-hash>
```

## Security Checklist

- [ ] Store credentials.json securely (not in git)
- [ ] Set proper file permissions: `chmod 600 main/credentials.json`
- [ ] Use environment variables for API keys
- [ ] Regular database backups
- [ ] Keep server OS updated: `sudo apt update && sudo apt upgrade`
- [ ] Use SSH keys instead of passwords
- [ ] Consider firewall rules if public-facing

## Cost Estimate

**Minimal Setup (recommended):**
- DigitalOcean Droplet: $6/month
- Google Maps API: Free tier (usually sufficient)
- Google Sheets API: Free
- **Total**: ~$6/month

**Cloud Functions (serverless):**
- Google Cloud Run: ~$1-2/month (pay per execution)
- AWS Lambda: Free tier eligible
- **Total**: ~$0-5/month

## Questions?

If you need help:
1. Check logs: `tail -f logs/scraper.log`
2. Test manually: `python run_eiendom_db.py`
3. Check database: `python db.py`

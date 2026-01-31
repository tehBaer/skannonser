# Quick Start Guide - Database Migration

## What Changed?

Your scraping system now uses a **SQLite database** instead of CSV files, while still maintaining Google Sheets integration.

### Benefits:
✅ **True database** - Better data integrity and querying  
✅ **Server-ready** - Easy to run on schedule (cron jobs)  
✅ **Google Sheets preserved** - Still syncs for your conditional formatting  
✅ **Track history** - Know when listings were added/removed  
✅ **Better performance** - Faster queries and updates  

## Quick Start (5 minutes)

### 1. Migrate Existing Data (Optional)

If you have existing CSV data you want to preserve:

```bash
cd /Users/tehbaer/Kode/skannonser
source .venv/bin/activate
python main/database/migrate_to_db.py

# Or use the CLI tool:
python main/tools/manage.py migrate
```

This creates `properties.db` with your existing data.

### 2. Test the New System

```bash
# Test database operations
python main/database/db.py

# Test scraping with database
python main/runners/run_eiendom_db.py

# Test Google Sheets sync
python main/sync/sync_to_sheets.py

# Or better, use the CLI tool:
python main/tools/manage.py stats
python main/tools/manage.py scrape eiendom
python main/tools/manage.py sync eiendom
```

### 3. Run Full Workflow

```bash
# This does everything: scrape + save to DB + sync to Sheets
python main/tools/scheduler.py eiendom

# Or use the CLI tool (recommended):
python main/tools/manage.py run eiendom
```

## Key Files

### New Files (what they do):

| File | Purpose |
|------|---------|
| `database/db.py` | Database operations - stores all listings |
| `runners/run_eiendom_db.py` | Scraper that saves to database (not CSV) |
| `sync/sync_to_sheets.py` | Pushes new listings from DB to Google Sheets |
| `tools/scheduler.py` | Runs everything on schedule |
| `tools/manage.py` | CLI tool for easy management |
| `database/migrate_to_db.py` | One-time migration from CSV to database |
| `monitoring/monitor_dashboard.py` | Web dashboard |
| `monitoring/test_setup.py` | Verify setup |

### Your Original Files:

Still work! Your old workflow with CSVs is unchanged. The new database system runs in parallel.

## Daily Usage

### Option 1: Manual Run (when you want)

```bash
cd /Users/tehbaer/Kode/skannonser
source .venv/bin/activate
python main/tools/manage.py run eiendom
```

### Option 2: Scheduled Run (server/cron)

See [README_DEPLOYMENT.md](README_DEPLOYMENT.md) for full server setup.

**Quick cron setup:**
```bash
crontab -e
# Add this line to run daily at 6 AM:
0 6 * * * cd /Users/tehbaer/Kode/skannonser && .venv/bin/python main/tools/manage.py run eiendom >> logs/scraper.log 2>&1
```

## Google Sheets Integration

### How it works:

1. **Database stores everything** - All listings go into `properties.db`
2. **Sync only NEW listings** - Only items not in Sheets get added
3. **Your formatting preserved** - Existing rows and formatting untouched

### Sync Commands:

```bash
# Sync new listings only (safe, recommended)
python main/sync/sync_to_sheets.py

# Or use the CLI tool:
python main/tools/manage.py sync eiendom

# Full sync - replace everything (use with caution)
python main/sync/sync_to_sheets.py --full
python main/tools/manage.py sync eiendom --full
```

### Set Up Your Sheet:

1. Keep the same column headers: `Finnkode`, `Tilgjengelighet`, `Adresse`, `Postnummer`, `Pris`, `URL`, `AREAL`, `PRIS KVM`
2. Apply conditional formatting to **entire columns** (not just specific rows)
3. New listings will automatically appear at the bottom with formatting applied

## Database Location

**Default location:** `/Users/tehbaer/Kode/skannonser/main/database/properties.db`

This is a single file containing all your data. Easy to backup!

## View Your Data

### From Command Line:

```bash
# View stats (CLI tool)
python main/tools/manage.py stats

# View stats (direct)
python -c "from main.database.db import PropertyDatabase; db = PropertyDatabase(); print(db.get_stats('eiendom'))"

# Open database directly
sqlite3 main/database/properties.db
# Then run SQL queries:
SELECT COUNT(*) FROM eiendom WHERE is_active = 1;
SELECT * FROM eiendom ORDER BY scraped_at DESC LIMIT 10;
.quit
```

### Export to CSV:

```python
# Quick export script
from main.database.db import PropertyDatabase
db = PropertyDatabase()
df = db.get_active_listings('eiendom')
df.to_csv('export.csv', index=False)
```

Or use the CLI:
```bash
python main/tools/manage.py export eiendom
```

## Troubleshooting

### "Module not found" errors

```bash
# Make sure you're in the virtual environment
source .venv/bin/activate
```

### Google Sheets authentication

```bash
# If you get auth errors, re-authenticate:
rm main/config/token.json
python main/sync/sync_to_sheets.py
# Or:
python main/tools/manage.py sync eiendom
# Follow the browser prompts
```

### Database locked error

```bash
# Check if another process is using it
ps aux | grep python
# Kill if needed
pkill -f scheduler.py
```

## Backup Your Data

### Automatic Backup Script:

```bash
# Create backups directory
mkdir -p backups

# Manual backup
cp main/database/properties.db backups/properties_$(date +%Y%m%d).db

# Add to crontab for daily automatic backup (2 AM)
0 2 * * * cp /Users/tehbaer/Kode/skannonser/main/database/properties.db /Users/tehbaer/Kode/skannonser/backups/properties_$(date +\%Y\%m\%d).db
```

## Next Steps

### For Server Deployment:

See detailed instructions in [README_DEPLOYMENT.md](README_DEPLOYMENT.md)

**Quick checklist:**
- [ ] Choose hosting (DigitalOcean $6/month, or AWS free tier)
- [ ] Set up cron job for daily runs
- [ ] Configure backups
- [ ] Test Google Sheets sync

### For Development:

Keep using your existing workflow! The old CSV-based system still works. The database version is additive.

## FAQ

**Q: Will my CSV files be deleted?**  
A: No! CSV files are still generated. The database is an additional storage layer.

**Q: What happens to my Google Sheets?**  
A: Your sheet stays exactly as is. New listings are appended to the bottom. Formatting is preserved.

**Q: Can I go back to the old way?**  
A: Yes! Your original `run_eiendom.py` still works unchanged.

**Q: How do I run this on a schedule?**  
A: Use cron (see above) or see README_DEPLOYMENT.md for full server setup options.

**Q: Where is my data stored?**  
A: In `properties.db` (SQLite file) in the project root. You can query it with any SQLite tool.

**Q: Does this cost money?**  
A: The database itself is free (SQLite). Server hosting costs $0-6/month depending on where you host it.

## Support

For issues or questions:
1. Check logs: `tail -f logs/scraper.log`
2. Test each component:
   - Database: `python main/db.py`
   - Scraper: `python main/run_eiendom_db.py`
   - Sync: `python main/sync_to_sheets.py`
3. See [README_DEPLOYMENT.md](README_DEPLOYMENT.md) for detailed troubleshooting

## Summary of Commands

```bash
# Initial setup
python main/database/migrate_to_db.py    # One-time: Import existing CSV data
python main/tools/manage.py migrate      # (or use CLI tool)

# Regular usage (CLI tool - recommended)
python main/tools/manage.py run eiendom      # Run everything (scrape + save + sync)
python main/tools/manage.py scrape eiendom   # Just scrape and save to DB
python main/tools/manage.py sync eiendom     # Just sync DB to Sheets
python main/tools/manage.py stats            # View database stats
python main/tools/manage.py export eiendom   # Export to CSV
python main/tools/manage.py backup           # Backup database
python main/tools/manage.py dashboard        # Web dashboard

# Direct script usage (alternative)
python main/runners/run_eiendom_db.py        # Just scrape and save to DB
python main/sync/sync_to_sheets.py           # Just sync DB to Sheets
python main/tools/scheduler.py eiendom       # Run full workflow

# Database operations
python main/database/db.py                   # View database stats
sqlite3 main/database/properties.db          # Query database directly

# Monitoring
python main/monitoring/monitor_dashboard.py  # Web dashboard
python main/monitoring/test_setup.py         # Test setup

# Scheduling (cron)
crontab -e                                   # Edit cron jobs
tail -f logs/scraper.log                     # View logs
```

---

**Ready to deploy to a server?** → See [README_DEPLOYMENT.md](README_DEPLOYMENT.md)

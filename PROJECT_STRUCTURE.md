# Project Structure

## Overview

This project has been reorganized for better maintainability and scalability.

## Folder Structure

```
/Users/tehbaer/Kode/skannonser/
├── main/
│   ├── database/              # 🗄️  Database operations
│   │   ├── __init__.py
│   │   ├── db.py             # Core database operations
│   │   ├── migrate_to_db.py  # CSV to DB migration
│   │   └── properties.db     # SQLite database file
│   │
│   ├── sync/                  # 🔄 Google Sheets synchronization
│   │   ├── __init__.py
│   │   └── sync_to_sheets.py # Sync DB to Google Sheets
│   │
│   ├── monitoring/            # 📊 Monitoring and testing
│   │   ├── __init__.py
│   │   ├── monitor_dashboard.py  # Web dashboard (port 8000)
│   │   └── test_setup.py         # Setup verification tests
│   │
│   ├── tools/                 # 🛠️  Management and scheduling
│   │   ├── __init__.py
│   │   ├── manage.py         # CLI management tool (main interface)
│   │   └── scheduler.py      # Automated task scheduler
│   │
│   ├── runners/               # ▶️  Scraper execution scripts
│   │   ├── __init__.py
│   │   ├── run_eiendom.py        # Original CSV-based runner
│   │   ├── run_eiendom_db.py     # Database-based runner
│   │   ├── run_jobs_FINN.py      # FINN jobs scraper
│   │   ├── run_jobs_NAV.py       # NAV jobs scraper
│   │   ├── run_rental.py         # Rental properties scraper
│   │   └── run_helper.py         # Common runner utilities
│   │
│   ├── extractors/            # 📥 Data extraction and parsing
│   │   ├── __init__.py
│   │   ├── extraction_eiendom.py         # Property extraction
│   │   ├── extraction_jobs_FINN.py       # FINN jobs extraction
│   │   ├── extraction_jobs_NAV.py        # NAV jobs extraction
│   │   ├── extraction_rental.py          # Rental extraction
│   │   ├── extraction.py                 # Generic extraction
│   │   ├── parsing_helpers_jobs_FINN.py  # FINN parsing helpers
│   │   ├── parsing_helpers_jobs_NAV.py   # NAV parsing helpers
│   │   └── parsing_helpers_rental.py     # Rental parsing helpers
│   │
│   ├── config/                # ⚙️  Configuration and credentials
│   │   ├── __init__.py
│   │   ├── config.py         # Configuration settings
│   │   ├── credentials.json  # Google API credentials
│   │   ├── token.json        # Google auth token
│   │   ├── requirements.txt  # Python dependencies
│   │   └── setup.py          # Setup configuration
│   │
│   ├── temp/                  # 🗑️  Temporary/test files
│   │   ├── debug.py
│   │   ├── test.py
│   │   └── integration_example.py
│   │
│   ├── crawl.py              # 🕷️  Web crawling utilities
│   ├── post_process.py       # 🔧 Data post-processing
│   ├── export.py             # 📤 Export utilities (legacy)
│   ├── googleUtils.py        # 📊 Google Sheets utilities
│   └── location_features.py  # 📍 Location-based features
│
├── readme.md                 # 📖 Project documentation
├── data/                     # All scraped data
│   ├── eiendom/             # Property data (CSVs, HTML)
│   ├── flippe/              # Rental data
│   └── jobbe/               # Jobs data
├── backups/                  # Database backups
├── logs/                     # Log files
│
├── QUICKSTART.md             # 🚀 5-minute quick start
├── README_DEPLOYMENT.md      # 🚢 Server deployment guide
├── UPGRADE_SUMMARY.md        # 📝 What's new
└── PROJECT_STRUCTURE.md      # 📁 This file
```

## Module Organization

### 🗄️ `database/`
**Purpose**: All database-related operations
- **db.py**: Core database class with CRUD operations
- **migrate_to_db.py**: Migration script from CSV to database

**Key Classes**:
- `PropertyDatabase` - Main database interface

**Usage**:
```python
from main.database.db import PropertyDatabase
db = PropertyDatabase()
stats = db.get_stats('eiendom')
```

### 🔄 `sync/`
**Purpose**: Synchronization with Google Sheets
- **sync_to_sheets.py**: Incremental and full sync operations

**Key Functions**:
- `sync_eiendom_to_sheets()` - Sync new listings only
- `full_sync_eiendom_to_sheets()` - Full replacement

**Usage**:
```python
from main.sync.sync_to_sheets import sync_eiendom_to_sheets
sync_eiendom_to_sheets()
```

### 📊 `monitoring/`
**Purpose**: Monitoring, testing, and health checks
- **monitor_dashboard.py**: Web-based dashboard (localhost:8000)
- **test_setup.py**: System verification tests

**Usage**:
```bash
# Web dashboard
python main/monitoring/monitor_dashboard.py

# Test setup
python main/monitoring/test_setup.py
```

### 🛠️ `tools/`
**Purpose**: Management and automation tools
- **manage.py**: CLI tool for all operations (⭐ **Main interface**)
- **scheduler.py**: Automated task execution

**Usage**:
```bash
# CLI tool (recommended)
python main/tools/manage.py --help
python main/tools/manage.py stats
python main/tools/manage.py run eiendom

# Scheduler
python main/tools/scheduler.py eiendom
```

### ▶️ `runners/`
**Purpose**: Scraper execution scripts
- **run_eiendom_db.py**: Database-enabled property scraper
- **run_eiendom.py**: Original CSV-based scraper (legacy)
- **run_jobs_FINN.py**: FINN job listings scraper
- **run_jobs_NAV.py**: NAV job listings scraper
- **run_rental.py**: Rental properties scraper
- **run_helper.py**: Common utilities

**Usage**:
```python
from main.runners.run_eiendom_db import run_eiendom_scrape
run_eiendom_scrape()
```

### 📥 `extractors/`
**Purpose**: Data extraction and parsing logic
- **extraction_*.py**: Extraction logic for different data types
- **parsing_helpers_*.py**: Helper functions for parsing

**Usage**:
```python
from main.extractors.extraction_eiendom import extractEiendomDataFromAds
extractEiendomDataFromAds(project, urls, output)
```

## Import Patterns

### From Outside main/
```python
# Database
from main.database.db import PropertyDatabase

# Sync
from main.sync.sync_to_sheets import sync_eiendom_to_sheets

# Tools
from main.tools.scheduler import run_scheduled_task

# Runners
from main.runners.run_eiendom_db import run_eiendom_scrape

# Extractors
from main.extractors.extraction_eiendom import extractEiendomDataFromAds
```

### From Inside main/
```python
# Database
from database.db import PropertyDatabase

# Sync
from sync.sync_to_sheets import sync_eiendom_to_sheets

# Utilities (same level)
from crawl import extract_URLs
from post_process import post_process_eiendom
```

## Command Paths (Updated)

### New Paths:
```bash
# Management CLI (⭐ Use this!)
python main/tools/manage.py [command]

# Direct script access
python main/runners/run_eiendom_db.py
python main/sync/sync_to_sheets.py
python main/monitoring/monitor_dashboard.py
python main/database/migrate_to_db.py
python main/tools/scheduler.py eiendom
```

### Backward Compatibility:
The CLI tool (`manage.py`) abstracts these paths, so you don't need to remember them:
```bash
python main/tools/manage.py run eiendom      # Better than: python main/runners/run_eiendom_db.py
python main/tools/manage.py stats            # Better than: python main/database/db.py
python main/tools/manage.py dashboard        # Better than: python main/monitoring/monitor_dashboard.py
```

## Quick Reference

### Common Tasks:

| Task | Command |
|------|---------|
| Run full workflow | `python main/tools/manage.py run eiendom` |
| View stats | `python main/tools/manage.py stats` |
| Scrape only | `python main/tools/manage.py scrape eiendom` |
| Sync to Sheets | `python main/tools/manage.py sync eiendom` |
| Web dashboard | `python main/tools/manage.py dashboard` |
| Backup database | `python main/tools/manage.py backup` |
| Export to CSV | `python main/tools/manage.py export eiendom` |
| Migrate CSV data | `python main/tools/manage.py migrate` |
| Test setup | `python main/monitoring/test_setup.py` |

### Cron Job (Updated Path):
```bash
# Daily at 6 AM
0 6 * * * cd /Users/tehbaer/Kode/skannonser && .venv/bin/python main/tools/manage.py run eiendom >> logs/scraper.log 2>&1
```

## Design Principles

### Separation of Concerns:
- **database/**: Data persistence layer
- **sync/**: External integrations (Google Sheets)
- **monitoring/**: Observability and testing
- **tools/**: User-facing utilities
- **runners/**: Orchestration and execution
- **extractors/**: Business logic (extraction & parsing)
- **Core utilities**: Stay in main/ (crawl, post_process, config, etc.)

### Why This Structure?

1. **Easier to find things**: Related files are grouped together
2. **Scalability**: Easy to add new scrapers, extractors, or tools
3. **Testing**: Each module can be tested independently
4. **Onboarding**: New developers understand structure quickly
5. **Maintenance**: Changes are localized to specific folders

## Migration Notes

### Old Path → New Path:
```
main/db.py → main/database/db.py
main/sync_to_sheets.py → main/sync/sync_to_sheets.py
main/monitor_dashboard.py → main/monitoring/monitor_dashboard.py
main/manage.py → main/tools/manage.py
main/scheduler.py → main/tools/scheduler.py
main/run_eiendom_db.py → main/runners/run_eiendom_db.py
main/extraction_eiendom.py → main/extractors/extraction_eiendom.py
```

### Update Your Scripts:
If you have custom scripts importing these modules, update imports:

**Before:**
```python
from main.db import PropertyDatabase
from main.sync_to_sheets import sync_eiendom_to_sheets
```

**After:**
```python
from main.database.db import PropertyDatabase
from main.sync.sync_to_sheets import sync_eiendom_to_sheets
```

### Use the CLI Tool Instead:
Better yet, use the CLI tool which handles all paths internally:
```bash
python main/tools/manage.py [command]
```

## Adding New Components

### New Scraper:
1. Create extractor: `main/extractors/extraction_newtype.py`
2. Create runner: `main/runners/run_newtype_db.py`
3. Add to scheduler: `main/tools/scheduler.py`
4. Add to CLI: `main/tools/manage.py`

### New Database Table:
1. Update schema: `main/database/db.py` (add table in `_init_db()`)
2. Add CRUD methods: `main/database/db.py`
3. Add sync function: `main/sync/sync_to_sheets.py`

### New Tool:
1. Create script: `main/tools/new_tool.py`
2. Add to CLI: `main/tools/manage.py` (optional)

## File Count by Category

```
Database:     2 files + database file
Sync:         1 file
Monitoring:   2 files
Tools:        2 files
Runners:      6 files
Extractors:   8 files
Config:       5 files (config.py, credentials, token, requirements, setup)
Temp:         3 files (debug, test, example)
Core Utils:   ~7 files (in main/)
```

## Recommended Entry Points

### For Users:
- **`main/tools/manage.py`** ⭐ Main interface
- **`main/monitoring/monitor_dashboard.py`** Web UI

### For Developers:
- **`main/database/db.py`** Database interface
- **`main/runners/run_eiendom_db.py`** Example runner
- **`main/extractors/extraction_eiendom.py`** Example extractor

### For Ops/DevOps:
- **`main/tools/scheduler.py`** Cron job entry point
- **`main/monitoring/test_setup.py`** Health check

## Summary

The new structure is:
- ✅ **More organized** - Files grouped by purpose
- ✅ **Scalable** - Easy to add new components
- ✅ **Maintainable** - Clear separation of concerns
- ✅ **Professional** - Follows Python best practices

**Use the CLI tool** (`main/tools/manage.py`) for all operations - it's the easiest interface!

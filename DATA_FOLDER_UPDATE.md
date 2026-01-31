# Data Folder Reorganization

## Overview
All scraped data folders have been moved into a centralized `data/` directory for better organization.

## Changes Made

### Folder Structure
**Before:**
```
skannonser/
├── eiendom/
├── flippe/
├── jobbe/
└── main/
```

**After:**
```
skannonser/
├── data/
│   ├── eiendom/
│   ├── flippe/
│   └── jobbe/
└── main/
```

### Code Updates
All references to the data folders have been updated in:

1. **Runner Scripts** - Updated `projectName` variables:
   - `main/runners/run_eiendom.py` → `'data/eiendom'`
   - `main/runners/run_eiendom_db.py` → `'data/eiendom'`
   - `main/runners/run_rental.py` → `'data/flippe'`
   - `main/runners/run_jobs_FINN.py` → `'data/jobbe'`
   - `main/runners/run_jobs_NAV.py` → `'data/jobbe'`
   - `main/run_eiendom.py` → `'data/eiendom'`

2. **Extraction Scripts**:
   - `main/extractors/extraction_eiendom.py` → Updated hardcoded path

3. **Database Scripts**:
   - `main/database/migrate_to_db.py` → Updated CSV path

4. **Documentation**:
   - `PROJECT_STRUCTURE.md` → Updated folder tree
   - `README_DEPLOYMENT.md` → Updated example paths

## Benefits
- ✅ **Cleaner Root Directory**: Data files are separated from application code
- ✅ **Better Organization**: All scraped data is in one logical location
- ✅ **Easier Backups**: Can backup/gitignore entire `data/` folder
- ✅ **Scalability**: Easy to add new data sources under `data/`

## No Action Required
All path references have been updated automatically. The system will continue to work as before, just with cleaner organization.

## Testing
Verified with: `.venv/bin/python main/tools/manage.py stats` ✓

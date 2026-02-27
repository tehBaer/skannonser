#!/usr/bin/env python3
"""
GitHub Actions entrypoint for eiendom scraping.
Runs scraping + database update only.
Intentionally excludes:
- Google Sheets sync
- Google travel API calculations
"""
import sys
import os

# Add parent and project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from main.runners.run_eiendom_db import run_eiendom_scrape
except ImportError:
    from runners.run_eiendom_db import run_eiendom_scrape


if __name__ == "__main__":
    run_eiendom_scrape(calculate_location_features=False)

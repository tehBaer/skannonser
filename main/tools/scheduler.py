"""
Scheduler for running property scraping tasks on a schedule.
Can be run as a standalone script or scheduled via cron.
"""
import sys
import os
from datetime import datetime
import argparse
import subprocess

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.runners.run_eiendom_db import run_eiendom_scrape
    from main.sync.helper_sync_to_sheets import sync_eiendom_to_sheets, sync_stale_eiendom_to_sheets
    from main.database.db import PropertyDatabase
    from main.tools.sync_finn_polygon_sheet import sync_polygon_to_sheet
except ImportError:
    from runners.run_eiendom_db import run_eiendom_scrape
    from sync.helper_sync_to_sheets import sync_eiendom_to_sheets, sync_stale_eiendom_to_sheets
    from database.db import PropertyDatabase
    from tools.sync_finn_polygon_sheet import sync_polygon_to_sheet


def _estimate_coordinate_fill_candidates(limit: int, include_inactive: bool) -> int:
    """Estimate how many listings will be passed to fill_missing_coordinates."""
    db = PropertyDatabase()
    df = db.get_eiendom_missing_coordinates()

    if not include_inactive and not df.empty:
        visible_statuses = {"solgt", "inaktiv"}
        status_normalized = (
            df["Tilgjengelighet"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )
        df = df[
            (df["stale"].fillna(0).astype(int) == 1)
            & (~status_normalized.isin(visible_statuses))
        ]

    if limit > 0:
        df = df.head(limit)

    return len(df)


def run_fill_missing_coordinates() -> None:
    """Run coordinate fill as part of full workflow.

    Uses env vars when provided:
      - COORDS_LIMIT (default: 0 = all)
      - COORDS_RPM (default: 60)
      - COORDS_INCLUDE_INACTIVE (1/yes/true to include inactive)
    """
    limit = (os.getenv("COORDS_LIMIT") or "0").strip() or "0"
    rpm = (os.getenv("COORDS_RPM") or "60").strip() or "60"
    limit_int = int(limit) if str(limit).strip() else 0
    include_inactive = (os.getenv("COORDS_INCLUDE_INACTIVE") or "0").strip().lower() in {"1", "yes", "true"}

    cmd = [
        sys.executable,
        "-m",
        "main.tools.fill_missing_coordinates",
        "--limit",
        limit,
        "--rpm",
        rpm,
    ]
    if include_inactive:
        cmd.append("--include-inactive")

    confirm_setting = (os.getenv("COORDS_CONFIRM") or "1").strip().lower()
    require_confirm = confirm_setting not in {"0", "no", "false"}

    if require_confirm:
        candidate_count = _estimate_coordinate_fill_candidates(limit_int, include_inactive)
        print(
            "Geocode preflight: "
            f"{candidate_count} candidate(s) "
            f"(limit={limit_int if limit_int > 0 else 'all'}, include_inactive={include_inactive}, rpm={rpm})"
        )

        prompt = (
            "Coordinate fill uses Google Geocoding API (billable). "
            "Proceed now? [y/N]: "
        )

        if sys.stdin.isatty():
            answer = input(prompt).strip().lower()
            if answer not in {"y", "yes"}:
                print("Coordinate fill skipped by user.")
                return
        else:
            print("Coordinate fill confirmation required, but no interactive terminal detected. Skipping.")
            return

    print(f"Running coordinate fill: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)

    # fill_missing_coordinates returns 2 when some rows fail to geocode.
    # Treat that as non-fatal so scrape/sync still complete with partial progress.
    if result.returncode in (0, 2):
        if result.returncode == 2:
            print("Coordinate fill completed with some unresolved rows; continuing workflow.")
        return

    raise RuntimeError(f"Coordinate fill failed with exit code {result.returncode}")


def run_scheduled_task(task_name: str, sync_sheets: bool = True):
    """Run a scheduled scraping task."""
    print(f"\n{'='*60}")
    print(f"Starting scheduled task: {task_name}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")
    
    try:
        if task_name == 'eiendom':
            # Run the scraper
            print("Step 1: Scraping property listings...")
            run_eiendom_scrape()

            # Fill missing coordinates for listings.
            print("\nStep 2: Filling missing coordinates...")
            run_fill_missing_coordinates()
            
            # Sync to Google Sheets if requested
            if sync_sheets:
                print("\nStep 3: Syncing to Google Sheets...")
                sync_eiendom_to_sheets()
                sync_stale_eiendom_to_sheets()
                sync_polygon_to_sheet(sheet_name="Finn Polygon Coords", source_file=os.path.join("main", "runners", "run_eiendom_db.py"))
            
            print(f"\n✓ Task '{task_name}' completed successfully")
            
        elif task_name == 'rental':
            print("Rental scraping not yet implemented with database")
            # TODO: Implement run_rental_db and sync
            
        elif task_name == 'jobs':
            print("Jobs scraping not yet implemented with database")
            # TODO: Implement run_jobs_db and sync
            
        else:
            print(f"Unknown task: {task_name}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Error running task '{task_name}': {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        print(f"\n{'='*60}")
        print(f"Task finished at: {datetime.now().isoformat()}")
        print(f"{'='*60}\n")


def main():
    """Main entry point for scheduler."""
    parser = argparse.ArgumentParser(description='Schedule property scraping tasks')
    parser.add_argument('task', 
                       choices=['eiendom', 'rental', 'jobs', 'all'],
                       help='Task to run')
    parser.add_argument('--no-sync', 
                       action='store_true',
                       help='Skip syncing to Google Sheets')
    
    args = parser.parse_args()
    
    sync_sheets = not args.no_sync
    
    if args.task == 'all':
        # Run all tasks
        tasks = ['eiendom', 'rental', 'jobs']
        exit_code = 0
        for task in tasks:
            result = run_scheduled_task(task, sync_sheets)
            if result != 0:
                exit_code = result
        return exit_code
    else:
        # Run single task
        return run_scheduled_task(args.task, sync_sheets)


if __name__ == "__main__":
    sys.exit(main())

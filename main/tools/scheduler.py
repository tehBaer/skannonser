"""
Scheduler for running property scraping tasks on a schedule.
Can be run as a standalone script or scheduled via cron.
"""
import sys
import os
from datetime import datetime
import argparse

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from main.runners.run_eiendom_db import run_eiendom_scrape
    from main.sync.sync_to_sheets import sync_eiendom_to_sheets
except ImportError:
    from runners.run_eiendom_db import run_eiendom_scrape
    from sync.sync_to_sheets import sync_eiendom_to_sheets


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
            
            # Sync to Google Sheets if requested
            if sync_sheets:
                print("\nStep 2: Syncing to Google Sheets...")
                sync_eiendom_to_sheets()
            
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

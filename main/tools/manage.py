#!/usr/bin/env python3
"""
Command-line interface for managing the property scraping system.
Makes it easy to run common tasks without remembering exact commands.

Usage:
    python manage.py --help
"""
# Add parent and project root to path for imports
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # main folder
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # project root

import argparse
from datetime import datetime

try:
    from main.database.db import PropertyDatabase
    from main.runners.run_eiendom_db import run_eiendom_scrape
    from main.sync.helper_sync_to_sheets import sync_eiendom_to_sheets, full_sync_eiendom_to_sheets
    from main.tools.scheduler import run_scheduled_task
except ImportError:
    from database.db import PropertyDatabase
    from runners.run_eiendom_db import run_eiendom_scrape
    from sync.helper_sync_to_sheets import sync_eiendom_to_sheets, full_sync_eiendom_to_sheets
    from scheduler import run_scheduled_task


def cmd_stats(args):
    """Show database statistics."""
    db = PropertyDatabase(args.db)
    
    print(f"\n{'='*60}")
    print(f"Database Statistics")
    print(f"{'='*60}")
    print(f"\nDatabase: {db.db_path}\n")
    
    for table in ['eiendom', 'leie', 'jobbe']:
        stats = db.get_stats(table)
        emoji = '🏠' if table == 'eiendom' else '🔑' if table == 'leie' else '💼'
        
        print(f"{emoji} {table.upper()}:")
        print(f"  Total:        {stats['total']}")
        print(f"  Active:       {stats['active']} ✓")
        print(f"  Inactive:     {stats['inactive']} ✗")
        print(f"  Not exported: {stats['not_exported']} ⏳")
        print()


def cmd_scrape(args):
    """Run the scraper."""
    print(f"\n{'='*60}")
    print(f"Running {args.type} scraper")
    print(f"{'='*60}\n")
    
    if args.type == 'eiendom':
        run_eiendom_scrape(args.db)
    else:
        print(f"Scraper for '{args.type}' not yet implemented")
        return 1
    
    return 0


def cmd_sync(args):
    """Sync database to Google Sheets."""
    print(f"\n{'='*60}")
    print(f"Syncing {args.type} to Google Sheets")
    print(f"{'='*60}\n")
    
    if args.type == 'eiendom':
        if args.full:
            return 0 if full_sync_eiendom_to_sheets(args.db, args.sheet) else 1
        else:
            return 0 if sync_eiendom_to_sheets(args.db, args.sheet) else 1
    else:
        print(f"Sync for '{args.type}' not yet implemented")
        return 1


def cmd_run(args):
    """Run full workflow (scrape + sync)."""
    print(f"\n{'='*60}")
    print(f"Running full workflow for {args.type}")
    print(f"{'='*60}\n")
    
    return run_scheduled_task(args.type, sync_sheets=not args.no_sync)


def cmd_export(args):
    """Export database to CSV."""
    db = PropertyDatabase(args.db)
    
    if args.type == 'all':
        tables = ['eiendom', 'leie', 'jobbe']
    else:
        tables = [args.type]
    
    print(f"\n{'='*60}")
    print(f"Exporting to CSV")
    print(f"{'='*60}\n")
    
    for table in tables:
        df = db.get_active_listings(table)
        if not df.empty:
            output_file = f"{table}_export_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(output_file, index=False)
            print(f"✓ Exported {len(df)} records from {table} to {output_file}")
        else:
            print(f"⚠ No data in {table} to export")
    
    return 0


def cmd_backup(args):
    """Backup database."""
    import shutil
    
    db = PropertyDatabase(args.db)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"backups/properties_{timestamp}.db"
    
    os.makedirs('backups', exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Creating database backup")
    print(f"{'='*60}\n")
    
    shutil.copy2(db.db_path, backup_path)
    print(f"✓ Database backed up to: {backup_path}")
    
    # Show backup size
    size_mb = os.path.getsize(backup_path) / (1024 * 1024)
    print(f"  Size: {size_mb:.2f} MB")
    
    return 0


def cmd_dashboard(args):
    """Start monitoring dashboard."""
    print(f"\n{'='*60}")
    print(f"Starting monitoring dashboard")
    print(f"{'='*60}\n")
    
    try:
        from main.monitoring.monitor_dashboard import main as dashboard_main
        dashboard_main()
    except ImportError:
        from monitoring.monitor_dashboard import main as dashboard_main
        dashboard_main()
    
    return 0


def cmd_migrate(args):
    """Migrate from CSV to database."""
    print(f"\n{'='*60}")
    print(f"Migrating CSV data to database")
    print(f"{'='*60}\n")
    
    try:
        from main.database.migrate_to_db import main as migrate_main
        migrate_main()
    except ImportError:
        from database.migrate_to_db import main as migrate_main
        migrate_main()
    
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Manage the property scraping system',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s stats                    # Show database statistics
  %(prog)s scrape eiendom           # Run eiendom scraper
  %(prog)s sync eiendom             # Sync new listings to Google Sheets
  %(prog)s run eiendom              # Run full workflow (scrape + sync)
  %(prog)s export eiendom           # Export database to CSV
  %(prog)s backup                   # Create database backup
  %(prog)s dashboard                # Start web dashboard
  %(prog)s migrate                  # Migrate CSV data to database
        """
    )
    
    parser.add_argument('--db', help='Path to database file (optional)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Stats command
    parser_stats = subparsers.add_parser('stats', help='Show database statistics')
    parser_stats.set_defaults(func=cmd_stats)
    
    # Scrape command
    parser_scrape = subparsers.add_parser('scrape', help='Run scraper')
    parser_scrape.add_argument('type', choices=['eiendom', 'rental', 'jobs'],
                              help='Type of scraper to run')
    parser_scrape.set_defaults(func=cmd_scrape)
    
    # Sync command
    parser_sync = subparsers.add_parser('sync', help='Sync to Google Sheets')
    parser_sync.add_argument('type', choices=['eiendom', 'rental', 'jobs'],
                            help='Type of data to sync')
    parser_sync.add_argument('--full', action='store_true',
                            help='Full sync (overwrite all data)')
    parser_sync.add_argument('--sheet', default='Eie',
                            help='Sheet name (default: Eie)')
    parser_sync.set_defaults(func=cmd_sync)
    
    # Run command (full workflow)
    parser_run = subparsers.add_parser('run', help='Run full workflow (scrape + sync)')
    parser_run.add_argument('type', choices=['eiendom', 'rental', 'jobs', 'all'],
                           help='Type of workflow to run')
    parser_run.add_argument('--no-sync', action='store_true',
                           help='Skip syncing to Google Sheets')
    parser_run.set_defaults(func=cmd_run)
    
    # Export command
    parser_export = subparsers.add_parser('export', help='Export database to CSV')
    parser_export.add_argument('type', choices=['eiendom', 'rental', 'jobs', 'all'],
                              help='Type of data to export')
    parser_export.set_defaults(func=cmd_export)
    
    # Backup command
    parser_backup = subparsers.add_parser('backup', help='Create database backup')
    parser_backup.set_defaults(func=cmd_backup)
    
    # Dashboard command
    parser_dashboard = subparsers.add_parser('dashboard', 
                                            help='Start monitoring dashboard')
    parser_dashboard.set_defaults(func=cmd_dashboard)
    
    # Migrate command
    parser_migrate = subparsers.add_parser('migrate', 
                                          help='Migrate CSV data to database')
    parser_migrate.set_defaults(func=cmd_migrate)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

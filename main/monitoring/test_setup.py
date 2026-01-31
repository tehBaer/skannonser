#!/usr/bin/env python3
"""
Quick test script to verify the database system is working correctly.
Run this to check your setup before using the system.
"""
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    try:
        from main.database.db import PropertyDatabase
        from main.runners.run_eiendom_db import run_eiendom_scrape
        from main.sync.sync_to_sheets import sync_eiendom_to_sheets
        from main.tools.scheduler import run_scheduled_task
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False


def test_database():
    """Test database creation and operations."""
    print("\nTesting database...")
    try:
        from main.database.db import PropertyDatabase
        import pandas as pd
        
        # Create test database
        test_db_path = 'test_properties.db'
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
        
        db = PropertyDatabase(test_db_path)
        print(f"✓ Database created at: {db.db_path}")
        
        # Test insert
        test_data = pd.DataFrame({
            'Finnkode': ['12345'],
            'Tilgjengelighet': ['Solgt'],
            'Adresse': ['Test Address'],
            'Postnummer': ['0123'],
            'Pris': [1000000],
            'URL': ['https://test.com'],
            'AREAL': [50],
            'PRIS KVM': [20000]
        })
        
        inserted, updated = db.insert_or_update_eiendom(test_data)
        print(f"✓ Insert test passed: {inserted} inserted")
        
        # Test query
        stats = db.get_stats('eiendom')
        print(f"✓ Query test passed: {stats['total']} total records")
        
        # Cleanup
        os.remove(test_db_path)
        print("✓ Database test complete")
        return True
        
    except Exception as e:
        print(f"✗ Database error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_google_credentials():
    """Test Google Sheets credentials."""
    print("\nTesting Google credentials...")
    try:
        from main.googleUtils import get_credentials, SPREADSHEET_ID
        
        # Check if credentials file exists
        creds_path = 'main/credentials.json'
        if not os.path.exists(creds_path):
            print(f"⚠ Credentials file not found: {creds_path}")
            print("  You'll need this for Google Sheets sync")
            return False
        
        print(f"✓ Credentials file found")
        print(f"✓ Spreadsheet ID configured: {SPREADSHEET_ID}")
        
        # Try to get credentials (may require browser auth)
        print("  Attempting to authenticate...")
        creds = get_credentials()
        print("✓ Google authentication successful")
        return True
        
    except Exception as e:
        print(f"⚠ Google credentials warning: {e}")
        print("  You can still use the database features")
        return False


def test_dependencies():
    """Test that all required packages are installed."""
    print("\nTesting dependencies...")
    required = [
        'pandas',
        'requests',
        'beautifulsoup4',
        'google-api-python-client',
        'google-auth',
        'geopy'
    ]
    
    missing = []
    for package in required:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} missing")
            missing.append(package)
    
    if missing:
        print(f"\n⚠ Missing packages: {', '.join(missing)}")
        print("  Install with: pip install -r main/requirements.txt")
        return False
    
    print("✓ All dependencies installed")
    return True


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("System Test - Database Setup Verification")
    print("="*60 + "\n")
    
    results = {
        'Dependencies': test_dependencies(),
        'Imports': test_imports(),
        'Database': test_database(),
        'Google Credentials': test_google_credentials()
    }
    
    print("\n" + "="*60)
    print("Test Results")
    print("="*60 + "\n")
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:20s} {status}")
    
    print("\n" + "="*60)
    
    all_passed = all(results.values())
    critical_passed = results['Dependencies'] and results['Imports'] and results['Database']
    
    if all_passed:
        print("✓ All tests passed! System is ready to use.")
        print("\nNext steps:")
        print("  1. Run: python main/manage.py stats")
        print("  2. Test: python main/manage.py scrape eiendom")
        print("  3. Sync: python main/manage.py sync eiendom")
    elif critical_passed:
        print("✓ Core features working (database and scraping)")
        print("⚠ Google Sheets sync may not work (authentication needed)")
        print("\nYou can still use:")
        print("  - Database storage")
        print("  - Scraping")
        print("  - CSV exports")
    else:
        print("✗ Some tests failed. Check errors above.")
        print("\nTroubleshooting:")
        print("  1. Activate virtual environment: source .venv/bin/activate")
        print("  2. Install dependencies: pip install -r main/requirements.txt")
        print("  3. Check Python version: python --version (need 3.12+)")
    
    print("="*60 + "\n")
    
    return 0 if critical_passed else 1


if __name__ == "__main__":
    sys.exit(main())

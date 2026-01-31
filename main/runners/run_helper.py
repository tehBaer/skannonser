import pandas as pd
import sys
import os
from pathlib import Path

try:
    from main.crawl import extract_URLs
    from main.export import try_verify_align_filter_merge_below
    from main.extractors.extraction_jobs_FINN import extractJobDataFromAds_FINN
    from main.post_process import post_process_jobs
except ImportError:
    from crawl import extract_URLs
    from export import try_verify_align_filter_merge_below
    from extractors.extraction_jobs_FINN import extractJobDataFromAds_FINN
    from post_process import post_process_jobs

def ensure_venv():
    """
    Checks if the script is running in the correct virtual environment.
    If not, it re-executes the script with the virtual environment's Python interpreter.
    """
    # Locate venv relative to this file
    # Script is in main/runners, .venv is two levels up
    venv_path = Path(__file__).resolve().parent.parent.parent / '.venv'

    if sys.platform == 'win32':
        venv_python = venv_path / 'Scripts' / 'python.exe'
    else:  # macOS / Linux
        venv_python = venv_path / 'bin' / 'python'

    # If we're not already running under the venv interpreter, re-execute with it
    if venv_python.exists():
        if Path(sys.executable).resolve() != venv_python.resolve():
            print(f"Restarting script with venv python: {venv_python}")
            # Re-execute the script with the venv's python interpreter
            os.execv(str(venv_python), [str(venv_python)] + sys.argv)
    else:
        # Exit if the virtual environment is not found
        sys.exit(f"Venv python not found at {venv_python}")

# --- Venv check ---
ensure_venv()


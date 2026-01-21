import os
import subprocess
import sys
import shutil
import re

venv_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '.venv'))
requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')

# Parse an optional "Uses Python X.Y" line in requirements.txt to determine
# a minimum required Python version for the project.
def parse_min_python(req_path: str):
    if not os.path.exists(req_path):
        return None
    try:
        with open(req_path, 'r', encoding='utf-8') as f:
            for _ in range(5):  # only inspect the first few lines
                line = f.readline()
                if not line:
                    break
                m = re.search(r'Uses Python\s*([0-9]+(?:\.[0-9]+)*)', line, re.IGNORECASE)
                if m:
                    ver = tuple(int(x) for x in m.group(1).split('.'))
                    return ver
    except Exception:
        return None
    return None

min_python = parse_min_python(requirements_path)
if min_python:
    # Compare only the same number of components as specified (e.g. 3.10 -> compare major/minor)
    running = tuple(sys.version_info[:len(min_python)])
    if running < min_python:
        if os.environ.get('SKIP_PYTHON_CHECK') == '1':
            print(f"Warning: running Python {sys.version.split()[0]} is older than the project's requested Python {'.'.join(map(str,min_python))}, continuing due to SKIP_PYTHON_CHECK=1")
        else:
            print('\nERROR: Your Python interpreter is too old for this project.')
            print(f"Project requests Python {'.'.join(map(str,min_python))} (from {requirements_path}), but you're running {sys.version.split()[0]}")
            print('\nPlease install a compatible Python and re-run the script. Options:')
            print('  - macOS / Linux: install a newer python (e.g. pyenv or system package) then:')
            print('      python3.10 -m venv .venv')
            print('      .venv/bin/python -m pip install --upgrade pip')
            print('      .venv/bin/python -m pip install -r main/requirements.txt')
            print('  - Windows (PowerShell):')
            print('      py -3.10 -m venv .venv')
            print('      .\.venv\Scripts\python -m pip install --upgrade pip')
            print('      .\.venv\Scripts\python -m pip install -r main\requirements.txt')
            print('\nIf you intentionally want to ignore this check set environment variable SKIP_PYTHON_CHECK=1 and re-run.')
            sys.exit(2)


def get_venv_python(venv_path: str) -> str:
    if os.name == 'nt':
        return os.path.join(venv_path, 'Scripts', 'python.exe')
    else:
        return os.path.join(venv_path, 'bin', 'python')

python_exe = get_venv_python(venv_dir)

# If venv doesn't exist, or it exists but doesn't contain the expected
# python executable for this OS, (re)create it.
if not os.path.exists(venv_dir) or not os.path.exists(python_exe):
    if os.path.exists(venv_dir) and not os.path.exists(python_exe):
        print(f'Found existing venv at {venv_dir} but it does not look compatible with this OS. Recreating...')
        try:
            shutil.rmtree(venv_dir)
        except Exception as e:
            print(f'Failed to remove existing venv: {e}')
            sys.exit(1)

    # Create the virtual environment using the Python interpreter running this script
    subprocess.run([sys.executable, '-m', 'venv', venv_dir], check=True)
    print(f'Virtual environment created at {venv_dir}')

# Refresh python_exe in case we just created the venv
python_exe = get_venv_python(venv_dir)

if not os.path.exists(requirements_path):
    print(f'Requirements file not found at {requirements_path}. Skipping install.')
else:
    # Use the venv python to run pip so we don't need to source/activate the venv
    try:
        subprocess.run([python_exe, '-m', 'pip', 'install', '--upgrade', 'pip'], check=True)
        subprocess.run([python_exe, '-m', 'pip', 'install', '-r', requirements_path], check=True)
        print('Dependencies installed')
    except subprocess.CalledProcessError as e:
        print('Failed to install dependencies:', e)
        sys.exit(e.returncode)

import subprocess
from pathlib import Path
from bs4 import BeautifulSoup
from main.parsing_helpers_jobs_FINN import FinnParser

subprocess.run(['..\\.venv\\Scripts\\activate.bat'], shell=True, check=True)
projectName = 'jobbe'

# Get all HTML files in the directory
html_dir = Path(f'{projectName}/html_extracted')
html_files = list(html_dir.glob('*.html'))[:5]  # Get first 5 HTML files

if not html_files:
    print(f"No HTML files found in {html_dir}")
else:
    for i, html_file in enumerate(html_files, 1):
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, 'html.parser')
        parser = FinnParser(soup)

        part1, part2 = parser.get_textcontent()

        print(f"\n=== {html_file.name} ===")
        if part2:
            print(part2)
        else:
            print("(No content after split)")
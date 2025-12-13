import subprocess
import pandas as pd

from main.crawl import extract_URLs
from main.export import try_verify_align_filter_merge_below
from main.extraction_jobs_FINN import extractJobDataFromAds_FINN
from main.post_process import post_process_jobs

subprocess.run(['..\\.venv\\Scripts\\activate.bat'], shell=True, check=True)

projectName = 'jobbe'
# 1
urlBase = 'https://www.finn.no/job/search?location=1.20001.20061&occupation=0.23'
regex = r'https://www\.finn\.no/job/ad/\d+'

urls = extract_URLs(urlBase, regex, projectName, "0_URLs.csv")
# urls = pd.read_csv(f'{projectName}/0_URLs.csv')  # for debugging quickly
extractJobDataFromAds_FINN(projectName, urls, "A_live.csv")

# 4
live_data = pd.read_csv(f'{projectName}/A_live.csv')
post_process_jobs(live_data, projectName, "AB_processed.csv")

headers = ['Finnkode', 'URL', 'Selskap', 'Stillingstittel', 'Industri', 'Tittel', 'Søknadsfrist', 'Posisjoner', 'FRIST', 'Innhold']

try_verify_align_filter_merge_below("Jobb",
                f"{projectName}/AB_processed.csv",
                f"{projectName}/sheet_downloaded.csv",
                f"{projectName}/C_filtered.csv",
                f"{projectName}/B_aligned.csv",
                                    headers)

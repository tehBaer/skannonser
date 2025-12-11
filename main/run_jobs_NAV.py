import subprocess
import pandas as pd
import re


from main.crawl import extract_URLs
from main.export import try_verify_align_filter_merge_below
from main.extraction_jobs_FINN import extractJobDataFromAds_FINN
from main.extraction_jobs_NAV import extractJobDataFromAds_NAV
from main.post_process import post_process_jobs

subprocess.run(['..\\.venv\\Scripts\\activate.bat'], shell=True, check=True)

projectName = 'jobbe_NAV'
# 1
urlBase = 'https://arbeidsplassen.nav.no/stillinger?county=OSLO&v=5&occupationLevel1=IT&occupationLevel2=IT.Utvikling&occupationLevel2=IT.Interaksjonsdesign&occupationLevel2=IT.Drift%2C+vedlikehold&pageCount=100'
regex = r'/stillinger/stilling/[a-f0-9-]+'
isNAV=True

# urls = extract_URLs(urlBase, regex, projectName, "0_URLs.csv", isNAV)
# urls = pd.read_csv(f'{projectName}/0_URLs.csv')  # for debugging quickly
# extractJobDataFromAds_NAV(projectName, urls, "A_live.csv")

# # 4
# live_data = pd.read_csv(f'{projectName}/A_live.csv')
# post_process_jobs(live_data, projectName, "AB_processed.csv")

headers = ['Finnkode', 'URL', 'Selskap', 'Stillingstittel', 'Tittel', 'Søknadsfrist', 'Posisjoner', 'FRIST', 'Innhold']

try_verify_align_filter_merge_below("NAV",
                f"{projectName}/AB_processed.csv",
                f"{projectName}/sheet_downloaded.csv",
                f"{projectName}/C_filtered.csv",
                f"{projectName}/B_aligned.csv",
                                    headers)


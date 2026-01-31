import pandas as pd

try:
    from .crawl import extract_URLs
    from .export import try_verify_align_filter_merge_below
    from .extraction_eiendom import extractEiendomDataFromAds
    from .post_process import post_process_eiendom
    from .run_helper import ensure_venv
except ImportError:
    from crawl import extract_URLs
    from export import try_verify_align_filter_merge_below
    from extraction_eiendom import extractEiendomDataFromAds
    from post_process import post_process_eiendom
    from run_helper import ensure_venv

ensure_venv()

projectName = 'data/eiendom'
# Extract URLs from the search results
urlBase = 'https://www.finn.no/realestate/homes/search.html?filters=&polylocation=10.515814226086547+59.830255688429475%2C10.718914241615323+59.89350518832623%2C10.860312986603077+59.90510937383482%2C10.816607919971034+59.96564316999155%2C10.233016736110244+60.03634039140428%2C10.376986367371302+59.84059035321431%2C10.515814226086547+59.830255688429475&property_type=4&property_type=1'
regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
urls = extract_URLs(urlBase, regex, projectName, "0_URLs.csv")
# urls = pd.read_csv(f'{projectName}/0_URLs.csv')  # for debugging quickly

# Extract data from each ad
extractEiendomDataFromAds(projectName, urls, "A_live.csv")

# Post-process the data
live_data = pd.read_csv(f'{projectName}/A_live.csv')
post_process_eiendom(live_data, projectName, "AB_processed.csv")

# Define headers for validation (after post-processing, area columns are replaced with AREAL and PRIS KVM)
headers = ['Finnkode', 'Tilgjengelighet', 'Adresse', 'Postnummer', 'Pris', 'URL',
           'AREAL', 'PRIS KVM']

# Verify, align, filter and merge with Google Sheets
try_verify_align_filter_merge_below("Eie",
                f"{projectName}/AB_processed.csv",
                f"{projectName}/sheet_downloaded.csv",
                f"{projectName}/C_filtered.csv",
                f"{projectName}/B_aligned.csv",
                                    headers)

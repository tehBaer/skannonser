import pandas as pd

try:
    from .crawl import extract_URLs
    from .export import try_verify_align_filter_merge_below
    from .extraction_rental import extractRentalDataFromAds
    from .post_process import post_process_rental
    from .run_helper import ensure_venv
except ImportError:
    from crawl import extract_URLs
    from export import try_verify_align_filter_merge_below
    from extraction_rental import extractRentalDataFromAds
    from post_process import post_process_rental
    from run_helper import ensure_venv

ensure_venv()

projectName = 'data/flippe'
# 1
urlBase = 'https://www.finn.no/realestate/lettings/search.html?radius=700&lat=59.939015007471454&lon=10.75032940563446&price_from=13000&price_to=18500'
regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
urls = extract_URLs(urlBase, regex, projectName, "0_URLs.csv")
# urls = pd.read_csv(f'{projectName}/0_URLs.csv')  # for debugging quickly
extractRentalDataFromAds(projectName, urls, "A_live.csv")

# Also extract data from the downloaded sheets
live_data = pd.read_csv(f'{projectName}/A_live.csv')
post_process_rental(live_data, projectName)

# Define headers for validation (after post-processing, area columns are replaced with AREAL and PRIS KVM)
headers = ['Finnkode', 'Tilgjengelighet', 'Adresse', 'Postnummer', 'Leiepris', 'Depositum', 'URL',
           'AREAL', 'PRIS KVM']


# Verify, align, filter and merge
try_verify_align_filter_merge_below("Leie2",
                f"{projectName}/AB_processed.csv",
                f"{projectName}/sheet_downloaded.csv",
                f"{projectName}/C_filtered.csv",
                f"{projectName}/B_aligned.csv",
                                    headers)

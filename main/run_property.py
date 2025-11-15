import subprocess
import pandas as pd
from main.post_process import post_process_property
from main.crawl import extract_URLs
from main.export import merge_above
from main.extraction_property import extractPropertyDataFromAds

subprocess.run(['..\\.venv\\Scripts\\activate.bat'], shell=True, check=True)

projectName = 'leie'
# 1
urlBase = 'https://www.finn.no/realestate/lettings/search.html?radius=700&lat=59.939015007471454&lon=10.75032940563446&price_from=13000&price_to=18500'
regex = r'/realestate/.*?/ad\.html\?finnkode=\d+'
urls = extract_URLs(urlBase, regex, projectName, "0_URLs.csv")
# urls = pd.read_csv(f'{projectName}/0_URLs.csv')
extractPropertyDataFromAds(projectName, urls, "live_data.csv")

# ALso extract data from the downloaded sheets

data = pd.read_csv(f'{projectName}/live_data.csv')
post_process_property(data, projectName, "live_data_parsed.csv")

# 4
emptyColCount = 3
merge_above(emptyColCount,
      "Leie",
      f"{projectName}/live_data_parsed.csv",
      f"{projectName}/sheet_downloaded.csv",
      f"{projectName}/live_missing_parsed.csv")

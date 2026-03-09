import os
import json
import glob
import pandas as pd
from bs4 import BeautifulSoup


def extract_urls_from_saved_html(project_folder: str):
    folder = os.path.join(project_folder, 'html_crawled')
    files = sorted(glob.glob(os.path.join(folder, 'page*.html')))
    urls = []
    seen = set()

    for path in files:
        with open(path, 'r', encoding='utf-8') as fh:
            soup = BeautifulSoup(fh.read(), 'html.parser')
            scripts = soup.find_all('script', type='application/ld+json')
            for s in scripts:
                text = s.string
                if not text:
                    continue
                try:
                    data = json.loads(text)
                except Exception:
                    continue

                # data may be a dict or a list
                items = data if isinstance(data, list) else [data]
                for entry in items:
                    # If it's an ItemList, look into itemListElement
                    if entry.get('@type') == 'ItemList' and 'itemListElement' in entry:
                        for li in entry['itemListElement']:
                            item = li.get('item') or li.get('@item') or {}
                            if isinstance(item, dict):
                                url = item.get('url') or item.get('@id') or item.get('identifier')
                                if url and url not in seen:
                                    seen.add(url)
                                    urls.append(url)
                    else:
                        # If it's a RealEstateListing directly
                        if entry.get('@type') == 'RealEstateListing':
                            url = entry.get('url') or entry.get('@id')
                            if url and url not in seen:
                                seen.add(url)
                                urls.append(url)

    df = pd.DataFrame(urls, columns=['URL'])
    os.makedirs(project_folder, exist_ok=True)
    outpath = os.path.join(project_folder, '0_URLs_from_saved.csv')
    df.to_csv(outpath, index=False)
    print(f"Extracted {len(urls)} URLs to {outpath}")
    return df


if __name__ == '__main__':
    extract_urls_from_saved_html('data/dnbeiendom')

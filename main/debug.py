from bs4 import BeautifulSoup
from parsing_helpers_jobs import JobParser

with open('../jobbe/html_extracted/436826659.html', 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

parser = JobParser(soup)
company = parser.get_company()
job_title = parser.get_job_title()
deadline = parser.get_deadline()
ad_text = parser.get_ad_text()

print(f"Company: {company}")
print(f"Job Title: {job_title}")
print(f"Deadline: {deadline}")
print(f"\nAd Text:\n{ad_text[:200] if ad_text else 'Not found'}...")
print(f"\nStarts with 'Eiendomsverdi er mer enn'?: {ad_text.startswith('Eiendomsverdi er mer enn') if ad_text else False}")
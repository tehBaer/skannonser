from bs4 import BeautifulSoup
from parsing_helpers_jobs import JobParser

with open('jobbe/html_extracted/436826659.html', 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

parser = JobParser(soup)
company = parser.get_company()
job_title = parser.get_job_title()
deadline = parser.get_deadline()
text_body = parser.get_text_body()

print(f"Company: {company}")
print(f"Job Title: {job_title}")
print(f"Deadline: {deadline}")
print(f"\n--- Text Body ---")
print(text_body)
print(f"--- End Text Body ---")
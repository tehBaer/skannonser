# from bs4 import BeautifulSoup
# from parsing_helpers_jobs import JobParser
#
# with open('jobbe/html_extracted/436826659.html', 'r', encoding='utf-8') as f:
#     html_content = f.read()
#
# soup = BeautifulSoup(html_content, 'html.parser')
#
# parser = JobParser(soup)
# company = parser.get_company()
# job_title = parser.get_job_title()
# deadline = parser.get_deadline()
#
# print(f"Company: {company}")
# print(f"Job Title: {job_title}")
# print(f"Deadline: {deadline}")


import pandas as pd
from main.post_process import post_process_jobs
data = pd.read_csv('jobbe/A_live.csv')
post_process_jobs(data, "jobbe", "AB_processed.csv")
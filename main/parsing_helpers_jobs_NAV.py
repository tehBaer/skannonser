import re
import json


class NAVParser:
    def __init__(self, soup):
        self.soup = soup



    def get_ad_title(self):
        """Extract job ad title from page title."""
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.get_text()
            # Remove " - arbeidsplassen.no" suffix if present
            if ' - arbeidsplassen.no' in title:
                title = title.split(' - arbeidsplassen.no')[0]
            return title.strip()
        return None

    def get_company(self) -> str:
        company_p = self.soup.select_one('p.navds-body-long.navds-typo--semibold')
        if company_p:
            return company_p.get_text(strip=True)
        return "Unknown"

    def get_job_title(self):
        """Extract job title from the page."""
        # Find the dt element containing "Stillingstittel"
        dt_element = self.soup.find('dt', class_='navds-label', string='Stillingstittel')

        # Get the next dd sibling element
        if dt_element:
            dd_element = dt_element.find_next_sibling('dd')
            if dd_element:
                return dd_element.get_text(strip=True)
        return None

    def get_job_positions(self):
        """Extract number of positions from the page."""
        # Find the dt element containing "Antall stillinger"
        dt_element = self.soup.find('dt', class_='navds-label', string='Antall stillinger')

        # Get the next dd sibling element
        if dt_element:
            dd_element = dt_element.find_next_sibling('dd')
            if dd_element:
                return dd_element.get_text(strip=True)
        return None

    def get_deadline(self) -> str:
        """Extract application deadline from the page."""
        # Look for the deadline element
        deadline_div = self.soup.find('div', class_='navds-stack flex-shrink-0 navds-vstack navds-stack-direction')

        if deadline_div:
            # Get the text content and clean it
            deadline_text = deadline_div.get_text(strip=True)

            # Remove "Søk på jobben" if present
            deadline_text = deadline_text.replace('Søk på jobben', '').strip()

            # Remove everything before the first digit
            match = re.search(r'\d', deadline_text)
            if match:
                deadline_text = deadline_text[match.start():]

            return deadline_text

        return "Not specified"

    def get_textcontent(self) -> str:
        try:
            content_div = self.soup.find('div', class_='arb-rich-text job-posting-text')
            if content_div:
                return content_div.get_text(separator=' ', strip=True)
            return ''
        except Exception as e:
            print(f"Error extracting text content: {e}")
            return ''
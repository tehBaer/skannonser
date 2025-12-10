import re
import json


class JobParser:
    """Parser for extracting job ad data from FINN.no HTML."""

    def __init__(self, soup):
        self.soup = soup
        self._targeting_data = None

    def _get_targeting_data(self):
        """Cache and return the targeting array from advertising JSON."""
        if self._targeting_data is None:
            script_tag = self.soup.find('script', {'id': 'advertising-initial-state', 'type': 'application/json'})
            if script_tag:
                try:
                    data = json.loads(script_tag.string)
                    self._targeting_data = data.get('config', {}).get('adServer', {}).get('gam', {}).get('targeting',
                                                                                                         [])
                except (json.JSONDecodeError, KeyError):
                    self._targeting_data = []
            else:
                self._targeting_data = []
        return self._targeting_data

    def _get_targeting_value(self, key):
        """Get value from targeting data by key."""
        targeting = self._get_targeting_data()
        for item in targeting:
            if item.get('key') == key and item.get('value'):
                return item['value']
        return None

    def get_company(self):
        """Extract company name."""
        value = self._get_targeting_value('company_name')
        if value:
            return value[0]

        # Fallback
        logo_img = self.soup.find('img', src=re.compile(r'finncdn\.no/mmo/logo'))
        if logo_img and logo_img.get('alt'):
            return logo_img.get('alt').strip()
        return None

    def get_job_title(self):
        """Extract job title."""
        value = self._get_targeting_value('job_title')
        return value[0] if value else None

    def get_occupation(self):
        """Extract occupation codes."""
        return self._get_targeting_value('occupation')

    def get_job_positions(self):
        """Extract number of job positions."""
        value = self._get_targeting_value('job_positions')
        return value[0] if value else None

    def get_industry(self):
        """Extract industry."""
        value = self._get_targeting_value('industry')
        return value[0] if value else None

    def get_ad_title(self):
        """Extract job ad title from page title."""
        title_tag = self.soup.find('title')
        if title_tag:
            title = title_tag.get_text()
            if ' | FINN.no' in title:
                title = title.split(' | FINN.no')[0]
            return title.strip()
        return None

    def get_deadline(self):
        """Extract application deadline."""
        list_items = self.soup.find_all('li')
        for li in list_items:
            text = li.get_text()
            if 'Frist' in text:
                # Try to extract date first
                date_match = re.search(r'\d{2}\.\d{2}\.\d{4}', text)
                if date_match:
                    return date_match.group(0)

                # If no date, extract text after "Frist"
                # Remove "Frist" and any leading/trailing whitespace
                deadline_text = text.replace('Frist', '').strip()
                if deadline_text:
                    return deadline_text
        return None

    def get_textcontent(self):
        """Extract the main text content of the job ad."""
        main_content = self.soup.find('article') or self.soup.find('main') or self.soup.find('div',
                                                                                             {'id': 'main-content'})

        if main_content:
            text = main_content.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()

            # Split after the word following "Ansettelsesform"
            match = re.search(r'Ansettelsesform\s+\S+', text)
            if match:
                split_pos = match.end()
                part2 = text[split_pos:].strip()

                # Split part2 before "Vis hele beskrivelsen"
                if 'Vis hele beskrivelsen' in part2:
                    part2 = part2.split('Vis hele beskrivelsen')[0].strip()

                return part2  # Returns single string, not tuple

            return text

        return None
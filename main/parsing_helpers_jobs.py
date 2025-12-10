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

    def get_text_body(self):
        """Extract the main text body/description of the job ad."""
        # Look for common containers that hold job description text
        # FINN.no typically uses specific div classes for the main content
        
        # Try to find the main article or content section
        main_content = self.soup.find('article')
        if main_content:
            # Get all text, removing extra whitespace
            text = main_content.get_text(separator='\n', strip=True)
            # Clean up multiple newlines
            text = re.sub(r'\n\s*\n', '\n\n', text)
            return text.strip()
        
        # Fallback: try to find div with data-testid or common class patterns
        content_div = self.soup.find('div', {'data-testid': 'description'})
        if content_div:
            text = content_div.get_text(separator='\n', strip=True)
            text = re.sub(r'\n\s*\n', '\n\n', text)
            return text.strip()
        
        return None
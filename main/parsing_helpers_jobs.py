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

    def get_ad_text(self):
        """Extract main job ad text content."""
        # Look for the main job description container
        # Common containers for FINN.no job ads
        selectors = [
            {'data-automation-id': 'import-decoration'},
            {'class': 'import-decoration'},
            {'class': 'job-description'}
        ]
        
        for selector in selectors:
            # Search across all element types, not just divs
            container = self.soup.find(attrs=selector)
            if container:
                # Get all text from the container, stripping extra whitespace
                text = container.get_text(separator=' ', strip=True)
                # Clean up multiple spaces
                text = ' '.join(text.split())
                return text
        
        return None
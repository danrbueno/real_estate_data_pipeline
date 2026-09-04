"""HTTP Client for fetching web pages"""

import requests
from typing import Optional
import time
from config import REQUEST_TIMEOUT, REQUEST_DELAY


class HTTPClient:
    """Client for making HTTP requests to web pages"""

    def __init__(self, delay: float = REQUEST_DELAY):
        self.delay = delay
        self.last_request_time = 0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def get(self, url: str) -> Optional[str]:
        """
        Fetch content from URL with delay between requests
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content or None if request fails
        """
        # Rate limiting
        time_since_last_request = time.time() - self.last_request_time
        if time_since_last_request < self.delay:
            time.sleep(self.delay - time_since_last_request)

        try:
            self.last_request_time = time.time()
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def close(self):
        """Close the session"""
        self.session.close()

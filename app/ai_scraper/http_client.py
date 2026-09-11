"""Standard-library HTTP client for fetching web pages."""

import time
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from config import REQUEST_TIMEOUT, REQUEST_DELAY


class HTTPClient:
    """Client for making HTTP requests to web pages"""

    def __init__(self, delay: float = REQUEST_DELAY):
        self.delay = delay
        self.last_request_time = 0
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

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
            request = Request(url, headers={"User-Agent": self.user_agent})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read().decode(response.headers.get_content_charset() or "utf-8")
        except (HTTPError, URLError, TimeoutError, UnicodeDecodeError) as e:
            print(f"Error fetching {url}: {e}")
            return None

    def close(self):
        """Release HTTP resources."""

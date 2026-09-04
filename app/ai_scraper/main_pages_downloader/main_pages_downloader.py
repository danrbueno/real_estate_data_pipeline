"""Orchestrator for downloading paginated listing pages as HTML."""

import os
import re
from pathlib import Path
from typing import List

from config import DFIMOVEIS_SEARCH_URL, MAX_PAGES, RAW_DATA_DIR, TRANSACTION_TYPES
from http_client import HTTPClient


class AIScraper:
    """Web scraper that saves pagination pages as HTML."""

    def __init__(self):
        self.http_client = HTTPClient()
        self.transaction_type = None
        self.raw_data_dir = None

    @staticmethod
    def count_properties_in_html(html: str) -> int:
        """Count unique property IDs in HTML by finding data-id attributes."""
        pattern = r'data-id="(\d+)"'
        matches = re.findall(pattern, html)
        return len(set(matches))

    def save_page_html(self, page: int, html: str) -> Path:
        """Save page HTML to file."""
        output_dir = Path(self.raw_data_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"page_{page:03d}.html"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        return file_path

    def scrape_transaction_type(self, transaction_type: str) -> List[str]:
        """Scrape and save all pagination pages for a transaction type."""
        self.transaction_type = transaction_type
        url_type = TRANSACTION_TYPES[transaction_type]
        self.raw_data_dir = os.path.join(RAW_DATA_DIR, transaction_type, "pages")

        saved_pages = []
        current_page = 1
        total_properties = 0
        print(f"\n🤖 Starting AI scraping for {transaction_type}...")
        print(f"📁 Output directory: {self.raw_data_dir}\n")

        while True:
            if MAX_PAGES and current_page > MAX_PAGES:
                print(f"⏹️  Reached max_pages limit: {MAX_PAGES}")
                break

            url = DFIMOVEIS_SEARCH_URL.format(url_type, current_page)
            print(f"📄 Page {current_page}: Fetching...", end=" ", flush=True)
            html = self.http_client.get(url)
            if not html:
                print("❌ Fetch failed")
                break

            file_path = self.save_page_html(current_page, html)
            saved_pages.append(str(file_path))
            property_count = self.count_properties_in_html(html)
            print(f"Saved → {property_count} properties")

            if property_count == 0:
                print("\n✅ Reached end of pagination")
                print(f"   Page {current_page} has NO properties")
                break

            total_properties += property_count
            current_page += 1

        print(f"\n{'='*60}")
        print("📊 Scraping Summary:")
        print(f"  Total pages fetched: {current_page - 1}")
        print(f"  Total properties found: {total_properties}")
        print(f"  Output directory: {self.raw_data_dir}")
        print(f"  Pages saved: {len(saved_pages)}")
        print(f"{'='*60}\n")
        return saved_pages

    def close(self):
        """Clean up resources."""
        self.http_client.close()
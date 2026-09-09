"""Orchestrator for downloading paginated listing pages into Postgres."""

import re
from typing import List

from config import DFIMOVEIS_SEARCH_URL, MAX_PAGES, TRANSACTION_TYPES
from db import Base, engine, Session
from http_client import HTTPClient
from models import MainPage


class AIScraper:
    """Web scraper that saves pagination pages to the tb_main_pages table."""

    def __init__(self):
        self.http_client = HTTPClient()
        self.transaction_type = None
        # Reset tb_main_pages on every run so it only holds the latest scrape.
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    @staticmethod
    def count_properties_in_html(html: str) -> int:
        """Count unique property IDs in HTML by finding data-id attributes."""
        pattern = r'data-id="(\d+)"'
        matches = re.findall(pattern, html)
        return len(set(matches))

    def save_page_html(self, page: int, url: str, html: str) -> int:
        """Save page HTML to the tb_main_pages table, returning the row id."""
        session = Session()
        try:
            record = MainPage(page=page, url=url, html_content=html)
            session.add(record)
            session.commit()
            return record.id
        finally:
            session.close()

    def scrape_transaction_type(self, transaction_type: str) -> List[int]:
        """Scrape and save all pagination pages for a transaction type."""
        self.transaction_type = transaction_type
        url_type = TRANSACTION_TYPES[transaction_type]

        saved_pages = []
        current_page = 1
        total_properties = 0
        print(f"\n🤖 Starting AI scraping for {transaction_type}...")
        print("🗄️  Saving pages to Postgres table: tb_main_pages\n")

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

            row_id = self.save_page_html(current_page, url, html)
            saved_pages.append(row_id)
            property_count = self.count_properties_in_html(html)
            print(f"Saved (id={row_id}) → {property_count} properties")

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
        print("  Table: tb_main_pages")
        print(f"  Pages saved: {len(saved_pages)}")
        print(f"{'='*60}\n")
        return saved_pages

    def close(self):
        """Clean up resources."""
        self.http_client.close()

"""Orchestrator for downloading paginated listing pages and extracting ad links."""

import json
import re
from pathlib import Path
from typing import Dict, List

from config import DFIMOVEIS_BASE_URL, DFIMOVEIS_SEARCH_URL, MAX_PAGES, RAW_DATA_DIR, TRANSACTION_TYPES
from http_client import HTTPClient

LINK_PATTERN = re.compile(
    r'(?:href=["\']?)?((?:https?://[^"\'\s>]+)?/imovel/[A-Za-z0-9][^"\'\s>)]*)',
    re.IGNORECASE,
)


class AIScraper:
    """Web scraper that extracts ad links from paginated listing pages, without saving HTML."""

    def __init__(self, http_client=None):
        self.http_client = http_client or HTTPClient()
        self.transaction_type = None

    @staticmethod
    def extract_property_links(html: str, base_url: str = DFIMOVEIS_BASE_URL) -> List[str]:
        """Extract unique ad links from a listing page's HTML using a regex (no AI call)."""
        links = []
        seen = set()
        base = base_url.rstrip("/")
        for raw_link in LINK_PATTERN.findall(html):
            cleaned = raw_link.rstrip(".,;:")
            full_url = cleaned if cleaned.startswith("http") else f"{base}{cleaned}"
            if full_url not in seen:
                seen.add(full_url)
                links.append(full_url)
        return links

    @staticmethod
    def links_output_path(transaction_type: str) -> Path:
        """Path of the JSON file where a transaction type's page links are saved."""
        return Path(RAW_DATA_DIR) / transaction_type / "links.json"

    def save_links(self, transaction_type: str, pages: List[Dict[str, object]]) -> Path:
        """Save the per-page ad links as a single JSON file, returning its path."""
        output_path = self.links_output_path(transaction_type)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(pages, ensure_ascii=False, indent=2), encoding="utf-8")
        return output_path

    def scrape_transaction_type(self, transaction_type: str) -> List[Dict[str, object]]:
        """Fetch every pagination page's HTML and extract its ad links, saving them as JSON."""
        self.transaction_type = transaction_type
        url_type = TRANSACTION_TYPES[transaction_type]

        pages: List[Dict[str, object]] = []
        current_page = 1
        total_links = 0
        print(f"\n🤖 Starting scraping for {transaction_type}...")

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

            links = self.extract_property_links(html)
            print(f"→ {len(links)} links")

            if not links:
                print("\n✅ Reached end of pagination")
                print(f"   Page {current_page} has NO properties")
                break

            pages.append({"page": current_page, "links": links})
            total_links += len(links)
            current_page += 1

        output_path = self.save_links(transaction_type, pages)

        print(f"\n{'='*60}")
        print("📊 Scraping Summary:")
        print(f"  Total pages fetched: {len(pages)}")
        print(f"  Total links found: {total_links}")
        print(f"  Output file: {output_path}")
        print(f"{'='*60}\n")
        return pages

    def close(self):
        """Clean up resources."""
        self.http_client.close()

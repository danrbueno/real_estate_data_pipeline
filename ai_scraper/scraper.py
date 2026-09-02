"""Orchestrator for AI-based web scraping - saves pagination pages as HTML"""

import os
import re
from typing import List
from pathlib import Path
from config import (
    DFIMOVEIS_SEARCH_URL,
    TRANSACTION_TYPES,
    OUTPUT_DIR,
    MAX_PAGES
)
from http_client import HTTPClient


class AIScraper:
    """AI-based web scraper that saves pagination pages as HTML"""

    def __init__(self):
        self.http_client = HTTPClient()
        self.transaction_type = None
        self.output_dir = None

    @staticmethod
    def count_properties_in_html(html: str) -> int:
        """Count unique property IDs in HTML by finding data-id attributes"""
        pattern = r'data-id="(\d+)"'
        matches = re.findall(pattern, html)
        # Remove duplicates and return count
        return len(set(matches))

    def save_page_html(self, page: int, html: str) -> Path:
        """Save page HTML to file"""
        output_dir = Path(self.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = output_dir / f"page_{page:03d}.html"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        return file_path

    def scrape_transaction_type(self, transaction_type: str) -> List[str]:
        """
        Scrape and save all pagination pages for a transaction type
        
        Args:
            transaction_type: "rentals" or "sales"
            
        Returns:
            List of saved file paths
        """
        self.transaction_type = transaction_type
        url_type = TRANSACTION_TYPES[transaction_type]
        
        # Set output directory
        self.output_dir = os.path.join(OUTPUT_DIR, transaction_type)
        
        saved_files = []
        current_page = 1
        total_properties = 0

        print(f"\n🤖 Starting AI scraping for {transaction_type}...")
        print(f"📁 Output directory: {self.output_dir}\n")

        while True:
            if MAX_PAGES and current_page > MAX_PAGES:
                print(f"⏹️  Reached max_pages limit: {MAX_PAGES}")
                break

            url = DFIMOVEIS_SEARCH_URL.format(url_type, current_page)
            print(f"📄 Page {current_page}: Fetching...", end=" ", flush=True)
            
            # Fetch page HTML
            html = self.http_client.get(url)
            if not html:
                print("❌ Fetch failed")
                break

            # Save HTML
            file_path = self.save_page_html(current_page, html)
            saved_files.append(str(file_path))
            
            # Count properties in page
            property_count = self.count_properties_in_html(html)
            print(f"Saved → {property_count} properties")
            
            # Check if page is empty (end of pagination)
            if property_count == 0:
                print(f"\n✅ Reached end of pagination")
                print(f"   Page {current_page} has NO properties")
                break

            total_properties += property_count
            current_page += 1

        # Summary
        print(f"\n{'='*60}")
        print(f"📊 Scraping Summary:")
        print(f"  Total pages fetched: {current_page - 1}")
        print(f"  Total properties found: {total_properties}")
        print(f"  Output directory: {self.output_dir}")
        print(f"  Files saved: {len(saved_files)}")
        print(f"{'='*60}\n")
        
        return saved_files

    def close(self):
        """Clean up resources"""
        self.http_client.close()

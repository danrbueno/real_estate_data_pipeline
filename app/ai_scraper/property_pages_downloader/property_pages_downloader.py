"""Download property detail pages linked from saved listing pages."""

import hashlib
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

from config import DFIMOVEIS_BASE_URL, RAW_DATA_DIR
from http_client import HTTPClient

LINK_PATTERN = re.compile(r'href="(/imovel/[^"]+)"')


class PropertyPagesDownloader:
    """Download each ad page found on saved listing pages."""

    def __init__(self, http_client: Optional[HTTPClient] = None):
        self.http_client = http_client or HTTPClient()

    @staticmethod
    def extract_property_links(html: str, base_url: str = DFIMOVEIS_BASE_URL) -> List[str]:
        """Extract unique ad links found on a listing page."""
        links = []
        seen = set()
        for href in LINK_PATTERN.findall(html):
            full_url = href if href.startswith("http") else f"{base_url}{href}"
            if full_url not in seen:
                seen.add(full_url)
                links.append(full_url)
        return links

    @staticmethod
    def _property_filename(link: str) -> str:
        parsed = urlparse(link)
        slug = re.sub(
            r"[^a-zA-Z0-9_-]+", "-", parsed.path.rstrip("/").split("/")[-1]
        ).strip("-")
        slug = slug or "property"
        digest = hashlib.sha256(link.encode("utf-8")).hexdigest()[:12]
        return f"{slug}-{digest}.html"

    def extract_property(self, link: str, output_dir: Optional[Path] = None) -> Optional[Path]:
        """Download and save a single ad page, returning its local path."""
        html = self.http_client.get(link)
        if not html:
            return None

        destination = output_dir or Path(RAW_DATA_DIR) / "properties"
        destination.mkdir(parents=True, exist_ok=True)
        output_path = destination / self._property_filename(link)
        output_path.write_text(html, encoding="utf-8")
        return output_path

    def extract_properties_from_page(
        self, html: str, base_url: str = DFIMOVEIS_BASE_URL,
        output_dir: Optional[Path] = None,
    ) -> List[Path]:
        """Download every ad linked from a single saved listing page."""
        properties = []
        for link in self.extract_property_links(html, base_url):
            property_path = self.extract_property(link, output_dir)
            if property_path:
                properties.append(property_path)
        return properties

    @staticmethod
    def _load_page_paths(transaction_type: str, max_pages: Optional[int] = None) -> List[Path]:
        pages_dir = Path(RAW_DATA_DIR) / transaction_type / "pages"
        paths = sorted(pages_dir.glob("page_*.html"))
        if max_pages:
            paths = paths[:max_pages]
        return paths

    def extract_transaction_type(
        self, transaction_type: str, max_pages: Optional[int] = None
    ) -> List[Path]:
        """Read saved listing pages and download their linked property pages."""
        page_paths = self._load_page_paths(transaction_type, max_pages)
        print(
            f"\n🔎 Downloading {transaction_type} property pages from "
            f"{len(page_paths)} saved page(s)..."
        )

        output_dir = Path(RAW_DATA_DIR) / transaction_type / "properties"
        all_properties: List[Path] = []
        for page_path in page_paths:
            html = page_path.read_text(encoding="utf-8")
            page_properties = self.extract_properties_from_page(html, output_dir=output_dir)
            print(f"📄 {page_path.name}: {len(page_properties)} propert(y/ies) downloaded")
            all_properties.extend(page_properties)

        print(f"\n{'='*60}")
        print("📊 Download Summary:")
        print(f"  Pages processed: {len(page_paths)}")
        print(f"  Property pages downloaded: {len(all_properties)}")
        print(f"  Output directory: {output_dir}")
        print(f"{'='*60}\n")
        return all_properties

    def close(self):
        """Clean up resources."""
        self.http_client.close()
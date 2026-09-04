"""AI Scraper module - OpenAI-powered web scraping agents"""

import sys
from pathlib import Path

# Shared modules use simple bare imports, so this folder must be on sys.path
# before the downloader packages are imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.ai_scraper.main_pages_downloader.main_pages_downloader import AIScraper
from app.ai_scraper.ai_agent import AIScrapingAgent
from app.ai_scraper.property_pages_downloader.property_pages_downloader import PropertyPagesDownloader
from app.ai_scraper.http_client import HTTPClient
from app.ai_scraper.config import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    TRANSACTION_TYPES,
)

__version__ = "1.0.0"
__all__ = [
    "AIScraper",
    "AIScrapingAgent",
    "PropertyPagesDownloader",
    "HTTPClient",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "TRANSACTION_TYPES",
]

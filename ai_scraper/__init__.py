"""AI Scraper module - OpenAI-powered web scraping agents"""

from ai_scraper.scraper import AIScraper
from ai_scraper.ai_agent import AIScrapingAgent
from ai_scraper.http_client import HTTPClient
from ai_scraper.config import (
    OPENAI_API_KEY,
    OPENAI_MODEL,
    TRANSACTION_TYPES,
)

__version__ = "1.0.0"
__all__ = [
    "AIScraper",
    "AIScrapingAgent",
    "HTTPClient",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "TRANSACTION_TYPES",
]

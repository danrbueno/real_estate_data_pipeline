"""AI Scraper module - OpenAI-powered web scraping agents"""

from app.ai_scraper.scraper import AIScraper
from app.ai_scraper.ai_agent import AIScrapingAgent
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
    "HTTPClient",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "TRANSACTION_TYPES",
]

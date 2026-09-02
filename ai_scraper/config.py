"""Configurações para o AI Scraper"""

import os
from pathlib import Path

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Look for .env in parent directory (project root)
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    # If python-dotenv is not installed, just skip
    pass

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4-turbo")

# DFImoveis Configuration
DFIMOVEIS_BASE_URL = "https://www.dfimoveis.com.br"
DFIMOVEIS_SEARCH_URL = "https://www.dfimoveis.com.br/{}/df/todos/apartamento?pagina={}"

# Transaction types mapping
TRANSACTION_TYPES = {
    "sales": "venda",
    "rentals": "aluguel"
}

# Output paths
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "web")

# Request timeout
REQUEST_TIMEOUT = 30

# Max pages to scrape (None = all)
MAX_PAGES = None

# Delay between requests (seconds)
REQUEST_DELAY = 2

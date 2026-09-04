"""Configurações para o AI Scraper"""

import os
from pathlib import Path

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Look for .env in config/, then app/, then project root, then let dotenv search upward
    _project_root = Path(__file__).parent.parent.parent
    _candidate_paths = [
        _project_root / "config" / ".env",
        Path(__file__).parent.parent / ".env",
        _project_root / ".env",
    ]
    _env_path = next((path for path in _candidate_paths if path.exists()), None)
    if _env_path:
        load_dotenv(_env_path)
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
RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "properties")

# Request timeout
REQUEST_TIMEOUT = 30

# Max pages to scrape (None = all)
MAX_PAGES = None

# Delay between requests (seconds)
REQUEST_DELAY = 2

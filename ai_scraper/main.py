"""
AI web scraping script using OpenAI agents
Usage: python main.py --type rentals|sales
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from scraper import AIScraper


def main():
    parser = argparse.ArgumentParser(
        description="AI-powered web scraper for DFImoveis real estate data"
    )
    parser.add_argument(
        "--type",
        "-t",
        choices=["rentals", "sales"],
        default="sales",
        help="Transaction type to scrape (default: sales)"
    )
    parser.add_argument(
        "--max-pages",
        "-m",
        type=int,
        default=None,
        help="Maximum number of pages to scrape (default: all)"
    )

    args = parser.parse_args()

    try:
        scraper = AIScraper()
        properties = scraper.scrape_transaction_type(args.type)
        scraper.close()

        print(f"\n✅ Scraping {args.type} completed successfully!")        
        return 0

    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

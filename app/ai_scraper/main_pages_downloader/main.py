"""Download paginated listing pages.

Usage: python -m app.ai_scraper.main_pages_downloader.main --type rentals|sales
"""

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    _ai_scraper_dir = Path(__file__).resolve().parents[1]
    _project_root = _ai_scraper_dir.parents[1]
    sys.path.insert(0, str(_project_root))
    sys.path.insert(0, str(_ai_scraper_dir))

from app.ai_scraper.main_pages_downloader.main_pages_downloader import AIScraper


def main():
    parser = argparse.ArgumentParser(
        description="Download paginated DFImoveis listing pages"
    )
    parser.add_argument(
        "--type", "-t", choices=["rentals", "sales"], default="sales",
        help="Transaction type to scrape (default: sales)"
    )
    parser.add_argument(
        "--max-pages", "-m", type=int, default=None,
        help="Maximum number of pages to scrape (default: all)"
    )

    args = parser.parse_args()

    try:
        scraper = AIScraper()
        scraper.scrape_transaction_type(args.type)
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
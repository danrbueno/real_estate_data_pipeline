"""Download paginated listing pages.

Usage: python -m app.ai_scraper.ad_links_collector.main --type rentals|sales
"""

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    _ai_scraper_dir = Path(__file__).resolve().parents[1]
    _project_root = _ai_scraper_dir.parents[1]
    sys.path.insert(0, str(_project_root))
    sys.path.insert(0, str(_ai_scraper_dir))

from app.ai_scraper.ad_links_collector.ad_links_collector import AdLinksCollector


def main():
    parser = argparse.ArgumentParser(
        description="Download paginated DFImoveis listing pages using AdLinksCollector"
    )
    parser.add_argument(
        "--type", "-t", choices=["rentals", "sales"], default="sales",
        help="Transaction type to scrape (default: sales)"
    )

    args = parser.parse_args()

    try:
        ad_links_collector = AdLinksCollector()
        ad_links_collector.collect(args.type)
        ad_links_collector.close()
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
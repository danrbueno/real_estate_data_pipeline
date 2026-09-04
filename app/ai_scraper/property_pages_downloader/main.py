"""Download property detail HTML pages from saved listing pages.

Usage: python -m app.ai_scraper.property_pages_downloader.main --type rentals|sales
"""

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    _ai_scraper_dir = Path(__file__).resolve().parents[1]
    _project_root = _ai_scraper_dir.parents[1]
    sys.path.insert(0, str(_project_root))
    sys.path.insert(0, str(_ai_scraper_dir))

from app.ai_scraper.property_pages_downloader.property_pages_downloader import PropertyPagesDownloader


def main():
    parser = argparse.ArgumentParser(
        description="Download DFImoveis property pages linked from saved listings"
    )
    parser.add_argument(
        "--type", "-t", choices=["rentals", "sales"], default="sales",
        help="Transaction type to download (default: sales)"
    )
    parser.add_argument(
        "--max-pages", "-m", type=int, default=None,
        help="Maximum number of saved listing pages to process (default: all)"
    )

    args = parser.parse_args()

    try:
        extractor = PropertyPagesDownloader()
        extractor.extract_transaction_type(args.type, args.max_pages)
        extractor.close()
        print(f"\n✅ Download of {args.type} property pages completed successfully!")
        return 0
    except KeyboardInterrupt:
        print("\n⚠️  Download interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Error during download: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
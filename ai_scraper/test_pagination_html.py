"""
Test script to fetch and save all pagination pages as HTML
"""

import sys
from pathlib import Path
import json

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import DFIMOVEIS_SEARCH_URL, TRANSACTION_TYPES, OUTPUT_DIR
from http_client import HTTPClient
from ai_agent import AIScrapingAgent


def save_page_html(transaction_type: str, page: int, html: str):
    """Save page HTML to file"""
    output_dir = Path(OUTPUT_DIR) / "raw_pages" / transaction_type
    output_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = output_dir / f"page_{page:03d}.html"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print(f"  💾 Saved: {file_path}")
    return file_path


def test_pagination_fetch(transaction_type: str = "rentals", max_pages: int = None):
    """Fetch all pages and save HTML"""
    
    if transaction_type not in TRANSACTION_TYPES:
        print(f"❌ Invalid transaction type: {transaction_type}")
        return
    
    tx_type_key = TRANSACTION_TYPES[transaction_type]
    print(f"🤖 Testing pagination fetch for: {transaction_type} ({tx_type_key})")
    print(f"📁 Output directory: {Path(OUTPUT_DIR) / 'raw_pages' / transaction_type}\n")
    
    http_client = HTTPClient()
    ai_agent = AIScrapingAgent()
    
    page = 1
    total_properties_in_pages = 0
    consecutive_empty_pages = 0
    
    try:
        while True:
            # Build URL
            url = DFIMOVEIS_SEARCH_URL.format(tx_type_key, page)
            print(f"📄 Page {page}: Fetching {url}")
            
            # Fetch HTML
            html = http_client.get(url)
            if not html:
                print(f"  ❌ Failed to fetch page {page}, stopping")
                break
            
            # Save HTML
            save_page_html(transaction_type, page, html)
            
            # Extract pagination info to know if there are more pages
            pagination_info = ai_agent.extract_pagination_info(html)
            
            if "error" in pagination_info:
                print(f"  ⚠️  Error extracting pagination: {pagination_info['error']}")
                break
            
            print(f"  📊 Page info: {pagination_info}")
            
            # Check if page is empty
            if pagination_info.get("page_is_empty", False):
                print(f"  ⚠️  Page is empty, stopping pagination")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    print(f"\n✅ Reached end of pagination (consecutive empty pages)")
                    break
            else:
                consecutive_empty_pages = 0
            
            # Count properties in this page
            if "total_in_page" in pagination_info:
                properties_in_page = pagination_info["total_in_page"]
                total_properties_in_pages += properties_in_page
                print(f"  🏠 Properties in this page: {properties_in_page}")
            
            # Check if there are more pages
            has_next = pagination_info.get("has_next_page", False)
            
            if max_pages and page >= max_pages:
                print(f"\n⏹️  Stopped at max_pages limit: {max_pages}")
                break
            
            if not has_next:
                print(f"\n✅ Reached last page (no more next pages)")
                break
            
            page += 1
            print()
        
        # Summary
        print(f"\n{'='*60}")
        print(f"📊 Summary:")
        print(f"  Total pages fetched: {page}")
        print(f"  Total properties found: {total_properties_in_pages}")
        print(f"  Output directory: {Path(OUTPUT_DIR) / 'raw_pages' / transaction_type}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        http_client.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test pagination fetch - saves all page HTMLs"
    )
    parser.add_argument(
        "--type",
        "-t",
        choices=["rentals", "sales"],
        default="rentals",
        help="Transaction type (default: rentals)"
    )
    parser.add_argument(
        "--max-pages",
        "-m",
        type=int,
        default=None,
        help="Maximum pages to fetch"
    )
    
    args = parser.parse_args()
    test_pagination_fetch(args.type, args.max_pages)

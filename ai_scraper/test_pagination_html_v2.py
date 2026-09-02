"""
Test script to fetch and save all pagination pages as HTML
Uses direct data-id counting for accurate property count
"""

import sys
import re
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import DFIMOVEIS_SEARCH_URL, TRANSACTION_TYPES, OUTPUT_DIR
from http_client import HTTPClient
from ai_agent import AIScrapingAgent


def count_properties_in_html(html: str) -> int:
    """Count unique property IDs in HTML"""
    pattern = r'data-id="(\d+)"'
    matches = re.findall(pattern, html)
    return len(set(matches))


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
    """Fetch all pages and save HTML - stops when page is empty"""
    
    if transaction_type not in TRANSACTION_TYPES:
        print(f"❌ Invalid transaction type: {transaction_type}")
        return
    
    tx_type_key = TRANSACTION_TYPES[transaction_type]
    print(f"🤖 Fetching all pagination pages for: {transaction_type} ({tx_type_key})")
    print(f"📁 Output directory: {Path(OUTPUT_DIR) / 'raw_pages' / transaction_type}\n")
    
    http_client = HTTPClient()
    
    page = 1
    total_properties = 0
    
    try:
        while True:
            # Build URL
            url = DFIMOVEIS_SEARCH_URL.format(tx_type_key, page)
            print(f"📄 Page {page}: Fetching...", end=" ", flush=True)
            
            # Fetch HTML
            html = http_client.get(url)
            if not html:
                print("❌ Fetch failed")
                break
            
            # Save HTML
            save_page_html(transaction_type, page, html)
            
            # Count properties directly
            property_count = count_properties_in_html(html)
            print(f"Found {property_count} properties")
            
            # Check if page is empty - stop pagination
            if property_count == 0:
                print(f"\n✅ Reached end of pagination")
                print(f"   Page {page} has NO properties - this is the end")
                break
            
            total_properties += property_count
            
            # Stop if reached max_pages limit
            if max_pages and page >= max_pages:
                print(f"\n⏹️  Reached max_pages limit: {max_pages}")
                break
            
            page += 1
        
        # Summary
        print(f"\n{'='*60}")
        print(f"📊 Summary:")
        print(f"  Total pages fetched: {page - 1}")
        print(f"  Last page with data: {page - 1}")
        print(f"  Total properties found: {total_properties}")
        print(f"  Output directory: {Path(OUTPUT_DIR) / 'raw_pages' / transaction_type}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        http_client.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch and save all pagination pages as HTML files"
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

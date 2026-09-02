"""
Test script to find the exact last page by testing specific pages
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import DFIMOVEIS_SEARCH_URL, TRANSACTION_TYPES, OUTPUT_DIR
from http_client import HTTPClient
from ai_agent import AIScrapingAgent


def test_specific_pages(transaction_type: str = "rentals", pages_to_test: list = None):
    """Test specific page numbers to find pagination limits"""
    
    if transaction_type not in TRANSACTION_TYPES:
        print(f"❌ Invalid transaction type: {transaction_type}")
        return
    
    if pages_to_test is None:
        pages_to_test = [1, 10, 50, 80, 85, 86, 87, 88, 89, 90]
    
    tx_type_key = TRANSACTION_TYPES[transaction_type]
    print(f"🤖 Testing specific pages for: {transaction_type} ({tx_type_key})\n")
    
    http_client = HTTPClient()
    ai_agent = AIScrapingAgent()
    
    results = []
    
    try:
        for page in pages_to_test:
            url = DFIMOVEIS_SEARCH_URL.format(tx_type_key, page)
            print(f"📄 Testing page {page}: {url}")
            
            # Fetch HTML
            html = http_client.get(url)
            if not html:
                print(f"  ❌ Failed to fetch page {page}\n")
                results.append({
                    "page": page,
                    "status": "fetch_error",
                    "has_next": None,
                    "items": None
                })
                continue
            
            # Extract pagination info
            pagination_info = ai_agent.extract_pagination_info(html)
            
            if "error" in pagination_info:
                print(f"  ⚠️  Error extracting pagination: {pagination_info['error']}\n")
                results.append({
                    "page": page,
                    "status": "extraction_error",
                    "has_next": None,
                    "items": None
                })
                continue
            
            has_next = pagination_info.get("has_next_page", False)
            is_empty = pagination_info.get("page_is_empty", False)
            items_count = pagination_info.get("total_in_page", 0)
            
            status = "✅ OK"
            if is_empty:
                status = "⚠️  EMPTY"
            if not has_next:
                status = "🏁 LAST PAGE"
            
            print(f"  {status}")
            print(f"  - Items: {items_count}")
            print(f"  - Has next: {has_next}")
            print(f"  - Is empty: {is_empty}\n")
            
            results.append({
                "page": page,
                "status": "ok" if not is_empty else "empty",
                "has_next": has_next,
                "items": items_count
            })
    
    except KeyboardInterrupt:
        print(f"\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        http_client.close()
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 Summary:")
    print(f"{'='*60}")
    for r in results:
        page = r['page']
        status = r['status']
        has_next = "✓" if r['has_next'] else "✗"
        items = r['items'] if r['items'] is not None else "?"
        print(f"  Page {page:3d}: {status:15s} | Items: {items:3} | Next: {has_next}")
    
    print(f"{'='*60}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test specific pagination pages"
    )
    parser.add_argument(
        "--type",
        "-t",
        choices=["rentals", "sales"],
        default="rentals",
        help="Transaction type (default: rentals)"
    )
    parser.add_argument(
        "--pages",
        "-p",
        type=int,
        nargs="+",
        default=[1, 10, 50, 80, 85, 86, 87, 88, 89, 90],
        help="Specific pages to test"
    )
    
    args = parser.parse_args()
    test_specific_pages(args.type, args.pages)

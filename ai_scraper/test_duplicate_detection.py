"""
Test script to find actual last page by detecting repeated content
"""

import sys
import json
from pathlib import Path
import hashlib

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import DFIMOVEIS_SEARCH_URL, TRANSACTION_TYPES
from http_client import HTTPClient
from ai_agent import AIScrapingAgent


def extract_property_ids(html: str, ai_agent: AIScrapingAgent) -> set:
    """Extract property IDs from HTML to detect duplicates"""
    
    # Look for data-id attributes which indicate property IDs
    import re
    ids = set()
    
    # Pattern to find data-id values (property identifiers)
    pattern = r'data-id="(\d+)"'
    matches = re.findall(pattern, html)
    ids.update(matches)
    
    return ids


def test_until_duplicate(transaction_type: str = "rentals", start_page: int = 1):
    """Test pages sequentially until finding duplicate content"""
    
    if transaction_type not in TRANSACTION_TYPES:
        print(f"❌ Invalid transaction type: {transaction_type}")
        return
    
    tx_type_key = TRANSACTION_TYPES[transaction_type]
    print(f"🤖 Testing for duplicate content to find actual last page")
    print(f"📍 Type: {transaction_type} ({tx_type_key})")
    print(f"🚀 Starting from page {start_page}\n")
    
    http_client = HTTPClient()
    ai_agent = AIScrapingAgent()
    
    previous_ids = None
    page = start_page
    
    try:
        while True:
            url = DFIMOVEIS_SEARCH_URL.format(tx_type_key, page)
            print(f"📄 Page {page}: Fetching...", end=" ", flush=True)
            
            # Fetch HTML
            html = http_client.get(url)
            if not html:
                print(f"❌ Fetch failed")
                break
            
            # Extract property IDs
            property_ids = extract_property_ids(html, ai_agent)
            print(f"Found {len(property_ids)} properties")
            
            # Check for duplicates
            if previous_ids is not None:
                if property_ids == previous_ids:
                    print(f"\n🏁 DUPLICATE CONTENT DETECTED!")
                    print(f"   Page {page - 1} and page {page} have identical properties")
                    print(f"   Last valid page: {page - 1}")
                    break
            
            # Check if empty
            if len(property_ids) == 0:
                print(f"\n⚠️  Page {page} has NO properties")
                print(f"   Last valid page: {page - 1}")
                break
            
            previous_ids = property_ids
            page += 1
            
            # Safety limit
            if page > 150:
                print(f"\n⏹️  Reached safety limit at page {page}")
                break
    
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
        description="Find actual last page by detecting duplicate content"
    )
    parser.add_argument(
        "--type",
        "-t",
        choices=["rentals", "sales"],
        default="rentals",
        help="Transaction type (default: rentals)"
    )
    parser.add_argument(
        "--start",
        "-s",
        type=int,
        default=1,
        help="Starting page number"
    )
    
    args = parser.parse_args()
    test_until_duplicate(args.type, args.start)

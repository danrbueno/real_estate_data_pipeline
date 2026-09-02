"""
Exemplo de uso do AI Scraper
Execute este arquivo para testar o scraper
"""

import sys
from pathlib import Path

# Add project root to path
project_root = str(Path(__file__).parent)
sys.path.insert(0, project_root)

from app.ai_scraper import AIScraper


def test_basic_scraping():
    """Test basic scraping functionality"""
    print("\n" + "="*60)
    print("🤖 AI Scraper - Test Suite")
    print("="*60)

    scraper = AIScraper()

    print("\n1️⃣  Testing rentals scraping...")
    try:
        rentals = scraper.scrape_transaction_type("rentals")
        print(f"✅ Success! Extracted {len(rentals)} rentals")
        if rentals:
            print(f"   First item: {rentals[0].get('title', 'N/A')}")
    except Exception as e:
        print(f"❌ Error: {e}")

    print("\n2️⃣  Testing sales scraping...")
    try:
        sales = scraper.scrape_transaction_type("sales")
        print(f"✅ Success! Extracted {len(sales)} sales")
        if sales:
            print(f"   First item: {sales[0].get('title', 'N/A')}")
    except Exception as e:
        print(f"❌ Error: {e}")

    scraper.close()

    print("\n" + "="*60)
    print("✅ Test suite completed!")
    print("="*60)


def example_single_transaction_type():
    """Example: Scrape single transaction type"""
    from app.ai_scraper import AIScraper

    scraper = AIScraper()

    # Scrape rentals only
    print("Scraping rentals...")
    properties = scraper.scrape_transaction_type("rentals")

    print(f"\nTotal properties extracted: {len(properties)}")
    print("\nSample data:")
    for prop in properties[:3]:
        print(f"- {prop.get('title', 'N/A')}: {prop.get('price', 'N/A')}")

    scraper.close()


def example_with_airflow():
    """Example: How to use with Airflow"""
    from app.ai_scraper import AIScraper

    def airflow_task_scrap_rentals():
        """Airflow task that scrapes rentals"""
        scraper = AIScraper()
        try:
            properties = scraper.scrape_transaction_type("rentals")
            return {"status": "success", "count": len(properties)}
        finally:
            scraper.close()

    # This would be called by Airflow
    result = airflow_task_scrap_rentals()
    print(f"Task result: {result}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AI Scraper Examples")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run test suite"
    )
    parser.add_argument(
        "--example",
        choices=["basic", "single", "airflow"],
        help="Run specific example"
    )

    args = parser.parse_args()

    if args.test:
        test_basic_scraping()
    elif args.example == "basic":
        example_single_transaction_type()
    elif args.example == "airflow":
        example_with_airflow()
    else:
        # Default: run basic example
        example_single_transaction_type()

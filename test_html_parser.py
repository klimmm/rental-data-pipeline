#!/usr/bin/env python3
"""
Test script to verify the HTML parser works correctly
"""
import asyncio
import os
from scraper.html_parser import DataDrivenHtmlParser
from scraper.async_scraper import AsyncScraper
from scraper.scraper_config import AsyncConfig


async def test_html_extraction():
    """Test that the raw HTML extraction works"""
    print("Testing raw HTML extraction...")

    # Create config for raw HTML extraction only
    config = AsyncConfig(
        max_concurrent=1,
        headless=True,
        proxy_configs=[],
        wait_for_selector='[data-name="SummaryHeader"], [data-name="OfferValuationContainerLoader"], [data-name="OfferUnpublished"], h5.error-code, [data-name="OfferValuationContainerLoader"], [data-name="OfferUnpublished"]',
        fallback_wait_for_selector='[data-name="PriceInfo"]',
        wait_for_selector_timeout=10000,
        scroll_to_element=True,
        use_page_content=True,  # Get raw HTML only
    )

    # Test URLs
    test_urls = [
        "https://www.cian.ru/rent/flat/319121013/",
        "https://www.cian.ru/rent/flat/123456789/",
        "https://www.cian.ru/rent/flat/284081870/",
       # "https://www.cian.ru/cat.php?currency=2&deal_type=rent&district%5B0%5D=21&engine_version=2&maxprice=100000&offer_type=flat&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room9=1&sort=creation_date_desc&type=4",
    ]
    scraper = AsyncScraper(config)
    results = await scraper.process_all(test_urls)

    if results and len(results) > 0:
        print(f"✓ Got {len(results)} results")

        successful_results = []
        for i, result in enumerate(results):
            print(f"\n--- Result {i+1} ---")
            print(f"URL: {result.get('url', '')}")
            print(f"Keys: {list(result.keys())}")

            if "error" in result:
                print(f"✗ Error occurred: {result['error']}")
            else:
                html_len = len(result.get("html", ""))
                page_content_len = len(result.get("page_content", ""))
                print(f"✓ HTML length: {html_len}")
                if page_content_len > 0:
                    print(f"✓ Page content length: {page_content_len}")
                successful_results.append(result)

        return successful_results
    else:
        print("✗ No results returned")
        return None


def test_html_parsing(html_results):
    """Test that the HTML parser works correctly using same logic as parse_data.py"""
    print("\nTesting HTML parsing...")

    if not html_results:
        print("✗ No HTML results to parse")
        return []

    parser = DataDrivenHtmlParser()
    parsed_results = []

    for i, html_result in enumerate(html_results):
        print(f"\n--- Processing Result {i+1} ---")

        if not html_result or "html" not in html_result:
            print("✗ No HTML to parse")
            continue

        try:
            # Prefer page_content over html if available
            html = html_result.get("page_content", html_result.get("html", ""))
            url = html_result["url"]

            # Use same card splitting logic as parse_data._parse_raw_html_results
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")
            offers_container = soup.select_one('[data-name="Offers"]')

            if offers_container:
                # This is a search page - split into individual cards
                print("✓ Detected search page - splitting into individual cards")
                cards = offers_container.select('[data-name="CardComponent"]')
                print(f"✓ Found {len(cards)} cards to process")

                for j, card in enumerate(cards):
                    try:
                        # Create individual HTML for each card
                        card_html = f"<html><body>{str(card)}</body></html>"
                        card_data = parser.parse(card_html, url)
                        if card_data and "offer_id" in card_data:
                            print(f"  ✓ Card {j+1}: offer_id {card_data['offer_id']}")
                            parsed_results.append(card_data)
                        else:
                            print(f"  ✗ Card {j+1}: No offer_id found")
                    except Exception as e:
                        print(f"  ✗ Card {j+1}: Parsing failed: {e}")
                        continue
            else:
                # This is a regular page (listing or summary) - parse normally
                print("✓ Processing as regular page")
                parsed = parser.parse(html, url)
                print(f"✓ Parsed successfully with keys: {list(parsed.keys())}")

                # Check key fields
                if "offer_id" in parsed:
                    print(f"✓ offer_id: {parsed['offer_id']}")
                if "offer_price" in parsed:
                    print(f"✓ offer_price: {parsed['offer_price']}")
                if "estimation_price" in parsed:
                    print(f"✓ estimation_price: {parsed['estimation_price']}")
                if "summary" in parsed:
                    print(f"✓ summary: {parsed['summary']}")

                parsed_results.append(parsed)

        except Exception as e:
            print(f"✗ Processing failed: {e}")

    return parsed_results


async def main():
    """Run the tests"""
    print("Starting HTML parser tests...\n")

    # Test 1: Raw HTML extraction
    html_results = await test_html_extraction()

    # Test 2: HTML parsing
    if html_results:
        parsed_data_list = test_html_parsing(html_results)

        # Save the parsed data
        if parsed_data_list:
            import json

            # Save all parsed data
            with open("test_parsed_data_all.json", "w", encoding="utf-8") as f:
                json.dump(parsed_data_list, f, ensure_ascii=False, indent=2)
            print(f"\n✓ All parsed data saved to: test_parsed_data_all.json")

            # Save individual results
            for i, parsed_data in enumerate(parsed_data_list):
                filename = f"test_parsed_data_{i+1}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(parsed_data, f, ensure_ascii=False, indent=2)
                print(f"✓ Result {i+1} saved to: {filename}")

            # Save raw HTML files for debugging
            for i, html_result in enumerate(html_results):
                if "html" in html_result:
                    filename = f"test_raw_html_{i+1}.html"
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(html_result["html"])
                    print(f"✓ Raw HTML {i+1} saved to: {filename}")

    print("\nTests complete!")


if __name__ == "__main__":
    # Note: You'll need to install beautifulsoup4 first:
    # pip install beautifulsoup4

    print("Note: Before running, ensure beautifulsoup4 is installed:")
    print("  pip install beautifulsoup4")
    print("\nAlso update the test_url variable with a valid CIAN listing URL\n")

    # Run tests:
    asyncio.run(main())

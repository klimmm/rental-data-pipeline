import re
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


def normalize_street_names(text):
    """Convert full street names to abbreviated forms"""
    if not text or not isinstance(text, str):
        return text

    # Define replacements
    replacements = [
        ("улица", "ул."),
        ("шоссе", "ш."),
        ("проспект", "просп."),
        ("переулок", "пер."),
        ("бульвар", "бул."),
        ("набережная", "наб."),
    ]

    # Apply replacements
    for long_form, short_form in replacements:
        text = re.sub(r"\b" + re.escape(long_form) + r"\b", short_form, text)

    return text


def build_address(city: str, street: str, building: str) -> str:
    """Build full address from city, street, and building components"""
    if city and street and building:
        return f"{city}, {street}, {building}"
    return ""


def parse_floor_info(floor_text: str) -> tuple:
    """Parse floor information from '3 из 9' format"""
    if not floor_text or not isinstance(floor_text, str):
        return None, None

    match = re.search(r"(\d+)\s*из\s*(\d+)", floor_text)
    if match:
        current_floor = int(match.group(1))
        total_floors = int(match.group(2))
        return current_floor, total_floors

    return None, None


class DataDrivenHtmlParser:
    """Simple HTML parser for CIAN listings"""

    def __init__(self):
        self.soup = None
        self.url = None

    def parse(self, html: str, url: str) -> Dict[str, Any]:
        """Parse HTML to extract CIAN listing data"""
        self.soup = BeautifulSoup(html, "html.parser")
        self.url = url

        result = {}

        # Extract offer ID from URL
        offer_id = self._extract_offer_id()
        if offer_id:
            result["offer_id"] = offer_id

        # Define all text-based selectors and their processing
        text_selectors = {
            # Selector: (field_name, post_processing_function)
            '[data-testid="valuation_estimationPrice"] span': ("estimated_price", None),
            '[data-name="OfferMetaData"] [data-testid="metadata-updated-date"] span': (
                "updated_date",
                lambda x: x.replace("Обновлено: ", ""),
            ),
            '[data-testid="valuation_offerPrice"] span': ("price_value", None),
            '[data-testid="price-amount"] span': ("price_value", None),
            '[data-mark="MainPrice"]': ("price_value", None),
            '[data-name="OfferMetaData"] [data-name="OfferStats"]': (
                "offer_stats",
                None,
            ),
            '[data-name="Description"] span': ("description", None),
            '[class*="absolute"] span': ("updated_date", None),
            '[data-mark="OfferSubtitle"]': ("title", None),
            '[data-mark="OfferTitle"]': ("title", None),
            "h5.error-code": ("error", None),
        }

        # Extract all text-based fields
        text_data = self._extract_text_fields(text_selectors)
        result.update(text_data)

        # Parse offer stats if present
        if "offer_stats" in result:
            parsed_stats = self.parse_offer_stats(result["offer_stats"])
            if parsed_stats:
                result.update(parsed_stats)

        # Parse title info if present
        if "title" in result:
            title_info = self._parse_offer_title(result["title"])
            result.update(title_info)

        # Define label-value pair selectors configuration
        label_value_selectors = {
            '[data-name="OfferFactItem"]': {"tag_name": "span", "min_elements": 2},
            '[data-name="ObjectFactoidsItem"]': {"tag_name": "span", "min_elements": 2},
            '[data-name="OfferSummaryInfoItem"]': {"tag_name": "p", "min_elements": 2},
        }

        # Parse all label-value pairs using unified configuration
        for selector, config in label_value_selectors.items():
            elements = self.soup.select(selector)
            if elements:
                parsed_data = self._parse_label_value_pairs(
                    elements,
                    tag_name=config["tag_name"],
                    min_elements=config["min_elements"],
                )
                result.update(parsed_data)

        # Parse floor information if "Этаж" field is present
        if "Этаж" in result:
            current_floor, total_floors = parse_floor_info(result["Этаж"])
            if current_floor is not None:
                result["floor"] = current_floor
            if total_floors is not None:
                result["total_floors"] = total_floors

        # Handle special cases
        geo_element = self.soup.select_one('[data-name="Geo"]')
        if geo_element:
            if link := geo_element.select_one('[data-name="UndergroundItem"] a'):
                station = link.get_text(strip=True).replace("м. ", "")
                result["metro_station"] = station

        # Check if unpublished
        result["is_unpublished"] = bool(
            self.soup.select_one('[data-name="OfferUnpublished"]')
        )

        # Extract address
        address_elem = self.soup.select_one('[data-name="Geo"] [itemprop="name"]')
        if address_elem:
            full_address = address_elem.get("content", "")
            if full_address:
                result["address"] = full_address

        # Define geo selectors configuration
        geo_selectors = [
            '[data-name="Geo"] [data-name="AddressItem"]',
            '[data-name="GeneralInfoSectionRowComponent"] [data-name="GeoLabel"]',
        ]

        for selector in geo_selectors:
            elements = self.soup.select(selector)
            if elements:
                geo_data = self._parse_semantic_geo_items(elements)
                result.update(geo_data)

        # Build full address from parsed geo components
        if "city" in result and "street" in result and "building" in result:
            # Normalize street name before building address
            normalized_street = normalize_street_names(result["street"])
            result["street"] = normalized_street
            address = build_address(
                result["city"], normalized_street, result["building"]
            )
            if address:
                result["address"] = address

        # Parse features as individual fields
        features_elements = self.soup.select('[data-name="FeaturesItem"]')
        if features_elements:
            for elem in features_elements:
                feature_name = elem.text.strip()
                if feature_name:
                    result[feature_name] = True

        # Parse summary header
        summary_header_element = self.soup.select_one('[data-name="SummaryHeader"]')
        if summary_header_element:
            summary_text = summary_header_element.get_text(strip=True)
            if summary_text:
                match = re.search(r"(\d+)", summary_text)
                if match:
                    result["summary"] = int(match.group(1))

        link_element = self.soup.select_one(
            '[data-name="LinkArea"] a[href*="/rent/flat/"]'
        )
        if link_element:
            url = link_element.get("href", "").rstrip("/")
            offer_id = self._extract_offer_id(url)
            result["offer_url"] = url
            result["offer_id"] = offer_id

        # Parse price info
        price_info_element = self.soup.select_one('[data-mark="PriceInfo"]')
        if price_info_element:
            rental_terms = self._parse_price_info_container(price_info_element)
            result.update(rental_terms)

        # Parse gallery
        gallery_element = self.soup.select_one('[data-name="Gallery"]')
        if gallery_element:
            images = self._parse_gallery(gallery_element)
            if images:
                result["image_urls"] = images

        # Add timestamp
        result["timestamp"] = self._get_timestamp()

        return result

    def _extract_text_fields(self, selectors: Dict[str, tuple]) -> Dict[str, Any]:
        """Extract all text fields based on selectors dictionary"""
        result = {}

        for selector, (field_name, processor) in selectors.items():
            element = self.soup.select_one(selector)
            if element and element.text.strip():
                text = element.text.strip()
                # Apply post-processing if provided
                if processor:
                    text = processor(text)
                # Don't overwrite existing values (priority order)
                if field_name not in result:
                    result[field_name] = text

        return result

    def _extract_offer_id(self, url=None) -> Optional[str]:
        """Extract offer ID from URL"""
        url = url or self.url
        match = re.search(r"/rent/flat/(\d+)", url)
        return match.group(1) if match else None

    def _parse_label_value_pairs(
        self, elements, tag_name: str = "span", min_elements: int = 2
    ) -> Dict[str, str]:
        """Generic parser for elements containing label-value pairs

        Args:
            elements: List of BeautifulSoup elements to parse
            tag_name: HTML tag to look for within each element (e.g., 'span', 'p')
            min_elements: Minimum number of child elements required

        Returns:
            Dictionary mapping labels to values
        """
        result = {}
        for item in elements:
            child_elements = item.select(tag_name)
            if len(child_elements) >= min_elements:
                label = child_elements[0].text.strip()
                value = (
                    child_elements[-1].text.strip()
                    if min_elements == 2
                    else child_elements[1].text.strip()
                )
                # Skip if label and value are the same (for span elements)
                if label and value and (tag_name != "span" or label != value):
                    result[label] = value
        return result

    def _parse_offer_title(self, title) -> Dict[str, Any]:
        result = {}
        text_processed = re.sub(r"(\d+)/(\d+)", r"\1 \2", title)
        numbers = re.findall(r"\d+[.,]\d+|\d+", text_processed)
        if len(numbers) >= 3:
            result["room_count"] = numbers[0] if len(numbers) > 3 else 0
            result["total_area"] = numbers[-3].replace(",", ".")
            result["floor"] = numbers[-2]
            result["total_floors"] = numbers[-1]

        return result

    def parse_offer_stats(self, stats_text):
        result = {}
        # Extract all numbers from the string
        numbers = re.findall(r"\d+", stats_text)
        if len(numbers) == 3:
            result["total_views"] = numbers[0]
            result["today_views"] = numbers[1]
            result["unique_views"] = numbers[2]
        return result

    def _parse_price_info_container(self, element) -> Dict[str, str]:
        result = {}
        price_info = element.get_text(strip=True)
        parts = price_info.split(",", 3)
        # print(f"raw_parts {raw_parts}")
        # parts = [p.strip() for p in raw_parts] + [""] * (4 - len(raw_parts))
        # rental_part, utilities_part, commission_part, deposit_part = parts
        result["rental_period"] = parts[0]
        result["utilities_payment"] = parts[1]
        result["commission"] = parts[2]
        result["security_deposit"] = parts[3]
        return result

    def _parse_semantic_geo_items(self, items) -> Dict[str, Any]:
        """Unified method to parse semantic geo information from AddressItem or GeoLabel elements"""
        result = {}

        for i, item in enumerate(items):
            # Skip if item is not a tag
            if not hasattr(item, "get"):
                continue

            text = item.text.strip()
            href = item.get("href", "")

            if not text:
                continue

            if i == 0:
                # First item is always city
                result["city"] = text
            elif i == 1 and (
                "district%5B0%5D=" in href or re.search(r"-04\d+/?$", href)
            ):
                result["district"] = text
            elif i == 2 and (
                "district%5B0%5D=" in href or re.search(r"-04\d+/?$", href)
            ):
                result["neighborhood"] = text
            elif re.search(r"-02\d+/?$", href) or re.search(r"street%5B0%5D=\d+", href):
                # Street patterns
                result["street"] = text
                # Extract street ID from pattern
                street_match = re.search(r"-02(\d+)/?$", href) or re.search(
                    r"street%5B0%5D=(\d+)", href
                )
                if street_match:
                    result["street_id"] = street_match.group(1)
            elif re.search(r"house%5B0%5D=\d+", href) or re.search(
                r"/dom/.*-\d+/?$", href
            ):
                # Building patterns
                result["building"] = text
                # Extract building ID from pattern
                building_match = re.search(r"house%5B0%5D=(\d+)", href) or re.search(
                    r"/dom/.*-(\d+)/?$", href
                )
                if building_match:
                    result["building_id"] = building_match.group(1)
            elif "metro%5B0%5D=" in href:
                # Metro station patterns
                result["metro_station"] = text.replace("м. ", "")
                # Extract metro ID from pattern
                metro_match = re.search(r"metro%5B0%5D=(\d+)", href)
                if metro_match:
                    result["metro_id"] = metro_match.group(1)

        return result

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime

        return datetime.now().isoformat()

    def _parse_gallery(self, element) -> List[str]:
        """Parse image URLs from Gallery container"""
        img_elements = element.select('img[src*="cdn-cian.ru"]')
        image_urls = []
        for img in img_elements:
            img_url = img.get("src", "")
            if img_url:
                # Replace -4.jpg with -1.jpg for better quality
                img_url = re.sub(r"-4\.jpg$", "-1.jpg", img_url)
                image_urls.append(img_url)

        return image_urls


# Backward compatibility alias
CianListingParser = DataDrivenHtmlParser

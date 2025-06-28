#!/usr/bin/env python3
import re
from datetime import datetime, timedelta


def parse_numeric_value(value):
    if isinstance(value, (int, float)):
        return value
    if not value or not isinstance(value, str):
        return 0

    try:
        cleaned = value.replace(",", ".")
        cleaned = re.sub(r"\s+", "", cleaned)
        match = re.search(r'(-?\d+(?:\.\d+)?)', cleaned)
        if match:
            result = float(match.group(1))
            return int(result) if result.is_integer() else result
        return 0
    except Exception as e:
        print(f"Error parsing '{value}': {e}")
        return value


def parse_floor_info(floor_text):
    """Parse floor information from '3 из 9' format"""
    if not floor_text or not isinstance(floor_text, str):
        return None, None

    match = re.search(r"(\d+)\s*из\s*(\d+)", floor_text)
    if match:
        current_floor = int(match.group(1))
        total_floors = int(match.group(2))
        return current_floor, total_floors

    return None, None


def parse_offer_stats(stats_text):
    """Parse offer stats from '549 просмотров, 56 за сегодня, 409 уникальных' format"""
    if not stats_text or not isinstance(stats_text, str):
        return None, None, None

    # Extract all numbers from the string
    numbers = re.findall(r"\d+", stats_text)

    # If we found exactly 3 numbers, return them as integers
    if len(numbers) == 3:
        return int(numbers[0]), int(numbers[1]), int(numbers[2])
    elif len(numbers) == 2:
        return int(numbers[0]), int(numbers[1]), None
    return None, None, None


def parse_title_info(title_text):
    if not title_text or not isinstance(title_text, str):
        return None, None, None, None
    text_processed = re.sub(r"(\d+)/(\d+)", r"\1 \2", title_text)
    numbers = re.findall(r"\d+[.,]\d+|\d+", text_processed)
    room_count = total_area = floor = total_floors = None
    if len(numbers) >= 3:
        room_count = numbers[0] if len(numbers) > 3 else 9
        total_area = numbers[-3].replace(",", ".")
        floor = numbers[-2]
        total_floors = numbers[-1]
    return room_count, total_area, floor, total_floors


def parse_address_id(href: str) -> str:
    """Extract ID from href using a list of regex patterns, returning first match."""
    if not href:
        return None
    patterns = [
        r"-02(\d+)/?$",  # Pattern: -02XXXX/
        r"street%5B0%5D=(\d+)",  # Pattern: street%5B0%5D=XXXX
        r"house%5B0%5D=(\d+)",  # Pattern: house%5B0%5D=XXXXXXX
        r"/dom/.*-(\d+)/?$",  # Pattern: /dom/...-XXXXXXX/
    ]
    for pattern in patterns:
        match = re.search(pattern, href)
        if match:
            return match.group(1)

    return None


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


def parse_russian_date(time_label):
    """Parse Russian time labels to YYYY-MM-DD HH:MM:SS format"""
    if not time_label:
        return None

    now = datetime.now()

    # Extract time component first (common to all formats)
    time_match = re.search(r"(\d{1,2}):(\d{2})", time_label)
    if not time_match:
        return time_label

    hour, minute = int(time_match.group(1)), int(time_match.group(2))

    try:
        # Handle different date formats
        if "сегодня" in time_label:
            # Today
            result = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        elif "вчера" in time_label:
            # Yesterday
            result = (now - timedelta(days=1)).replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )
        else:
            # Handle "DD month" format
            date_match = re.search(r"(\d{1,2})\s+([а-яА-Я]+)", time_label)
            if not date_match:
                return time_label

            day = int(date_match.group(1))
            month_name = date_match.group(2).lower()

            # Map month names to numbers
            months = {
                "янв": 1,
                "фев": 2,
                "мар": 3,
                "апр": 4,
                "май": 5,
                "мая": 5,
                "июн": 6,
                "июл": 7,
                "авг": 8,
                "сен": 9,
                "окт": 10,
                "ноя": 11,
                "дек": 12,
            }

            if month_name not in months:
                return time_label

            month = months[month_name]
            year = now.year

            # Create datetime and adjust year if needed
            result = datetime(year, month, day, hour, minute)
            if result > now:
                result = result.replace(year=year - 1)

        return result.strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print(f"Error parsing time label '{time_label}': {e}")
        return time_label


def normalize_listings(flattened_listings):
    """Normalize flattened listings data."""
    normalized_listings = []

    for listing in flattened_listings:
        normalized = listing.copy()

        # Normalize street names
        if "street" in normalized and normalized["street"]:
            normalized["street"] = normalize_street_names(normalized["street"])

        # Parse address IDs
        if "street_href" in normalized and normalized["street_href"]:
            normalized["street_id"] = parse_address_id(normalized["street_href"])
        if "building_href" in normalized and normalized["building_href"]:
            normalized["building_id"] = parse_address_id(normalized["building_href"])

        # Build full address
        city = normalized.get("city")
        street = normalized.get("street")
        building_number = normalized.get("building_number")
        if city and street and building_number:
            normalized["address"] = f"{city}, {street}, {building_number}"
        else:
            raw_addr = normalized.get("raw_address") or normalized.get(
                "raw_full_address"
            )
            normalized["address"] = (
                normalize_street_names(raw_addr) if raw_addr else None
            )

        # Parse floor information
        if "floor_combined" in normalized and normalized["floor_combined"]:
            current_floor, total_floors = parse_floor_info(normalized["floor_combined"])
            if current_floor is not None and not normalized.get("floor"):
                normalized["floor"] = current_floor
            if total_floors is not None and not normalized.get("total_floors"):
                normalized["total_floors"] = total_floors

        # Parse offer stats
        if "offer_stats" in normalized and normalized["offer_stats"]:
            total_views, today_views, unique_views = parse_offer_stats(
                normalized["offer_stats"]
            )
            if total_views is not None and not normalized.get("total_views"):
                normalized["total_views"] = total_views
            if today_views is not None and not normalized.get("today_views"):
                normalized["today_views"] = today_views
            if unique_views is not None and not normalized.get("unique_views"):
                normalized["unique_views"] = unique_views

        # Parse title information
        if "title" in normalized and normalized["title"]:
            room_count, total_area, floor, total_floors = parse_title_info(
                normalized["title"]
            )

            # Fill missing values with title info
            if room_count is not None and not normalized.get("room_count"):
                normalized["room_count"] = room_count
            if total_area is not None and not normalized.get("total_area"):
                normalized["total_area"] = total_area
            if floor is not None and not normalized.get("floor"):
                normalized["floor"] = floor
            if total_floors is not None and not normalized.get("total_floors"):
                normalized["total_floors"] = total_floors
        if not normalized.get("is_unpublished", False):
            normalized["status"] = 'active'
        else:
            normalized["status"] = 'non active'

        # Parse numeric values
        numeric_fields = [
            "price_value",
            "estimated_price",
            "security_deposit",
            "commission",
            "prepayment",
            "total_area",
            "living_area",
            "kitchen_area",
            "ceiling_height",
            "total_floors",
            "floor",
            "room_count",
            "total_views",
            "today_views",
            "unique_views",
            "sleeping_places",
            "distance",
            "room_area",
            "rooms_for_rent",
            "rooms_in_apartment",
            "price_change_value",
        ]
        for field in numeric_fields:
            if field in normalized and normalized[field] is not None:
                normalized[field] = parse_numeric_value(normalized[field])

        # Parse date fields
        date_fields = [
            "timestamp",
            "updated_date",
            "last_active",
            "publication_date",
            "unpublished_date",
            "price_change_date",
        ]
        for field in date_fields:
            if field in normalized and normalized[field] is not None:
                normalized[field] = parse_russian_date(normalized[field])

        # Clean up temporary columns
        temp_cols = [
            "street_href",
            "building_href",
            "raw_address",
            "raw_full_address",
            "floor_combined",
            "offer_stats",
            "title",
        ]
        for col in temp_cols:
            if col in normalized:
                del normalized[col]

        normalized_listings.append(normalized)

    return normalized_listings

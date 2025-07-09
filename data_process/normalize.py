#!/usr/bin/env python3
import re
from datetime import datetime, timedelta
import pandas as pd


def parse_numeric_value(value):
    if isinstance(value, (int, float)):
        return value
    if not value or not isinstance(value, str):
        return None

    try:
        cleaned = value.replace(",", ".")
        cleaned = re.sub(r"\s+", "", cleaned)
        match = re.search(r"(-?\d+(?:\.\d+)?)", cleaned)
        if match:
            result = float(match.group(1))
            return int(result) if result.is_integer() else result
        return None
    except Exception as e:
        print(f"Error parsing '{value}': {e}")
        return None


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
        return pd.NaT


def normalize_listings(flattened_listings):
    """Normalize flattened listings data."""
    normalized_listings = []

    for listing in flattened_listings:
        normalized = listing.copy()

        if not normalized.get("is_unpublished", False):
            normalized["status"] = "active"
        else:
            normalized["status"] = "non active"

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
            "floor_combined",
        ]
        for col in temp_cols:
            if col in normalized:
                del normalized[col]

        normalized_listings.append(normalized)

    return normalized_listings

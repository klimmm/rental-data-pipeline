#!/usr/bin/env python3
from typing import Any, Dict

def get_field_mappings() -> Dict[str, str]:
    """Get field mappings for converting Russian field names to English."""
    return {
        # Rental terms
        "Залог": "security_deposit",
        "deposit": "security_deposit",
        "Комиссии": "commission",
        "Комиссия": "commission",
        "Оплата ЖКХ": "utilities_payment",
        "Предоплата": "prepayment",
        "Предоплаты": "prepayment",
        "Срок аренды": "rental_period",
        "lease_term": "rental_period",
        "Торг": "negotiable",
        "Условия проживания": "living_conditions",
        # Apartment fields
        "Балкон/лоджия": "balcony",
        "Вид из окон": "view",
        "Высота потолков": "ceiling_height",
        "Год постройки": "year_built",
        "Жилая площадь": "living_area",
        "Комнат в аренду": "rooms_for_rent",
        "Комнат в квартире": "rooms_in_apartment",
        "Общая площадь": "total_area",
        "Планировка": "layout",
        "Площадь комнат": "room_area",
        "Площадь кухни": "kitchen_area",
        "Ремонт": "renovation",
        "Санузел": "bathroom",
        "Спальных мест": "sleeping_places",
        "Тип жилья": "apartment_type",
        "Этаж": "floor_combined",
        # Building fields
        "Аварийность": "emergency",
        "Газоснабжение": "gas_supply",
        "Количество лифтов": "elevators",
        "Мусоропровод": "garbage_chute",
        "Отопление": "heating",
        "Парковка": "parking",
        "Подъезды": "entrances",
        "Строительная серия": "building_series",
        "Тип дома": "building_type",
        "Тип перекрытий": "ceiling_type",
    }


def get_features_logic() -> Dict[str, str]:
    """Get features logic for unpacking features into boolean columns."""
    return {
        "has_refrigerator": "Холодильник",
        "has_dishwasher": "Посудомоечная машина",
        "has_washing_machine": "Стиральная машина",
        "has_air_conditioner": "Кондиционер",
        "has_internet": "Интернет",
        "has_bathtub": "Ванна",
        "has_room_furniture": "Мебель в комнатах",
        "has_tv": "Телевизор",
        "has_kitchen_furniture": "Мебель на кухне",
        "has_shower_cabin": "Душевая кабина",
    }


def flatten_listing(
    listing: Dict[str, Any],
) -> Dict[str, Any]:
    """Flatten a nested listing dictionary into a flat structure."""
    flattened = {}
    field_mappings = get_field_mappings()
    features_logic = get_features_logic()

    # Top level fields
    flattened["offer_id"] = listing.get("offer_id")
    flattened["price_value"] = listing.get("offer_price")
    flattened["estimated_price"] = listing.get("estimation_price") or listing.get(
        "estimated_price"
    )
    flattened["title"] = listing.get("title")
    flattened["description"] = listing.get("description")
    flattened["offer_url"] = listing.get("url") or listing.get("offer_url")

    # Metadata
    metadata = listing.get("metadata", {})
    flattened["updated_date"] = metadata.get("updated_date")
    flattened["offer_stats"] = metadata.get("offer_stats")
    flattened["is_unpublished"] = metadata.get("is_unpublished")

    # Geo information
    geo = listing.get("geo", {})

    address_items = geo.get("address_items", [])
    if len(address_items) == 5:
        flattened["city"] = address_items[0].get("text")
        flattened["district"] = address_items[1].get("text")
        flattened["neighborhood"] = address_items[2].get("text")
        flattened["street"] = address_items[3].get("text")
        flattened["building_number"] = address_items[4].get("text")
        flattened["street_href"] = address_items[3].get("href", "")
        flattened["building_href"] = address_items[4].get("href", "")
    elif len(address_items) == 3:
        flattened["city"] = address_items[0].get("text")
        flattened["district"] = geo.get("district")
        flattened["neighborhood"] = geo.get("neighborhood")
        flattened["street"] = address_items[1].get("text")
        flattened["building_number"] = address_items[2].get("text")
        flattened["street_href"] = address_items[1].get("href", "")
        flattened["building_href"] = address_items[2].get("href", "")

    flattened["raw_address"] = geo.get("address")
    flattened["raw_full_address"] = geo.get("full_address")

    # Metro stations - get closest one and all stations
    metro_stations = geo.get("metro_stations", [])
    if metro_stations:
        flattened["metro_station"] = metro_stations[0].get("name")
    else:
        flattened["metro_station"] = geo.get("metro_station")

    # Rental terms - flatten all key-value pairs
    rental_terms = listing.get("rental_terms", {})
    for key, value in rental_terms.items():
        mapped_key = field_mappings.get(key, key.lower().replace(" ", "_"))
        flattened[mapped_key] = value

    # Apartment details - flatten all key-value pairs
    apartment = listing.get("apartment", {})
    for key, value in apartment.items():
        mapped_key = field_mappings.get(key, key.lower().replace(" ", "_"))
        flattened[mapped_key] = value

    # Building details - flatten all key-value pairs
    building = listing.get("building", {})
    for key, value in building.items():
        mapped_key = field_mappings.get(key, key.lower().replace(" ", "_"))
        flattened[mapped_key] = value

    # Features - unpack into boolean columns
    features = listing.get("features")
    if features is not None:
        features_text = "; ".join(features) if features else ""
        # Unpack features into boolean columns
        for feature_name, feature_keyword in features_logic.items():
            flattened[feature_name] = feature_keyword in features_text
    else:
        # If features field doesn't exist, leave has_{feature} fields as None
        for feature_name in features_logic.keys():
            flattened[feature_name] = None

    return flattened


def flatten_listings(parse_listings):
    """Complete pipeline: flatten then normalize."""

    flattened_listings = []
    for listing in parse_listings:
        try:
            flattened = flatten_listing(listing)
            flattened_listings.append(flattened)
        except Exception as e:
            print(f"Error {e}")

    return flattened_listings
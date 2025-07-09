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
        "Холодильник": "has_refrigerator",
        "Посудомоечная машина": "has_dishwasher",
        "Стиральная машина": "has_washing_machine",
        "Кондиционер": "has_air_conditioner",
        "Интернет": "has_internet",
        "Ванна": "has_bathtub",
        "Мебель в комнатах": "has_room_furniture",
        "Телевизор": "has_tv",
        "Мебель на кухне": "has_kitchen_furniture",
        "Душевая кабина": "has_shower_cabin",
    }

def flatten_listings(parse_listings):
    """Flatten listings - handles both single listing dict or list of listings."""
    field_mappings = get_field_mappings()

    def flatten_single(listing):
        return {
            field_mappings.get(key, key): value 
            for key, value in listing.items()
        }

    try:
        return [flatten_single(listing) for listing in parse_listings]
    except Exception as e:
        print(f"Error flattening listings: {e}")
        return []
from scraper.async_http_processor import AsyncHttpProcessor
import logging
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)


def create_geocoding_request(address: str, offer_id: str) -> Dict[str, Any]:
    """Create a geocoding request for Nominatim API"""
    return {
        "url": "https://nominatim.openstreetmap.org/search",
        "request_id": offer_id,
        "method": "GET",
        "params": {
            "q": address,
            "format": "json",
            "countrycodes": "ru",
            "addressdetails": 1,
        },
    }


def create_distance_request(
    from_coords: str, to_coords: str, offer_id: str
) -> Dict[str, Any]:
    """Create a distance/routing request for OSRM API"""
    return {
        "url": f"https://routing.openstreetmap.de/routed-foot/route/v1/foot/{from_coords};{to_coords}",
        "request_id": offer_id,
        "method": "GET",
        "params": {
            "overview": "false",
            "alternatives": "false",
        },
    }


async def get_distance(listings_data, ref_coords, config):
    # Create geocoding requests
    geocoding_requests = []
    for listing in listings_data:
        geocoding_requests.append(
            create_geocoding_request(listing["address"], listing["offer_id"])
        )

    processor = AsyncHttpProcessor(config)

    geocoding_results = await processor.process_all(geocoding_requests)
    #with open("geo.json", "w", encoding="utf-8") as f:
    #    json.dump(geocoding_results, f, ensure_ascii=False, indent=2)

    distance_requests = []

    for result in geocoding_results:
        if result.get("status") == 200:
            try:
                offer_id = result["request_id"]
                lat = float(result["data"][0]["lat"])
                lon = float(result["data"][0]["lon"])

                to_coords = f"{lon},{lat}"
                distance_requests.append(
                    create_distance_request(ref_coords, to_coords, offer_id)
                )
            except (KeyError, IndexError, TypeError, ValueError) as e:
                logger.warning(
                    f"Invalid geocoding data for {result.get('request_id', 'unknown')}: {e}"
                )

    distance_results = await processor.process_all(distance_requests)
    #with open("distance.json", "w", encoding="utf-8") as f:
    #    json.dump(distance_results, f, ensure_ascii=False, indent=2)

    # Create list of dictionaries with only offer_id and distance
    distance_data = []
    for result in distance_results:
        if result.get("status") == 200:
            try:
                offer_id = result["request_id"]
                distance_meters = result["data"]["routes"][0]["distance"]
                distance_km = round(distance_meters / 1000, 2)

                distance_data.append({"offer_id": offer_id, "distance": distance_km})
            except (KeyError, IndexError, TypeError, ValueError) as e:
                logger.warning(
                    f"Invalid route data for {result.get('request_id', 'unknown')}: {e}"
                )

    return distance_data
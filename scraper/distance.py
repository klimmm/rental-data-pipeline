from .async_http_processor import AsyncHttpProcessor
import logging
import json
from typing import List, Dict, Any
import asyncio
from .scraper_config import AsyncConfig
try:
    from ..vpn_manager.vpn_manager import VPNManager
except ImportError:
    VPNManager = None


def setup_logging(level=logging.INFO):
    """Configure logging format and level"""
    logging.basicConfig(
        level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def load_listings(filename: str) -> List[Dict[str, Any]]:
    """Load listings data from JSON file"""
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def save_listings(listings: List[Dict[str, Any]], filename: str) -> None:
    """Save listings data to JSON file"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)


def clean_coordinate_data(raw_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract all coordinate data from HTTP response format as a flat list"""
    cleaned_data = []

    for result in raw_results:
        if "error" not in result and result.get("status") == 200:
            address = result["request_id"]
            data = result.get("data", [])

            # Add each result with the query address included
            for location in data:
                location["query_address"] = address
                cleaned_data.append(location)

    return cleaned_data


async def get_coordinates(listings_data, ref_coords, config, proxy_configs):
    # Create geocoding requests with deduplication
    geocoding_requests = []
    seen_addresses = set()

    for listing in listings_data:
        address = listing.get("geo", {}).get("address", "")
        if address and address not in seen_addresses:
            seen_addresses.add(address)
            geocoding_requests.append(
                {
                    "url": "https://nominatim.openstreetmap.org/search",
                    "method": "GET",
                    "params": {
                        "q": address,
                        "format": "json",
                        "countrycodes": "ru",
                        "addressdetails": 1,
                    },
                    "request_id": address,  # Use address as request_id for deduplication
                }
            )

    # Initialize processor
    processor = AsyncHttpProcessor(config, proxy_configs)

    # Get geocoding results
    geocoding_results = await processor.process_all(geocoding_requests)
    return geocoding_results


async def calculate_distances(listings_data, ref_coords, config, proxy_configs):
    # Create geocoding requests
    geocoding_requests = []
    for listing in listings_data:
        if listing.get("distance") is None:
            address = listing.get("geo", {}).get("address", "")
            if address:
                geocoding_requests.append(
                    {
                        "url": "https://nominatim.openstreetmap.org/search",
                        "method": "GET",
                        "params": {
                            "q": address,
                            "format": "json",
                            "countrycodes": "ru",
                            "addressdetails": 1,
                        },
                        "request_id": listing.get("offer_id", "unknown"),
                    }
                )

    # Initialize processor
    processor = AsyncHttpProcessor(config, proxy_configs)

    # Get geocoding results
    geocoding_results = await processor.process_all(geocoding_requests)

    # Extract coordinates and prepare distance requests
    distance_requests = []
    coords_map = {}  # Map request_id to coordinates

    for result in geocoding_results:
        if "error" not in result and result["status"] == 200:
            request_id = result["request_id"]
            data = result["data"]

            if data and len(data) > 0:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                coords_map[request_id] = (lat, lon)

                from_coord = f"{ref_coords[1]},{ref_coords[0]}"  # Convert (lat,lon) to lon,lat
                to_coord = f"{lon},{lat}"  # lon,lat format required by routing API

                distance_requests.append(
                    {
                        "url": f"https://routing.openstreetmap.de/routed-foot/route/v1/foot/{from_coord};{to_coord}",
                        "method": "GET",
                        "params": {
                            "overview": "false",
                            "alternatives": "false",
                        },
                        "request_id": request_id,
                    }
                )

    # Get distance results
    distance_results = await processor.process_all(distance_requests)

    # Update original listings
    for result in distance_results:
        if "error" not in result and result["status"] == 200:
            request_id = result["request_id"]
            data = result["data"]

            if "routes" in data and len(data["routes"]) > 0:
                distance_meters = data["routes"][0]["distance"]
                distance_km = round(distance_meters / 1000, 2)

                # Find and update the original listing
                for listing in listings_data:
                    if listing.get("offer_id") == request_id:
                        listing["distance"] = distance_km
                        break

    return listings_data


async def calculate_and_update_distances(listings_data, proxy_configs=None):
    """
    Async wrapper for calculate_distances that matches the expected interface
    in parse_data_cian.py
    """
    # Default reference coordinates (Moscow center)
    ref_coords = (55.7355742, 37.5701096)
    
    # Create config for HTTP processor
    config = AsyncConfig(max_concurrent=9, max_retries=3, timeout=20)
    
    # Call the async calculate_distances function directly
    return await calculate_distances(listings_data, ref_coords, config, proxy_configs or [])


async def main(
    listings=None,
    num_workers=4,
    proxy_configs=None,
):
    config = AsyncConfig(max_concurrent=4, max_retries=3, timeout=20)
    
    # Handle proxy configs
    if proxy_configs is None and VPNManager is not None:
        vpn_manager = VPNManager(vpn_type="xray")
        proxy_configs = vpn_manager.get_all_proxies(exclude_countries=None)
    else:
        proxy_configs = proxy_configs or []
    
    setup_logging(level=logging.INFO)
    listings_data = load_listings("merged_listings.json")
    listings_data = listings_data
    ref_coords = (55.7355742, 37.5701096)

    results = await calculate_distances(
        listings_data, ref_coords, config, proxy_configs
    )

    save_listings(results, "test_data_distance_coordinates.json")
    # Print results
    print(f"Updated {len(results)} listings")
    for listing in results:
        if "distance" in listing:
            print(f"Listing {listing['offer_id']}: {listing['distance']} km")

    return results


if __name__ == "__main__":

    asyncio.run(main())

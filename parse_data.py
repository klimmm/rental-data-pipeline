import os
import gc
import json
import logging
import yaml
import math

from search_configs.construct_url import (
    construct_search_url,
    generate_search_page_urls,
    generate_listing_page_urls,
)

import pandas as pd

from scraper.scraper_config import AsyncConfig
from scraper.async_scraper import AsyncScraper
from distance import get_distance
from data_process.flatten import flatten_listings
from data_process.normalize import normalize_listings
from data_process.merge import merge_listings

try:
    from vpn_manager.vpn_manager import VPNManager

    vpn_available = True
except ImportError:
    VPNManager = None
    vpn_available = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScraperPipeline:
    """Pipeline for scraping, processing, and merging data."""

    def __init__(
        self,
        data_dir,
        use_proxies,
        search_config_path,
        check_missing_estimations,
        check_if_unpublished,
        update_current_search_listings,
        should_scrape_new=True,
        check_missing=False,
    ):

        self.check_missing_estimations = check_missing_estimations
        self.check_if_unpublished = check_if_unpublished
        self.update_current_search_listings = update_current_search_listings
        self.should_scrape_new = should_scrape_new
        self.check_missing = check_missing
        self.json_file_path = os.path.join(data_dir, "merged_listings_flattened.json")
        self.json_distance = os.path.join(
            data_dir, "merged_listings_flattened_distance.json"
        )
        self.search_config_path = search_config_path
        self.logger = logging.getLogger(__name__)

        # Derive file paths for output files
        self.base_dir = data_dir
        self.image_urls_file = os.path.join(self.base_dir, "image_urls.json")
        self.parsed_listings_file = os.path.join(
            self.base_dir, "parsed_listings_flattened.json"
        )
        self.csv_file = os.path.join(self.base_dir, "combined_data.csv")
        self.json_interm = os.path.join(
            self.base_dir, "merged_listings_flattened_interm.json"
        )
        # Initialize VPN and proxy configurations
        self.use_proxies = use_proxies
        if self.use_proxies and vpn_available:
            vpn_manager = VPNManager()
            self.proxy_configs = vpn_manager.get_all_proxies()
            self.logger.info(
                f"Initialized VPN manager with {len(self.proxy_configs)} proxies"
            )
        else:
            self.proxy_configs = []
            if self.use_proxies and not vpn_available:
                self.logger.warning("VPN requested but not available")

        # Initialize configurations
        self.search_config = None
        self.listings_per_page = 28
        self.total_pages = 34
        self.base_url = None
        self.scraper_configs = None
        self.ref_coords = (
            "37.5701096,55.7355742"  # Already in API string format (lon,lat)
        )
        # Data containers
        self.search_summary = None
        self.current_search_listings = []
        self.db_listings = None
        self.merged_data = None
        self.listings_to_scrape = []
        self.parsed_listings = None
        self.missing_listings = []
        self.db_listings = None
        self.db_active_ids = None
        self.db_active_ids_missing_estimation = None

    def setup(self):
        """Set up configurations and prepare for scraping."""
        # Load search configuration
        self.search_config = self._load_yaml_file(self.search_config_path)
        self.base_url = construct_search_url(self.search_config)
        self.logger.info(f"Base URL: {self.base_url}")
        # Initialize scraper configurations
        self.scraper_configs = self._get_scraper_configs()

        # Load existing data
        self._load_existing_data()
        self.db_listings = self._load_json_file(self.json_file_path) or []
        self.logger.info(
            f"DEBUG: use_proxies={self.use_proxies}, "
            f"self.proxy_configs={len(self.proxy_configs)}"
        )
        self.logger.info(
            f"DEBUG: use_proxies={self.check_missing_estimations}, "
            f"self.check_if_unpublished={self.check_if_unpublished} "
            f"self.update_current_search_listings={self.update_current_search_listings} "
        )

    def _load_existing_data(self):
        self.db_listings = self._load_json_file(self.json_file_path) or []
        self.db_active_ids = {
            listing["offer_id"]
            for listing in self.db_listings
            if not listing.get("is_unpublished", False)
        }
        self.db_active_ids_missing_estimation = {
            listing["offer_id"]
            for listing in self.db_listings
            if not listing.get("estimated_price", "")
            and not listing.get("is_unpublished", False)
        }

        self.logger.info(
            f"Loaded {len(self.db_listings)} existing listings, "
            f"including active: {len(self.db_active_ids)}, "
            f"missing estimation: {len(self.db_active_ids_missing_estimation)}"
        )

    def _get_scraper_configs(self):
        """Generate AsyncScraperConfig objects for each scraping phase."""
        base_dir = os.path.dirname(__file__)
        scraper_scripts_dir = os.path.join(base_dir, "scraper", "js")

        def create_config(**overrides):
            """Factory function to create config with shared proxy_configs and custom overrides"""
            return AsyncConfig(
                proxy_configs=self.proxy_configs,
                use_proxies=self.use_proxies,
                **overrides,
            )

        configs = {
            "summary_extraction": create_config(
                parsing_script_path=os.path.join(
                    scraper_scripts_dir, "extract_summary.js"
                ),
            ),
            "search_pages": create_config(
                parsing_script_path=os.path.join(
                    scraper_scripts_dir, "parse_search_page.js"
                ),
            ),
            "listing_pages": create_config(
                parsing_script_path=os.path.join(
                    scraper_scripts_dir, "parse_listing_page.js"
                ),
            ),
            "distance": create_config(
                max_concurrent=5,
            ),
        }

        return configs

    def _save_json(self, filename, data):
        """Save data to a JSON file."""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_json_file(self, filename):
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _load_yaml_file(self, filename):
        with open(filename, "r") as f:
            return yaml.safe_load(f)

    def _normalize_merge_validate_save(
        self, db_listings, new_listings, merge_label, file_path
    ):
        """Merge and validate listings data."""
        normalized_db_listings = normalize_listings(db_listings)
        flat_new_listings = flatten_listings(new_listings)
        normalized_new_listings = normalize_listings(flat_new_listings)
        merged_data = merge_listings(normalized_db_listings, normalized_new_listings)
        self._save_json(file_path, merged_data)
        return merged_data

    async def _run_scraper(self, config_key, urls):
        """Helper method to run AsyncScraper with given config and URLs"""
        config = self.scraper_configs[config_key]
        config.urls = urls

        scraper = AsyncScraper(
            config=config,
        )

        return await scraper.process_all(urls)

    async def scrape_search_pages(self):
        """Step 2: Scrape all search result pages."""
        self.logger.info("Scraping search result pages...")

        try:
            self.search_summary = await self._run_scraper(
                "summary_extraction", [self.base_url]
            )
            self.logger.info("Successfully extracted summary information")
            try:
                self.total_pages = math.ceil(
                    self.search_summary[0]["listings"] / self.listings_per_page
                )
            except (KeyError, IndexError, TypeError):
                pass

            search_urls = generate_search_page_urls(self.base_url, self.total_pages)
            search_results = await self._run_scraper("search_pages", search_urls)
            for result in search_results:
                if "search_results" in result:
                    self.current_search_listings.extend(result["search_results"])
            self._save_json(self.image_urls_file, self.current_search_listings)

            self.logger.info(
                f"Found {len(self.current_search_listings)} listings in search"
            )
            self.current_search_ids = {
                listing["offer_id"] for listing in self.current_search_listings
            }
            self.missing_listings = self.db_active_ids - self.current_search_ids
            for offer_id in self.missing_listings:
                listing = {"offer_id": offer_id, "is_unpublished": True}
                self.current_search_listings.append(listing)

            self.merged_data = self._normalize_merge_validate_save(
                self.db_listings,
                self.current_search_listings,
                "SEARCH MERGE",
                self.json_interm,
            )
            self.logger.info(
                f"Intermediate merge complete with {len(self.merged_data)} listings"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed on search scraping: {e}")
            return False

    async def scrape_individual_listings(self):
        """Step 3: Identify and scrape listings need to be scraped individually."""

        try:
            self.logger.info("Identifying listings that need detailed scraping...")
            self.listings_to_scrape = []

            if self.update_current_search_listings:
                self.logger.info(
                    f"Added {len(self.current_search_ids)} "
                    f"listings from search to scrape"
                )
                self.listings_to_scrape.extend(self.current_search_ids)

            elif self.should_scrape_new:
                new_listings = self.current_search_ids - self.db_active_ids
                self.logger.info(f"Added {len(new_listings)} new listings to scrape")
                self.listings_to_scrape.extend(new_listings)

            if self.check_if_unpublished:
                self.logger.info(
                    f"Added {len(self.missing_listings)} missing listings to scrape"
                )
                self.listings_to_scrape.extend(self.missing_listings)

            if self.check_missing_estimations:
                self.logger.info(
                    f"Added {len(self.db_active_ids_missing_estimation)} "
                    f"active listings with missing price estimation"
                )
                self.listings_to_scrape.extend(self.db_active_ids_missing_estimation)

            if not self.listings_to_scrape:
                self.logger.info("No listings to scrape")
                return True

            self.logger.info(
                f"Scraping {len(self.listings_to_scrape)} individual listing pages..."
            )
            listing_page_urls = generate_listing_page_urls(self.listings_to_scrape)
            self.parsed_listings = await self._run_scraper(
                "listing_pages", listing_page_urls
            )
            self._save_json(self.parsed_listings_file, self.parsed_listings)
            self.merged_data = self._normalize_merge_validate_save(
                self.merged_data,
                self.parsed_listings,
                "PARSED MERGE",
                self.json_file_path,
            )
            self.logger.info(
                f"Final merge complete with {len(self.merged_data)} listings"
            )

            return True
        except Exception as e:
            self.logger.error(f"Failed to identify listings to scrape: {e}")
            return False

    async def get_distance(self):
        """Step 4: Update distances."""
        self.logger.info("Calculating and updating distances...")

        try:
            listings_missing_distance = []
            for listing in self.merged_data:
                if listing.get("distance") is None:
                    offer_id = listing.get("offer_id", "unknown")
                    address = listing.get("address", "")
                    if address:
                        listings_missing_distance.append(
                            {"offer_id": offer_id, "address": address}
                        )

            distance_data = await get_distance(
                listings_missing_distance,
                self.ref_coords,
                self.scraper_configs["distance"],
            )
            self._normalize_merge_validate_save(
                self.merged_data, distance_data, "DISTANCE", self.json_file_path
            )
            self.logger.info("Successfully updated distances")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update distances: {e}")
            return False

    def transform_and_convert_to_csv(self):
        """Step 5: Convert JSON data to CSV format."""
        try:

            df = pd.DataFrame(self.merged_data)
            df.to_csv(self.csv_file, index=False, encoding="utf-8")

            self.logger.info(f"Successfully converted data to CSV at {self.csv_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to transform and convert to CSV: {e}")
            return False

    def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up resources...")

        try:
            gc.collect()
            self.logger.info("Successfully cleaned up resources")
            return True
        except Exception as e:
            self.logger.error(f"Failed to clean up resources: {e}")
            return False

    async def run(self):
        """Run the complete pipeline."""
        self.logger.info("Starting scraper pipeline...")

        try:
            # Step 0: Setup
            self.setup()

            # Step 1: Scrape search pages
            if not await self.scrape_search_pages():
                raise Exception("Failed at search page scraping step")

            # Step 2: Scrape listing pages
            if not await self.scrape_individual_listings():
                raise Exception("Failed at listing page scraping step")

            # Step 3: Add distances
            if not await self.get_distance():
                raise Exception("Failed at distance calculation step")

            # Step 4: Transform data
            if not self.transform_and_convert_to_csv():
                raise Exception("Failed at data transformation step")

            # Step 5: Cleanup
            self.cleanup()

            self.logger.info("Pipeline completed successfully")
            return self.merged_data
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.cleanup()  # Always try to cleanup even on failure
            raise

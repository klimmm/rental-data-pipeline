import os
import copy
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

from data_process_utils.json_to_csv import convert_json_to_csv
from data_process_utils.transform import transform_listings_data
from data_process_utils.normalize_data import normalize_listings
from data_process_utils.merge_data import merge_data
from data_process_utils.validation import validate_merge
from scraper.scraper_config import AsyncConfig
from scraper.async_scraper import AsyncScraper
from scraper.distance import calculate_and_update_distances

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
    ):

        self.check_missing_estimations = check_missing_estimations
        self.check_if_unpublished = check_if_unpublished
        self.update_current_search_listings = update_current_search_listings
        self.should_scrape_new = True
        self.should_hadle_errors = True
        self.json_file_path = os.path.join(data_dir, "merged_listings.json")
        self.json_distance = os.path.join(data_dir, "merged_listings_distance.json")
        self.search_config_path = search_config_path
        self.logger = logging.getLogger(__name__)

        # Derive file paths for output files
        self.base_dir = data_dir
        self.image_urls_file = os.path.join(self.base_dir, "image_urls.json")
        self.csv_file = os.path.join(self.base_dir, "combined_data.csv")
        self.json_interm = os.path.join(self.base_dir, "merged_listings_interm.json")
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

        # Data containers
        self.search_summary = None
        self.current_search_listings = []
        self.db_listings = None
        self.merged_data = None
        self.listings_to_scrape = []
        self.parsed_listings = None

    def setup(self):
        """Set up configurations and prepare for scraping."""
        # Load search configuration
        self.search_config = self._load_yaml_file(self.search_config_path)
        self.base_url = construct_search_url(self.search_config)
        self.logger.info(f"Base URL: {self.base_url}")
        # Initialize scraper configurations
        self.scraper_configs = self._get_scraper_configs()

        # Load existing data
        self.db_listings = self._load_json_file(self.json_file_path) or []
        self.logger.info(f"Loaded {len(self.db_listings)} existing listings")
        self.logger.info(
            f"DEBUG: use_proxies={self.use_proxies}, self.proxy_configs={len(self.proxy_configs)}"
        )
        self.logger.info(
            f"DEBUG: use_proxies={self.check_missing_estimations}, self.check_if_unpublished={self.check_if_unpublished} "
            f"self.update_current_search_listings={self.update_current_search_listings} "
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
        normalize_listings(new_listings)
        # original_data = copy.deepcopy(db_listings)
        merged_data = merge_data(db_listings, new_listings)
        # validate_merge(original_data, new_listings, merged_data, merge_label)
        self._save_json(file_path, merged_data)

        return merged_data

    async def _run_scraper(self, config_key, urls):
        """Helper method to run AsyncScraper with given config and URLs"""
        config = self.scraper_configs[config_key]
        config.urls = urls

        # Create scraper instance - AsyncScraper will load parsing script from config
        scraper = AsyncScraper(
            config=config,
            proxy_configs=self.proxy_configs if self.use_proxies else [],
            cookies=None,
        )

        # Scrape URLs
        return await scraper.scrape_all(urls)

    async def extract_summary(self):
        """Step 1: Extract summary information to determine total pages."""
        self.logger.info("Extracting summary information...")

        try:
            self.search_summary = await self._run_scraper(
                "summary_extraction", [self.base_url]
            )
            self.logger.info("Successfully extracted summary information")
            return True
        except Exception as e:
            self.logger.error(f"Failed to extract summary: {e}")
            return False

    async def scrape_search_pages(self):
        """Step 2: Scrape all search result pages."""
        self.logger.info("Scraping search result pages...")

        try:
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

    def identify_listings_to_scrape(self):
        """Step 3: Identify which listings need to be scraped individually."""
        self.logger.info("Identifying listings that need detailed scraping...")

        try:
            # Get IDs from current search results
            self.current_search_ids = {
                listing["offer_id"] for listing in self.current_search_listings
            }

            # Get IDs from database, separated by status
            self.db_active_ids = {
                listing["offer_id"]
                for listing in self.db_listings
                if not listing.get("metadata", {}).get("is_unpublished", False)
            }

            # Get IDs of listings in database needing price estimation
            self.missing_price_estimation_ids = {
                listing["offer_id"]
                for listing in self.db_listings
                if not listing.get("estimated_price", "")
            }

            self.errors_404 = {
                listing["offer_id"]
                for listing in self.db_listings
                if not listing.get("estimated_price", "")
            }

            # Initialize our main collection of listings to scrape
            self.listings_to_scrape = []

            # Process each type of listing through dedicated methods
            if self.should_scrape_new:
                newly_discovered_listings = self.current_search_ids - self.db_active_ids
                self.logger.info(
                    f"Found {len(newly_discovered_listings)} new listings to scrape"
                )
                self.listings_to_scrape.extend(newly_discovered_listings)

            if self.check_if_unpublished:
                listings_to_check_unpublished = (
                    self.db_active_ids - self.current_search_ids
                )
                self.logger.info(
                    f"Will mark {len(listings_to_check_unpublished)} listings as unpublished"
                )
                self.listings_to_scrape.extend(listings_to_check_unpublished)

            if self.check_missing_estimations:
                active_listings_needing_estimation = (
                    self.missing_price_estimation_ids & self.db_active_ids
                )
                self.logger.info(
                    f"Found {len(active_listings_needing_estimation)} active listings with missing price estimation"
                )
                self.listings_to_scrape.extend(active_listings_needing_estimation)
            if self.update_current_search_listings:
                self.listings_to_scrape.extend(self.current_search_ids)

            self.logger.info(
                f"Total listings to scrape: {len(self.listings_to_scrape)}"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to identify listings to scrape: {e}")
            return False

    async def scrape_listings(self):
        """Step 4: Scrape individual listing detail pages."""

        if not self.listings_to_scrape:
            self.logger.info("No listings to scrape")
            return True

        self.logger.info(
            f"Scraping {len(self.listings_to_scrape)} individual listing pages..."
        )

        try:
            listing_page_urls = generate_listing_page_urls(self.listings_to_scrape)
            self.parsed_listings = await self._run_scraper(
                "listing_pages", listing_page_urls
            )
            if self.check_if_unpublished:
                self.hadle_errors()
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
            self.logger.error(f"Failed to scrape listing pages: {e}")
            return False

    def hadle_errors(self):
        """Identify listings that should be removed from the database and remove them."""
        ids_to_mark_unpublished = self.db_active_ids - self.current_search_ids
        if ids_to_mark_unpublished:
            for offer_id in ids_to_mark_unpublished:
                listing = {"offer_id": offer_id, "metadata": {"is_unpublished": True}}
                self.parsed_listings.append(listing)

    def transform_data(self):
        """Step 5: Transform listings data."""
        self.logger.info("Transforming listings data...")

        try:
            transform_listings_data(self.merged_data)
            self.logger.info("Successfully transformed listings data")
            return True
        except Exception as e:
            self.logger.error(f"Failed to transform data: {e}")
            return False

    async def calculate_distances(self):
        """Step 6: Calculate and update distances."""
        self.logger.info("Calculating and updating distances...")

        try:
            data_with_distance = await calculate_and_update_distances(
                self.merged_data, proxy_configs=self.proxy_configs
            )
            self._save_json(self.json_distance, data_with_distance)
            self.logger.info("Successfully calculated and updated distances")
            return True
        except Exception as e:
            self.logger.error(f"Failed to calculate distances: {e}")
            return False

    def convert_to_csv(self):
        """Step 8: Convert JSON data to CSV format."""
        self.logger.info("Converting JSON to CSV...")

        try:
            convert_json_to_csv(output_file=self.csv_file, listings=self.merged_data)
            self.logger.info(f"Successfully converted data to CSV at {self.csv_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to convert to CSV: {e}")
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

            # Step 1: Extract summary
            if not await self.extract_summary():
                raise Exception("Failed at summary extraction step")

            # Step 2: Scrape search pages
            if not await self.scrape_search_pages():
                raise Exception("Failed at search page scraping step")

            # Step 3: Identify listings to scrape
            if not self.identify_listings_to_scrape():
                raise Exception("Failed at identifying listings to scrape step")

            # Step 4: Scrape listing pages
            if not await self.scrape_listings():
                raise Exception("Failed at listing page scraping step")

            # Step 5: Transform data
            if not self.transform_data():
                raise Exception("Failed at data transformation step")

            # Step 6: Calculate distances
            if not await self.calculate_distances():
                raise Exception("Failed at distance calculation step")

            # Step 7: Convert to CSV
            if not self.convert_to_csv():
                raise Exception("Failed at CSV conversion step")

            # Step 8: Cleanup
            self.cleanup()

            self.logger.info("Pipeline completed successfully")
            return self.merged_data
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.cleanup()  # Always try to cleanup even on failure
            raise

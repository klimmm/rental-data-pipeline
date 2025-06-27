from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class AsyncConfig:
    """Configuration for AsyncHttpProcessor adapted from AsyncScraperConfig"""

    # Concurrency settings (direct reuse)
    max_concurrent: int = 2
    max_requests_per_session: int = 20
    max_pages_per_browser: int = 10
    max_retries: int = 5
    use_proxies: bool = True

    timeout: int = 30

    follow_redirects: bool = True
    verify_ssl: bool = True

    additional_headers: Dict[str, str] = field(default_factory=dict)

    parsing_script_path: str = "js/parse_listing_page.js"
    results_filename: str = None  # Auto-generated if None
    cookies_path: str = None
    use_cookies: bool = False

    urls: List[str] = field(default_factory=list)
    proxy_configs: List[Dict] = field(default_factory=list)

    # Core settings
    headless: bool = True
    navigation_timeout: int = 30000
    wait_until: str = "domcontentloaded"  # Faster than networkidle

    # Performance
    block_images: bool = True
    block_fonts: bool = True

    # Browser behavior
    locale: str = "ru-RU"
    timezone_id: str = "Europe/Moscow"
    color_scheme: str = "light"
    js_enabled: bool = True

    viewports: List[Dict[str, int]] = field(
        default_factory=lambda: [
            {"width": 1920, "height": 1080},  # Full HD
            {"width": 1366, "height": 768},  # Common laptop
            {"width": 1440, "height": 900},  # MacBook Air
            {"width": 1536, "height": 864},  # Windows laptop
            {"width": 1600, "height": 900},  # 16:9 monitor
        ]
    )

    accept_language: str = "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
    proxy_accept_languages = {
        "none": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "italy": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        "france": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "netherlands": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        "germany": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "switzerland": "de-CH,de;q=0.9,fr-CH;q=0.8,fr;q=0.7,it-CH;q=0.6,it;q=0.5,en;q=0.4",
        "spain": "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
        "uk": "en-GB,en;q=0.9,en-US;q=0.8",
        # Northern Europe
        "finland": "fi-FI,fi;q=0.9,en-US;q=0.8,en;q=0.7,sv-FI;q=0.6",
        # Eastern Europe
        "poland": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "latvia": "lv-LV,lv;q=0.9,en-US;q=0.8,en;q=0.7,ru;q=0.6",
        "hungary": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
        "moldova": "ro-MD,ro;q=0.9,ru;q=0.8,en-US;q=0.7,en;q=0.6",
        "russia": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        # Central Asia
        "kazakhstan": "kk-KZ,kk;q=0.9,ru;q=0.8,en-US;q=0.7,en;q=0.6",
        # East Asia
        "japan": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        "hong_kong": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        # Middle East
        "turkey": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        # North America
        "usa": "en-US,en;q=0.9",
        "canada": "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7",
    }

    user_agents: List[str] = field(
        default_factory=lambda: [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59 Safari/537.36",
        ]
    )
    proxy_user_agents = {
        "none": "PythonGeocoder/1.0",
        "italy": "ItaliaMapServices/2.3.1 (support@mapservices.it)",
        "netherlands": "NLRoutePlanner/3.4.2 (https://routeplanner-nl.com/contact)",
        "finland": "NordicLocationFinder/2.1.0 (info@nordic-location.fi)",
        "uk": "UKPropertyDistance/3.0.5 (api@property-distance.co.uk)",
        "kazakhstan": "CentralAsiaGeoAPI/1.7.2 (https://geoapi.kz)",
        "moldova": "EasternEuropeLocator/2.4 (support@locator-ee.md)",
        "switzerland": "AlpineRouteCalculator/3.2.1 (info@alpine-routes.ch)",
        "france": "FranceGeocodeur/2.6.3 (contact@geocodeur.fr)",
        "russia": "MoscowMetroProximity/3.5.0 (https://metro-proximity.ru)",
        # Unsuccessful proxies
        "japan": "TokyoAddressResolver/2.8.4 (info@address-resolver.jp)",
        "turkey": "AnatolianGeocodingAPI/3.1.2 (support@geocoding-tr.com.tr)",
        "poland": "PolishLocationServices/2.0.5 (contact@location-services.pl)",
        "latvia": "BalticCoordinatesMapper/1.9.3 (https://coordinates.lv)",
        "hong_kong": "AsiaProximityCalculator/3.7.1 (api@proximity-calc.hk)",
        "spain": "IberianRouteFinder/2.4.6 (support@route-finder.es)",
        "canada": "CanadianPropertyLocator/3.2.0 (info@property-locator.ca)",
        "hungary": "CentralEuropeGeoTools/2.1.3 (https://geotools-ce.hu)",
        "germany": "DeutschlandAddressAPI/3.6.2 (contact@address-api.de)",
        "usa": "USGeocodingServices/4.0.1 (support@geocoding-usa.com)",
    }
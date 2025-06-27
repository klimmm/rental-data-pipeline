"""VPN Manager - Main module coordinating proxy and xray functionality."""

import json
from pathlib import Path
from typing import Dict, List, Optional
import time
import subprocess
import tempfile

try:
    from .xray_config_generator import XrayConfigGenerator
    from .singbox_config_generator import SingboxConfigGenerator
    from .server_performance import run_speed_test
    from .server_parser import ServerParser
except ImportError:
    from xray_config_generator import XrayConfigGenerator
    from singbox_config_generator import SingboxConfigGenerator
    from server_performance import run_speed_test
    from server_parser import ServerParser


class VPNManager:
    """Manages VPN connections for scraping through multiple servers."""

    def __init__(
        self,
        servers_dir: str = None,
        vpn_type: str = "singbox",
    ):
        """Initialize VPN Manager."""
        import os

        base_dir = os.path.dirname(__file__)
        if servers_dir is None:
            servers_dir = os.path.join(base_dir, "servers")
        self.servers_dir = Path(servers_dir)
        self.vpn_type = vpn_type.lower()
        # Update servers from subscription URL before loading
        self.vpn_servers = self._load_vpn_servers()
        self.speed_results = {}
        self.sorted_servers = None
        self.combined_config = None  # Store the generated config
        self.run_vpn()
        self.proxy_mapping = self._create_proxy_mapping()
        self._speed_test_completed = False
        # Don't run speed test on init - run it lazily when needed

    def _load_vpn_servers(self) -> List[Dict]:
        """Load all VPN server configurations from the servers directory."""
        parser = ServerParser()
        parser.update_servers(servers_dir=self.servers_dir)

        servers = []
        for file_path in self.servers_dir.glob("*.json"):
            with open(file_path, "r") as f:
                server_config = json.load(f)
                servers.append(server_config)
        print(f" loaded{len(servers)} servers")
        return servers

    def _create_proxy_mapping(self) -> Dict[str, Dict[str, int]]:
        """Create proxy port mappings by generating combined config using vpn_config_extractor.

        Returns a mapping of server name to dict with 'socks' and 'http' ports.
        Uses the config generators to create a combined config and extract port assignments.
        """
        proxy_mapping = {}

        try:
            # Use the already generated combined config
            if not self.combined_config:
                raise Exception("No combined config available")

            # Extract port mappings from inbound configurations
            inbounds = self.combined_config.get("inbounds", [])

            # Group inbounds by server (each server has socks and http inbounds)
            server_ports = {}

            for inbound in inbounds:
                # Extract server name from tag
                tag = inbound.get("tag", "")
                port = inbound.get("port") or inbound.get("listen_port")
                if tag.startswith("socks-"):
                    server_name = tag[6:]  # Remove 'socks-' prefix
                    if server_name not in server_ports:
                        server_ports[server_name] = {}

                    server_ports[server_name]["socks"] = port

                elif tag.startswith("http-"):
                    server_name = tag[5:]  # Remove 'http-' prefix
                    if server_name not in server_ports:
                        server_ports[server_name] = {}
                    server_ports[server_name]["http"] = port

            # Validate that all servers have both socks and http ports
            for server_name, ports in server_ports.items():
                if "socks" in ports and "http" in ports:
                    proxy_mapping[server_name] = ports
                else:
                    print(
                        f"Warning: Incomplete port mapping for server {server_name}: {ports}"
                    )

        except Exception as e:
            print(f"Error: Could not create proxy mapping: {e}")
            return {}

        return proxy_mapping

    def _get_available_servers(self, exclude_countries=None) -> List[Dict]:
        """Get list of available servers excluding specified ones.

        Args:
            exclude_servers: List/set of server names to exclude
            exclude_countries: List/set of country names to exclude

        Returns:
            List of available server configurations
        """
        if not self.sorted_servers:
            return []

        available_servers = self.sorted_servers

        # Filter by excluded countries
        if exclude_countries:
            available_servers = [
                server
                for server in available_servers
                if server["country"] not in exclude_countries
            ]

        return available_servers

    def _generate_combined_config(self) -> bool:
        """Generate combined VPN configuration for all servers.

        Returns:
            True if config generation succeeded, False otherwise
        """
        try:
            if self.vpn_type == "xray":
                generator = XrayConfigGenerator()
            elif self.vpn_type == "singbox":
                generator = SingboxConfigGenerator()
            else:
                print(f"❌ Unsupported VPN type: {self.vpn_type}")
                return False

            # Generate combined config for all servers
            self.combined_config = generator.generate_combined_config(self.vpn_servers)
            if not self.combined_config:
                print(f"❌ Failed to generate {self.vpn_type} configuration")
                return False

            return True

        except Exception as e:
            print(f"❌ Failed to generate {self.vpn_type} config: {e}")
            return False

    def _run_speed_test_and_sort_servers(self):
        """Run speed test on all servers and get pre-sorted results."""
        self.speed_results = run_speed_test(self.vpn_servers, self.proxy_mapping)
        self.sorted_servers = []
        for server_name, latency in self.speed_results.items():
            server = next(s for s in self.vpn_servers if s["name"] == server_name)
            self.sorted_servers.append(server)

    def _create_proxy_config(self, server_name: str) -> Optional[Dict]:
        """Create proxy configuration for a server."""
        ports = self.proxy_mapping.get(server_name)
        if ports and "socks" in ports and "http" in ports:
            proxy = {
                "server": f"http://127.0.0.1:{ports['http']}",
                "socks5_server": f"socks5://127.0.0.1:{ports['socks']}",
                "server_name": server_name,
            }
            return proxy
        return None

    def get_proxy(self, identifier, exclude_countries=["russia"], country=None) -> Optional[Dict]:
        """Get proxy by index (returns best servers first), by server name, or by country.

        Args:
            identifier: String server name or country name, or numeric index
            exclude_countries: Optional list/set of country names to exclude when using numeric index
            country: Optional country name to get proxy for (overrides identifier)
        Returns:
            Dict with proxy config, None for direct connection, or raises exception
        """
        
        # Run speed test lazily if not already done and identifier is int (needs sorted servers)
        if isinstance(identifier, int) and not self._speed_test_completed:
            self._run_speed_test_and_sort_servers()
            self._speed_test_completed = True

        # Determine the target country from identifier or country parameter
        target_country = None
        if country:
            target_country = country.lower()
        elif isinstance(identifier, str):
            # Check if identifier might be a country name
            country_servers = [s for s in self.sorted_servers or [] if s["country"].lower() == identifier.lower()]
            if country_servers:
                target_country = identifier.lower()

        # Don't exclude the target country if it's specifically requested
        effective_exclude_countries = exclude_countries.copy() if exclude_countries else []
        if target_country and target_country in [c.lower() for c in effective_exclude_countries]:
            effective_exclude_countries = [c for c in effective_exclude_countries if c.lower() != target_country]

        available_servers = self._get_available_servers(effective_exclude_countries)

        if not available_servers:
            print("⚠️ No working VPN servers available, using direct connection")
            return None

        # If country is specified, filter by country and get the best server
        if country:
            country_servers = [s for s in available_servers if s["country"].lower() == country.lower()]
            if not country_servers:
                raise Exception(f"No working VPN servers available for country '{country}'")
            # Get the best (first) server for this country
            server = country_servers[0]
            proxy = self._create_proxy_config(server["name"])
            return proxy

        if isinstance(identifier, str):
            # First check if it's a server name
            server_by_name = next((s for s in available_servers if s["name"] == identifier), None)
            if server_by_name:
                proxy = self._create_proxy_config(identifier)
                return proxy
            
            # If not a server name, try it as a country name
            country_servers = [s for s in available_servers if s["country"].lower() == identifier.lower()]
            if country_servers:
                # Get the best (first) server for this country
                server = country_servers[0]
                proxy = self._create_proxy_config(server["name"])
                return proxy
            
            # If neither server name nor country, raise exception
            raise Exception(f"No VPN server or country found matching '{identifier}'")

        server = available_servers[identifier % len(available_servers)]
        server_name = server["name"]
        proxy = self._create_proxy_config(server_name)

        return proxy

    def get_all_proxies(self, exclude_countries=["russia", "hong kong"]) -> List[Dict]:
        """Get all proxy configurations for sorted servers.

        Args:
            exclude_countries: Optional list/set of country names to exclude

        Returns:
            List of proxy configurations for all available servers
        """
        # Run speed test lazily if not already done
        if not self._speed_test_completed:
            self._run_speed_test_and_sort_servers()
            self._speed_test_completed = True
        
        available_servers = self._get_available_servers(exclude_countries)

        proxies = []
        for server in available_servers:
            proxy_config = self._create_proxy_config(server["name"])
            if proxy_config:
                proxies.append(proxy_config)

        return proxies

    def get_all_proxies_no_speed_test(self, exclude_countries=["russia", "hong kong"]) -> List[Dict]:
        """Get all proxy configurations without running speed tests.

        Args:
            exclude_countries: Optional list/set of country names to exclude

        Returns:
            List of proxy configurations for all servers (unsorted)
        """
        available_servers = []
        
        # Filter servers by excluded countries
        for server in self.vpn_servers:
            if exclude_countries:
                if server["country"] not in exclude_countries:
                    available_servers.append(server)
            else:
                available_servers.append(server)

        proxies = []
        for server in available_servers:
            proxy_config = self._create_proxy_config(server["name"])
            if proxy_config:
                proxies.append(proxy_config)

        return proxies

    def run_vpn(self) -> bool:
        """Ensure VPN (Xray or Singbox) is running with the provided configuration."""
        # Generate combined config
        if not self._generate_combined_config():
            return False

        # Save combined config to temporary file
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(self.combined_config, f, indent=2)
                config_path = f.name
        except Exception as e:
            print(f"❌ Failed to save config to temporary file: {e}")
            return False

        # Check and stop any existing VPN processes
        vpn_processes = ["xray", "sing-box"]
        for process_name in vpn_processes:
            try:
                result = subprocess.run(
                    ["pgrep", process_name], capture_output=True, text=True
                )
                if result.returncode == 0:
                    print(
                        f"🔄 Stopping existing {process_name} processes..."
                    )
                    pids = result.stdout.strip().split("\n")
                    for pid in pids:
                        if pid:
                            print(f"🛑 Stopping existing {process_name} process {pid}")
                            subprocess.run(["kill", pid], capture_output=True)
                    time.sleep(0.1)  # Wait for processes to stop
            except Exception:
                # If we can't check or kill processes, just continue
                pass

        print(f"⚠️ Starting {self.vpn_type}...")

        try:
            print(f"🚀 Starting {self.vpn_type} with configuration: {config_path}")

            if self.vpn_type == "xray":
                vpn_process = subprocess.Popen(
                    ["xray", "run", "-c", config_path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            elif self.vpn_type == "singbox":
                vpn_process = subprocess.Popen(
                    ["sing-box", "run", "-c", config_path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

            time.sleep(1)  # Wait a moment for VPN to start

            if vpn_process.poll() is None:
                time.sleep(1)  # Wait a moment for VPN to start
                print(f"✅ {self.vpn_type.capitalize()} started successfully")
                return True
            else:
                print(f"❌ {self.vpn_type.capitalize()} failed to start")
                return False
        except Exception as e:
            print(f"❌ Failed to start {self.vpn_type}: {e}")
            return False

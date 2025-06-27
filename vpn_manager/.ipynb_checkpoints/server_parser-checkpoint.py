#!/usr/bin/env python3
"""Server Parser - Extract and parse VPN server configurations from URLs."""

import requests
import base64
import re
import urllib.parse
from typing import List, Dict, Optional


class ServerParser:
    """Parser for extracting VPN server configurations from subscription URLs."""

    def __init__(self):
        """Initialize the server parser."""
        self.country_translations = {
            "США": "usa",
            "Япония": "japan",
            "Венгрия": "hungary",
            "Латвия": "latvia",
            "Франция": "france",
            "Финляндия": "finland",
            "Молдова": "moldova",
            "Канада": "canada",
            "Швейцария": "switzerland",
            "Гонконг": "hong kong",
            "Англия": "uk",
            "Польша": "poland",
            "Испания": "spain",
            "Нидерланды": "netherlands",
            "Казахстан": "kazakhstan",
            "Турция": "turkey",
            "Германия": "germany",
            "Россия": "russia",
            "Италия": "italy",
        }

    def fetch_vpn_config(self, url: str) -> Optional[str]:
        """Fetch the VPN configuration data from the URL."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def decode_content(self, content: str) -> str:
        """Decode base64 content if needed."""
        try:
            # Try to decode as base64 first
            decoded_content = base64.b64decode(content).decode("utf-8")
            print("Decoded base64 content successfully")
            return decoded_content
        except:
            # If it fails, assume it's already in plain text
            print("Content is not base64 encoded, using as-is")
            return content

    def extract_country(self, comment: str) -> str:
        """Extract country name from the comment and translate to English lowercase."""
        if not comment:
            return "unknown"

        # Try to extract the country name (non-emoji part)
        match = re.search(r"[^🏁🏴🏳️⚐⚑🚩🎌🏴‍☠️\s]+$", comment)
        if match:
            country_name = match.group(0).strip()
            # Translate if available in our dictionary
            return self.country_translations.get(country_name, country_name.lower())

        return comment.strip().lower()

    def parse_vless_url(self, vless_url: str) -> Dict:
        """Parse a vless URL into a dictionary of configuration parameters."""
        try:
            # Extract the comment part (usually contains the country or server name)
            url_parts = vless_url.split("#", 1)
            main_url = url_parts[0]
            comment = url_parts[1] if len(url_parts) > 1 else ""

            # Extract the protocol
            protocol_parts = main_url.split("://", 1)
            protocol = protocol_parts[0]

            # Extract user ID and server info
            remaining = protocol_parts[1]
            user_server_parts = remaining.split("@", 1)
            user_id = user_server_parts[0]

            # Extract server and params
            server_params = user_server_parts[1].split("?", 1)
            server_info = server_params[0]

            # Extract host and port
            if ":" in server_info:
                host, port = server_info.split(":", 1)
            else:
                host = server_info
                port = ""

            # Parse parameters
            params = {}
            if len(server_params) > 1:
                param_string = server_params[1]
                param_pairs = param_string.split("&")
                for pair in param_pairs:
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        params[key] = urllib.parse.unquote(value)
                    else:
                        params[pair] = ""

            # Extract country from comment
            country = self.extract_country(comment)

            # Create the configuration dictionary
            config = {
                "protocol": protocol,
                "user_id": user_id,
                "host": host,
                "port": port,
                "params": params,
                "comment": comment,
                "country": country,
            }

            return config

        except Exception as e:
            print(f"Error parsing vless URL: {e}")
            print(f"Problematic URL: {vless_url}")
            return {"error": str(e), "url": vless_url}

    def update_servers(self, url: str) -> List[Dict]:
        """Parse all servers from a subscription URL."""
        print(f"Fetching VPN configurations from: {url}")
        content = self.fetch_vpn_config(url)
        if not content:
            print("Failed to fetch VPN configurations. Please check the URL and try again.")
            return []

        print(f"Successfully fetched configuration data ({len(content)} bytes)")

        # Decode if base64 encoded
        content = self.decode_content(content)

        # Split the content into lines
        lines = content.strip().split("\n")
        print(f"Found {len(lines)} configuration entries")

        # Parse each configuration
        configs = []
        urls = []
        for line in lines:
            # Skip empty lines
            if not line.strip():
                continue
            urls.append(line)
            # Parse the vless URL
            try:
                config = self.parse_vless_url(line)
                if "error" not in config:  # Only add valid configs
                    configs.append(config)
            except Exception as e:
                print(f"Error parsing a configuration line: {e}")

        print(f"Successfully parsed {len(configs)} valid configurations")
        
        # Convert to generic format before returning
        generic_configs = self.convert_to_generic_format(configs)
        if urls:
            print("")
            # print(urls)
        print(f"Converted {len(generic_configs)} servers to generic format")
        return generic_configs

    def convert_to_generic_format(self, servers: List[Dict]) -> List[Dict]:
        """Convert servers to generic format and return as list of JSONs."""
        generic_servers = []
        country_counters = {}
        
        for server in servers:
            try:
                country = server.get("country", "unknown")
                
                # Generate unique name for servers from same country
                if country in country_counters:
                    country_counters[country] += 1
                    unique_name = f"{country}_{country_counters[country]}"
                else:
                    country_counters[country] = 1
                    unique_name = country
                
                # Create generic server configuration
                generic_config = {
                    "name": unique_name,
                    "country": country,
                    "protocol": server.get("protocol", "vless"),
                    "server": {
                        "host": server.get("host", ""),
                        "port": int(server.get("port", 0)) if server.get("port") else 0
                    },
                    "auth": {
                        "user_id": server.get("user_id", ""),
                        "flow": server.get("params", {}).get("flow", "")
                    },
                    "transport": {
                        "type": server.get("params", {}).get("type", "tcp"),
                        "security": server.get("params", {}).get("security", "reality")
                    },
                    "tls": {
                        "server_name": server.get("params", {}).get("sni", ""),
                        "fingerprint": server.get("params", {}).get("fp", "chrome"),
                        "public_key": server.get("params", {}).get("pbk", ""),
                        "short_id": server.get("params", {}).get("sid", "")
                    },
                    "metadata": {
                        "original_comment": server.get("comment", ""),
                        "original_url": server.get("url", ""),
                        "source": "subscription"
                    }
                }
                
                generic_servers.append(generic_config)
                
            except Exception as e:
                print(f"Error converting server config for {server.get('country', 'unknown')}: {e}")
        
        return generic_servers


def main():
    """Example usage of the ServerParser."""
    parser = ServerParser()
    url = "https://conn-liberty.ru/connection/subs/2e11aaf8-df3f-4bc4-b847-5903e90ebff2"
    
    # Parse servers from URL (already in generic format)
    print("🔄 Parsing servers from subscription URL...")
    servers = parser.parse_servers_from_url(url)
    
    print(f"\n📊 Found {len(servers)} servers in generic format")
    
    # Display server summary
    print("\n🌍 Available servers:")
    seen_countries = set()
    for server in servers:
        country = server.get('country', 'unknown')
        if country not in seen_countries:
            seen_countries.add(country)
            server_info = server.get('server', {})
            host = server_info.get('host', '')
            port = server_info.get('port', '')
            print(f"  - {country}: {host}:{port}")
    


if __name__ == "__main__":
    main()
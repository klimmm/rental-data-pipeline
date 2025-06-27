from typing import List, Dict, Tuple
try:
    from .base_config_generator import BaseConfigGenerator
except ImportError:
    from base_config_generator import BaseConfigGenerator


class XrayConfigGenerator(BaseConfigGenerator):
    """Generator for Xray VPN configurations."""

    def generate_outbound_config(self, server_config: Dict, tag: str) -> Dict:
        """Generate an outbound configuration for a single server.
        
        Optimized for REALITY security which is used in all servers.
        
        Works with modern server format:
        {
            "name": "canada",
            "country": "canada",
            "protocol": "vless",
            "server": {
                "host": "wintassa.site",
                "port": 8443
            },
            "auth": {
                "user_id": "794bd8b3-69c1-43f8-819d-6bf218b53f19",
                "flow": "xtls-rprx-vision"
            },
            "transport": {
                "type": "tcp",
                "security": "reality"
            },
            "tls": {
                "server_name": "yandex.ru",
                "fingerprint": "random",
                "public_key": "CfFyRy7BlzQMmXIeWQI1QaBwL8z1TVoaTFmGSRsqXHs",
                "short_id": "18260a022e1511e7"
            },
            "metadata": {
                "original_comment": "🇨🇦 Канада",
                "original_url": "",
                "source": "subscription"
            }
        }
        """
        # Modern format only - direct access to keys
        protocol = server_config["protocol"]
        server = server_config["server"]
        auth = server_config["auth"]
        transport = server_config["transport"]
        tls = server_config["tls"]
        
        host = server["host"]
        port = server["port"]
        user_id = auth["user_id"]
        flow = auth["flow"]
        network = transport["type"]
        
        # Reality settings
        reality_settings = {
            "fingerprint": tls["fingerprint"],
            "serverName": tls["server_name"],
            "publicKey": tls["public_key"],
            "shortId": tls["short_id"],
        }
        
        # Create stream settings with REALITY
        stream_settings = {
            "network": network,
            "security": "reality",  # Always REALITY
            "realitySettings": reality_settings
        }
            
        return {
            "protocol": protocol,
            "settings": {
                "vnext": [
                    {
                        "address": host,
                        "port": int(port),
                        "users": [
                            {
                                "id": user_id,
                                "flow": flow,
                                "encryption": "none",
                            }
                        ],
                    }
                ]
            },
            "streamSettings": stream_settings,
            "tag": tag,
        }

    def generate_single_server_config(self, server_config: Dict, socks_port=1080, http_port=1081) -> Dict:
        """Generate Xray config for a single server with configurable ports."""
        try:
            # Get identifier from server name
            tags = self.generate_tag_names(server_config)
            socks_tag = tags["socks"]
            http_tag = tags["http"]
            outbound_tag = tags["outbound"]
            
            xray_config = {
                "log": {"loglevel": "warning"},
                "inbounds": [
                    {
                        "port": socks_port,
                        "protocol": "socks",
                        "settings": {"auth": "noauth", "udp": True},
                        "tag": socks_tag
                    },
                    {
                        "port": http_port, 
                        "protocol": "http",
                        "tag": http_tag
                    },
                ],
                "outbounds": [
                    self.generate_outbound_config(server_config, outbound_tag),
                    {"protocol": "freedom", "tag": "direct"},
                ],
                "routing": {
                    "domainStrategy": "AsIs",
                    "rules": [
                        # All traffic, including private IPs (except 198.18.x.x) goes through the VPN
                        {"type": "field", "inboundTag": [socks_tag, http_tag], "outboundTag": outbound_tag},
                    ],
                },
            }
            return xray_config
        except Exception as e:
            print(f"Error generating single Xray config: {e}")
            return None

    def create_empty_combined_config(self) -> Dict:
        """Create an empty combined Xray config structure."""
        return {
            "log": {"loglevel": "warning"},
            "inbounds": [],
            "outbounds": [],
            "routing": {"rules": []},
        }

    def _add_routing_rule(self, combined_config: Dict, single_config: Dict) -> None:
        """Add routing rule from single Xray config to combined config."""
        # Extract the routing rule (all traffic through VPN)
        routing_rule = single_config["routing"]["rules"][0]
        combined_config["routing"]["rules"].append(routing_rule)

    def add_direct_components(self, combined_config: Dict) -> None:
        """Add direct outbound to the combined Xray config without routing rule for private IPs."""
        # Add direct freedom outbound (used as fallback only)
        combined_config["outbounds"].append({"protocol": "freedom", "tag": "direct"})
        
        # No specific rule for private IPs - they will go through the VPN per each server's routing rules
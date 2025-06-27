from typing import List, Dict, Tuple
try:
    from .base_config_generator import BaseConfigGenerator
except ImportError:
    from base_config_generator import BaseConfigGenerator

from typing import List, Dict, Tuple


class SingboxConfigGenerator(BaseConfigGenerator):
    """Generator for Sing-box VPN configurations."""

    def generate_outbound_config(self, server_config: Dict, tag: str) -> Dict:
        """Generate an outbound configuration for a single server.
        
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
        # Extract server details
        protocol = server_config["protocol"]
        server = server_config["server"]
        auth = server_config["auth"]
        transport = server_config["transport"]
        tls = server_config["tls"]
        
        # Create base outbound config
        outbound = {
            "type": protocol,
            "tag": tag,
            "server": server["host"],
            "server_port": int(server["port"]),
            "uuid": auth["user_id"],
            "flow": auth.get("flow", "")
        }
        
        # Add TLS and REALITY settings (no transport section needed for TCP)
        if transport["security"] == "reality":
            outbound["tls"] = {
                "enabled": True,
                "server_name": tls["server_name"],
                "utls": {
                    "enabled": True,
                    "fingerprint": tls["fingerprint"]
                },
                "reality": {
                    "enabled": True,
                    "public_key": tls["public_key"],
                    "short_id": tls["short_id"]
                }
            }
        
        return outbound

    def generate_single_server_config(self, server_config: Dict, socks_port=1080, http_port=1081) -> Dict:
        """Generate Sing-box config for a single server with configurable ports."""
        try:            
            tags = self.generate_tag_names(server_config)
            socks_tag = tags["socks"]
            http_tag = tags["http"]
            outbound_tag = tags["outbound"]
            
            singbox_config = {
                "log": {
                    "level": "warn",
                    "timestamp": True
                },
                "inbounds": [
                    {
                        "type": "socks",
                        "tag": socks_tag,
                        "listen": "::",
                        "listen_port": socks_port,
                        "users": []
                    },
                    {
                        "type": "http",
                        "tag": http_tag,
                        "listen": "::",
                        "listen_port": http_port,
                        "users": []
                    }
                ],
                "outbounds": [
                    self.generate_outbound_config(server_config, outbound_tag),
                    {
                        "type": "direct",
                        "tag": "direct"
                    }
                ],
                "route": {
                    "rules": [
                        # All traffic goes through the VPN
                        {
                            "inbound": [socks_tag, http_tag],
                            "outbound": outbound_tag
                        }
                    ],
                    "final": outbound_tag
                }
            }
            return singbox_config
        except Exception as e:
            print(f"Error generating single Sing-box config: {e}")
            return None

    def create_empty_combined_config(self) -> Dict:
        """Create an empty combined Sing-box config structure."""
        return {
            "log": {
                "level": "warn",
                "timestamp": True
            },
            "inbounds": [],
            "outbounds": [],
            "route": {
                "rules": [],
                "final": "direct"
            }
        }

    def _add_routing_rule(self, combined_config: Dict, single_config: Dict) -> None:
        """Add routing rule from single Sing-box config to combined config."""
        # Extract the routing rule (all traffic through VPN)
        routing_rule = single_config["route"]["rules"][0]
        combined_config["route"]["rules"].append(routing_rule)

    def add_direct_components(self, combined_config: Dict) -> None:
        """Add direct outbound to the combined Sing-box config without routing rule for private IPs."""
        # Add direct outbound (used as fallback only)
        combined_config["outbounds"].append({
            "type": "direct",
            "tag": "direct"
        })
        
        # No specific rule for private IPs - they will go through the VPN per each server's routing rules
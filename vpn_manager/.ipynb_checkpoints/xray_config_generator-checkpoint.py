from typing import Dict

try:
    from vpn_manager.base_config_generator import BaseConfigGenerator
except ImportError:
    from base_config_generator import BaseConfigGenerator


class XrayConfigGenerator(BaseConfigGenerator):
    """Generator for Xray VPN configurations."""

    def generate_outbound_config(self, config: Dict, tag: str) -> Dict:

        return {
            "protocol": config["protocol"],
            "settings": {
                "vnext": [
                    {
                        "address": config["server"]["host"],
                        "port": int(config["server"]["port"]),
                        "users": [
                            {
                                "id": config["auth"]["user_id"],
                                "flow": config["auth"]["flow"],
                                "encryption": "none",
                            }
                        ],
                    }
                ]
            },
            "streamSettings": {
                "network": config["transport"]["type"],
                "security": config["transport"]["security"],
                "realitySettings": {
                    "fingerprint": config["tls"]["fingerprint"],
                    "serverName": config["tls"]["server_name"],
                    "publicKey": config["tls"]["public_key"],
                    "shortId": config["tls"]["short_id"],
                },
            },
            "tag": tag,
        }

    def generate_single_server_config(
        self, config: Dict, socks_port=1080, http_port=1081
    ) -> Dict:
        """Generate Xray config for a single server with configurable ports."""
        try:
            tags = self.generate_tag_names(config)
            outbound_config = self.generate_outbound_config(config, tags["outbound"])
            xray_config = {
                "log": {"loglevel": "warning"},
                "inbounds": [
                    {
                        "port": socks_port,
                        "protocol": "socks",
                        "settings": {"auth": "noauth", "udp": True},
                        "tag": tags["socks"],
                    },
                    {"port": http_port, "protocol": "http", "tag": tags["http"]},
                ],
                "outbounds": [
                    outbound_config,
                    {"protocol": "freedom", "tag": "direct"},
                ],
                "routing": {
                    "domainStrategy": "AsIs",
                    "rules": [
                        {
                            "type": "field",
                            "inboundTag": [tags["socks"], tags["http"]],
                            "outboundTag": tags["outbound"],
                        },
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
        direct_components = {"protocol": "freedom", "tag": "direct"}
        combined_config["outbounds"].append(direct_components)

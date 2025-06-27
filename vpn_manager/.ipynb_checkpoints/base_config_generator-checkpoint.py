from abc import ABC, abstractmethod
from typing import List, Dict
import json
import os
from pathlib import Path


class BaseConfigGenerator(ABC):
    """Base class for VPN configuration generators."""

    def __init__(self):
        """Initialize the config generator."""
        pass

    @abstractmethod
    def generate_outbound_config(self, server_config: Dict, tag: str) -> Dict:
        """Generate an outbound configuration for a single server.
        
        Must be implemented by subclasses to create format-specific outbound configurations.
        """
        pass

    def get_server_identifier(self, server_config: Dict) -> str:
        """Extract a unique identifier from the server config.
        
        Args:
            server_config: Server configuration in modern format
            
        Returns:
            String identifier for this server
        """
        return server_config["name"]

    def generate_tag_names(self, server_config: Dict) -> Dict[str, str]:
        """Generate consistent tag names for a server.
        
        Args:
            identifier: Unique identifier for this server
            
        Returns:
            Dictionary containing tag names for socks, http, and outbound
        """
        identifier = self.get_server_identifier(server_config)
        return {
            "socks": f"socks-{identifier}",
            "http": f"http-{identifier}",
            "outbound": f"{identifier}-out"
        }

    @abstractmethod
    def generate_single_server_config(self, server_config: Dict, socks_port: int = 1080, http_port: int = 1081) -> Dict:
        """Generate config for a single server with configurable ports.
        
        Must be implemented by subclasses to create format-specific configurations.
        """
        pass

    def generate_combined_config(self, servers: List[Dict]) -> Dict:
        """Generate a combined config with all servers.
        
        This is a template method that calls abstract methods that subclasses must implement.
        """
        try:
            # Initialize empty combined config with format-specific structure
            combined_config = self.create_empty_combined_config()
            
            port_counter = 10801
            server_tags = []
            
            # Process each server individually
            for server_config in servers:
                # Each server config must have a name field
                if "name" not in server_config:
                    print(f"Warning: Server config missing name field, skipping: {server_config}")
                    continue
                
                # Generate a single server config
                temp_config = self.generate_single_server_config(
                    server_config, 
                    socks_port=port_counter,
                    http_port=port_counter+1
                )
                
                # Extract components and add to combined config
                self.add_components_to_combined_config(combined_config, temp_config, server_tags)
                
                # Increment port counter
                port_counter += 2
            
            # Add direct outbound and routing rule
            self.add_direct_components(combined_config)
            
            return combined_config
        except Exception as e:
            print(f"Error generating combined config: {e}")
            return None

    @abstractmethod
    def create_empty_combined_config(self) -> Dict:
        """Create an empty combined config with format-specific structure.
        
        Must be implemented by subclasses.
        """
        pass

    def add_components_to_combined_config(self, combined_config: Dict, single_config: Dict, server_tags: List[str]) -> None:
        """Add components from a single server config to the combined config.
        
        This is a template method that delegates routing rule extraction to subclasses.
        """
        # Extract both inbounds (SOCKS and HTTP)
        for inbound in single_config["inbounds"]:
            combined_config["inbounds"].append(inbound)
        
        # Extract the outbound
        outbound = single_config["outbounds"][0]
        combined_config["outbounds"].append(outbound)
        
        # Extract and add routing rule (implementation specific to each format)
        self._add_routing_rule(combined_config, single_config)
        
        # Track server tags
        server_tags.append(outbound["tag"])
    
    @abstractmethod
    def _add_routing_rule(self, combined_config: Dict, single_config: Dict) -> None:
        """Add routing rule from single config to combined config.
        
        Must be implemented by subclasses to handle format-specific routing structures.
        """
        pass

    @abstractmethod
    def add_direct_components(self, combined_config: Dict) -> None:
        """Add direct outbound and related routing rules to the combined config.
        
        Must be implemented by subclasses to handle format-specific components.
        """
        pass

    @staticmethod
    def load_saved_servers(servers_dir: str) -> List[Dict]:
        """Load saved server configurations from JSON files."""
        base_dir = os.path.dirname(__file__)
        servers_path = Path(base_dir) / servers_dir

        if not servers_path.exists():
            print(f"Warning: Servers directory {servers_path} does not exist")
            return []

        servers = []

        for json_file in servers_path.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    server_config = json.load(f)
                    servers.append(server_config)
            except Exception as e:
                print(f"Error loading server config from {json_file}: {e}")

        return servers

    @staticmethod
    def save_config(config: Dict, filename: str, output_dir) -> bool:
        """Save a single configuration to file."""
        try:
            # Handle both string and Path objects
            if isinstance(output_dir, str):
                base_dir = os.path.dirname(__file__)
                output_path = Path(base_dir) / output_dir
            else:
                output_path = output_dir

            output_path.mkdir(exist_ok=True)

            # Save the configuration
            file_path = output_path / filename
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)

            return True
        except Exception as e:
            print(f"Error saving configuration to {filename}: {e}")
            return False
# proxy_manager.py
import asyncio
import logging
import random
from typing import List, Dict, Optional, Set

logger = logging.getLogger("proxy_manager")

class ProxyManager:
    """Manages proxies for HTTP requests"""
    
    def __init__(self, proxy_configs: Optional[List[Dict[str, str]]] = None):
        self.proxy_configs = proxy_configs or []
        self.working_proxies = self.proxy_configs.copy()
        self.used_proxies = set()
        self.proxy_lock = asyncio.Lock()
    
    async def get_available_proxy(self):
        """Get an available proxy from the working proxies list"""
        if not self.working_proxies:
            return None
            
        async with self.proxy_lock:
            # Find proxies that aren't currently in use
            available = [
                p for p in self.working_proxies
                if p.get("server_name") not in self.used_proxies
            ]
            
            if not available:
                # All proxies are in use
                return None
                
            # Choose a random available proxy
            proxy = random.choice(available)
            
            # Mark this proxy as in use
            server_name = proxy.get("server_name")
            if server_name:
                self.used_proxies.add(server_name)
                
            return proxy
    
    async def release_proxy(self, proxy):
        """Release a proxy back to the available pool"""
        if not proxy:
            return
            
        async with self.proxy_lock:
            server_name = proxy.get("server_name")
            if server_name and server_name in self.used_proxies:
                self.used_proxies.remove(server_name)
    
    async def mark_proxy_failed(self, proxy):
        """Mark a proxy as failed and remove it from the working proxies list"""
        if not proxy:
            return
            
        async with self.proxy_lock:
            server_name = proxy.get("server_name")
            
            # Remove from used proxies if it's there
            if server_name and server_name in self.used_proxies:
                self.used_proxies.remove(server_name)
            
            # Remove from working proxies
            if proxy in self.working_proxies:
                self.working_proxies.remove(proxy)
                logger.warning(f"Proxy {server_name} marked as failed and removed from working pool")
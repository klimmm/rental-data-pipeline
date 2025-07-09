"""
Example showing how AsyncScraper and AsyncHttpProcessor could inherit from BaseAsyncProcessor
"""

import asyncio
from playwright.async_api import async_playwright
from .base_async_processor import BaseAsyncProcessor
from .performance_tracker import performance_tracker
import logging
import random

logger = logging.getLogger(__name__)


class AsyncScraper(BaseAsyncProcessor):
    """AsyncScraper refactored to inherit from BaseAsyncProcessor"""

    def __init__(self, config):
        super().__init__(config)
        
        # Handle cases where no parsing script is provided
        if config.parsing_script_path:
            with open(config.parsing_script_path, "r") as f:
                self.parsing_script = f.read()
        else:
            self.parsing_script = None


    def _create_task(self, url: str) -> dict:
        """Convert URL to task dictionary"""
        return {"url": url, "retries": 0}

    async def worker(self, worker_id: int, queue: asyncio.Queue) -> list:
        """Worker that processes URLs from the queue"""
        async with async_playwright() as playwright:
            return await self._worker_loop(worker_id, queue, playwright)

    async def _create_client(self, worker_id: int, playwright=None):
        """Create browser instance for worker"""
        proxy = await self.get_available_proxy()
        launch_options = {"headless": self.config.headless}

        if proxy:
            launch_options["proxy"] = {"server": proxy["server"]}

        browser = await playwright.chromium.launch(**launch_options)
        browser._worker_id = worker_id
        browser._proxy_name = proxy["server_name"] if proxy else None
        logger.info(f"worker_id {worker_id} using proxy {browser._proxy_name}")
        return browser

    @performance_tracker
    async def _process_task(self, browser, task: dict):
        """Process a single URL using the browser"""
        url = task["url"]
        context = None
        page = None

        try:
            context = await browser.new_context(
                user_agent=random.choice(self.config.user_agents),
                viewport=random.choice(self.config.viewports),
                locale=self.config.locale,
                timezone_id=self.config.timezone_id,
                color_scheme=self.config.color_scheme,
                extra_http_headers={
                    "Accept-Language": self.config.accept_language,
                },
                geolocation={"latitude": 55.7558, "longitude": 37.6173},
                permissions=["geolocation"],
            )

            # Apply resource blocking if configured
            if self.config.block_images:
                await context.route(
                    "**/*.{png,jpg,jpeg,gif,svg}", lambda route: route.abort()
                )

            if self.cookies:
                logger.info("adding cookies")
                await context.add_cookies(self.cookies)

            page = await context.new_page()

            await page.goto(
                url,
                timeout=self.config.navigation_timeout,
                wait_until=self.config.wait_until,
            )
            
            # Wait for critical elements if configured
            if self.config.wait_for_selector:
                element = None
                try:
                    element = await page.wait_for_selector(
                        self.config.wait_for_selector,
                        timeout=self.config.wait_for_selector_timeout,
                        state='attached'
                    )
                    
                    # Scroll to element if configured and element was found
                    if element and self.config.scroll_to_element:
                        await element.scroll_into_view_if_needed()
                        await page.wait_for_timeout(2000)  # Wait for lazy loading
                        
                except Exception as e:
                    # Try fallback selector if configured
                    if self.config.fallback_wait_for_selector:
                        try:
                            logger.info(f"Primary selector failed, trying fallback selector '{self.config.fallback_wait_for_selector}' on {url}")
                            element = await page.wait_for_selector(
                                self.config.fallback_wait_for_selector,
                                timeout=self.config.wait_for_selector_timeout,
                                state='attached'
                            )
                            
                            # Scroll to fallback element if found
                            if element and self.config.scroll_to_element:
                                await element.scroll_into_view_if_needed()
                                await page.wait_for_timeout(2000)  # Wait for lazy loading
                                
                        except Exception as fallback_e:
                            # Both primary and fallback failed
                            self._increment_retry_count(task)
                            will_retry = self._should_retry_task(task)
                            
                            error_msg = f"Both primary '{self.config.wait_for_selector}' and fallback '{self.config.fallback_wait_for_selector}' selectors failed on {url}: {e}, {fallback_e}"
                            
                            if will_retry:
                                logger.error(f"{error_msg} - Will retry ({task['retries']}/{self.config.max_retries})")
                                raise Exception(error_msg)
                            else:
                                logger.warning(f"{error_msg} - No more retries, continuing with partial results")
                    else:
                        # No fallback selector, handle as before
                        self._increment_retry_count(task)
                        will_retry = self._should_retry_task(task)
                        
                        error_msg = f"Timeout waiting for selector '{self.config.wait_for_selector}' on {url}: {e}"
                        
                        if will_retry:
                            logger.error(f"{error_msg} - Will retry ({task['retries']}/{self.config.max_retries})")
                            raise Exception(error_msg)
                        else:
                            logger.warning(f"{error_msg} - No more retries, continuing with partial results")

            # Execute parsing script if available
            if self.parsing_script:
                result = await page.evaluate(self.parsing_script)
            else:
                # No parsing script - start with basic info
                result = {
                    "timestamp": await page.evaluate("new Date().toISOString()")
                }
            
            # Add page content if requested
            if self.config.use_page_content:
                page_content = await page.content()
                result["html"] = page_content
            
            result["url"] = url
            if task["retries"] > 0:
                result["retries"] = task["retries"]

            return result, False

        except Exception as e:
            self._increment_retry_count(task)
            needs_retry = self._should_retry_task(task)

            error_result = {
                "url": url,
                "error": str(e),
                "retries": task["retries"] - 1,
            }

            await self._add_random_delay()
            return error_result, needs_retry

        finally:
            if page:
                await page.close()
            if context:
                await context.close()

    async def _close_client(self, browser):
        """Close browser instance"""
        await browser.close()

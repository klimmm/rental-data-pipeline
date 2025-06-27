import asyncio
from typing import List, Dict, Optional
from playwright.async_api import async_playwright
from .performance_tracker import performance_tracker, ProgressTracker
import random
import logging

logger = logging.getLogger(__name__)


class AsyncScraper:
    def __init__(
        self,
        config,
        parsing_script: str = None,
        proxy_configs: List[Dict] = [],
        cookies: Optional[List[Dict]] = None,
    ):
        self.config = config
        self.working_proxies = proxy_configs[:self.config.max_concurrent + 2]
        
        if parsing_script:
            self.parsing_script = parsing_script
        else:
            with open(config.parsing_script_path, "r") as f:
                self.parsing_script = f.read()
                
        self.cookies = cookies
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.proxy_lock = asyncio.Lock()
        self.used_proxies = set()

    async def get_available_proxy(self):
        """Get an available proxy from the working proxies list"""
        async with self.proxy_lock:
            available = [
                p
                for p in self.working_proxies
                if p.get("server_name") not in self.used_proxies
            ]

            if not available:
                return None
            proxy = random.choice(available)
            self.used_proxies.add(proxy.get("server_name"))
            return proxy

    async def scrape_all(self, urls: List[str]) -> List[Dict]:
        if not urls:
            return []

        url_queue = asyncio.Queue()
        for url in urls:
            task = {"url": url, "retries": 0}
            await url_queue.put(task)

        worker_count = min(
            len(urls),
            max(2, min(self.config.max_concurrent, len(self.working_proxies))),
        )
        worker_tasks = [self.worker(i, url_queue) for i in range(worker_count)]

        self.progress_tracker = ProgressTracker(len(urls), track_memory=True)

        worker_results = await asyncio.gather(*worker_tasks)

        # Combine results
        all_results = []
        for worker_result in worker_results:
            all_results.extend(worker_result)

        return all_results

    async def worker(self, worker_id, queue):
        results = []
        pages_processed = 0
        async with async_playwright() as playwright:

            # Create browser
            browser = await self.create_browser(playwright, worker_id)
            try:
                while True:
                    if pages_processed >= self.config.max_pages_per_browser:
                        logger.info(
                            f" page_processed = {pages_processed}, recreating browser"
                        )
                        if browser._proxy_name != "none":
                            self.used_proxies.remove(browser._proxy_name)
                        await browser.close()
                        browser = await self.create_browser(playwright, worker_id)
                        pages_processed = 0

                    try:
                        # Get task (not just URL)
                        task = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    # Process the task
                    result, needs_retry = await self._scrape_url(browser, task)
                    pages_processed += 1
                    if not needs_retry:
                        results.append(result)
                    else:
                        # Return to queue for retry
                        await queue.put(task)

                    queue.task_done()

            finally:
                await browser.close()

        return results

    async def create_browser(self, playwright_instance, worker_id):
        """Create and configure a single browser instance with memory tracking"""
        # Get proxy for this worker
        proxy = await self.get_available_proxy()
        # Setup launch options
        launch_options = {"headless": True}
        if proxy:

            launch_options["proxy"] = {"server": proxy["server"]}

        # Launch browser
        try:
            browser = await playwright_instance.chromium.launch(**launch_options)

            # Store metadata
            browser._worker_id = worker_id
            browser._proxy_name = proxy["server_name"] if proxy else "none"
            logger.info(f"worker_id {worker_id} using proxy {browser._proxy_name}")
            return browser

        except Exception as e:
            logger.error(f"Failed to create browser for worker {worker_id}: {e}")
            return None

    @performance_tracker
    async def _scrape_url(self, browser, task):
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

            result = await page.evaluate(self.parsing_script)
            result["url"] = url
            if task["retries"] > 0:
                result["retries"] = task["retries"]

            return result, False

        except Exception as e:
            error_message = str(e)
            task["retries"] += 1

            needs_retry = task["retries"] <= self.config.max_retries

            error_result = {
                "url": url,
                "error": error_message,
                "retries": task["retries"] - 1,
            }

            return error_result, needs_retry

        finally:
            if page:
                await page.close()
            if context:
                await context.close()
import asyncio
import aiohttp
import random
import logging
from typing import List, Dict, Optional
from .performance_tracker import performance_tracker, ProgressTracker

logger = logging.getLogger(__name__)


class AsyncHttpProcessor:
    """HTTP processor focused solely on making HTTP requests"""

    def __init__(
        self,
        config,
        proxy_configs: List[Dict] = [],
        cookies: Optional[Dict] = None,
    ):
        self.config = config
        self.working_proxies = proxy_configs
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

    async def process_all(self, requests: List[Dict]) -> List[Dict]:
        """Process all HTTP requests in parallel

        Each request should be a dictionary with:
        {
            "url": "https://example.com",  # Required
            "method": "GET",               # Optional (default: GET)
            "params": {},                  # Optional URL parameters
            "data": {},                    # Optional POST data
            "headers": {},                 # Optional additional headers
            "request_id": "some-id"        # Optional request identifier
        }
        """
        if not requests:
            return []

        task_queue = asyncio.Queue()
        for req in requests:
            task = {"request": req, "retries": 0}
            await task_queue.put(task)

        worker_count = min(
            len(requests),
            max(2, min(self.config.max_concurrent, len(self.working_proxies) or 1)),
        )

        worker_tasks = [self.worker(i, task_queue) for i in range(worker_count)]

        self.progress_tracker = ProgressTracker(len(requests), track_memory=True)

        worker_results = await asyncio.gather(*worker_tasks)

        all_results = []
        for worker_result in worker_results:
            all_results.extend(worker_result)

        return all_results

    async def worker(self, worker_id, queue):
        """Worker that processes requests from the queue"""
        results = []
        requests_processed = 0

        # Create client session with proxy
        session = await self.create_session(worker_id)
        sleep_time = 0.5 + random.uniform(0, 0.5)
        await asyncio.sleep(sleep_time)
        try:
            while True:
                # Check if session needs recreation
                if requests_processed >= self.config.max_requests_per_session:
                    logger.info(
                        f"Requests processed = {requests_processed}, recreating session"
                    )
                    if session._proxy_name != "none":
                        self.used_proxies.remove(session._proxy_name)
                    await session.close()
                    session = await self.create_session(worker_id)
                    requests_processed = 0

                try:
                    # Get task from queue
                    task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                # Process the task
                result, needs_retry = await self._make_request(session, task)
                requests_processed += 1

                if not needs_retry:
                    results.append(result)
                else:
                    # Return to queue for retry
                    await queue.put(task)

                queue.task_done()

        finally:
            await session.close()

        return results

    async def create_session(self, worker_id):
        """Create and configure a client session with proxy"""
        # Get proxy for this worker
        proxy = await self.get_available_proxy()
        user_agent = None
        accept_language = None
        proxy_name = proxy["server_name"] if proxy else "none"
        user_agent = self.config.proxy_user_agents.get(proxy_name)
        accept_language = self.config.proxy_accept_languages[proxy_name]
        logger.info(f"Worker {worker_id} using user agent {user_agent}, language {accept_language}")
            
        # Setup session options
        session_options = {
            "timeout": aiohttp.ClientTimeout(total=self.config.timeout),
            "cookies": self.cookies,
            "headers": {
                "User-Agent": user_agent or random.choice(self.config.user_agents),
                "Accept-Language": accept_language or self.config.accept_language,
            },
        }

        if proxy:
            session_options["proxy"] = proxy["server"]

        try:
            session = aiohttp.ClientSession(**session_options)

            # Store metadata
            session._worker_id = worker_id
            session._proxy_name = proxy_name
            # logger.info(f"Worker {worker_id} using proxy {session._proxy_name}")

            return session

        except Exception as e:
            logger.error(f"Failed to create session for worker {worker_id}: {e}")
            return None

    @performance_tracker
    async def _make_request(self, session, task):
        """Make an HTTP request and return the response"""
        request = task["request"]
        url = request["url"]
        method = request.get("method", "GET").upper()
        params = request.get("params", None)
        data = request.get("data", None)
        headers = request.get("headers", None)
        request_id = request.get("request_id", url)

        try:
            # Make the request
            async with getattr(session, method.lower())(
                url, params=params, data=data, headers=headers
            ) as response:
                response.raise_for_status()

                # Get response content based on content type
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    response_data = await response.json()
                else:
                    response_data = await response.text()

                result = {
                    "request_id": request_id,
                    "url": url,
                    "status": response.status,
                    "data": response_data,
                    "headers": dict(response.headers),
                }

                if task["retries"] > 0:
                    result["retries"] = task["retries"]

                sleep_time = 0.2 + random.uniform(0, 0.2)
                await asyncio.sleep(sleep_time)
                
                return result, False  # Success

        except Exception as e:
            error_message = str(e)
            task["retries"] += 1

            # Log the specific error for debugging
            logger.error(
                f"Request failed for {url}: {type(e).__name__}: {error_message}"
            )

            needs_retry = task["retries"] <= self.config.max_retries

            error_result = {
                "request_id": request_id,
                "url": url,
                "error": error_message,
                "retries": task["retries"] - 1,
            }

            return error_result, needs_retry

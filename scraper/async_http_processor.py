"""
Example showing how AsyncScraper and AsyncHttpProcessor could inherit from BaseAsyncProcessor
"""

import asyncio
import aiohttp
from .base_async_processor import BaseAsyncProcessor
from .performance_tracker import performance_tracker
import logging

logger = logging.getLogger(__name__)


class AsyncHttpProcessor(BaseAsyncProcessor):
    """AsyncHttpProcessor refactored to inherit from BaseAsyncProcessor"""

    # Circuit breaker: when a remote endpoint (e.g. Nominatim) starts
    # rate-limiting us, stop hammering it. Retrying 429s 5× per request ×
    # hundreds of requests both wastes ~30 min and reinforces the IP ban.
    # After this many consecutive 429s, all subsequent tasks in this
    # processor instance fail immediately without hitting the network.
    _RATE_LIMIT_BREAKER_THRESHOLD = 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._consecutive_429s = 0
        self._circuit_open = False

    def _create_task(self, request: dict) -> dict:
        return {"request": request, "retries": 0}

    async def worker(self, worker_id: int, queue: asyncio.Queue) -> list:
        """Worker that processes requests from the queue"""
        return await self._worker_loop(worker_id, queue)

    async def _create_client(self, worker_id: int):
        """Create HTTP session for worker"""
        proxy = await self.get_available_proxy()
        proxy_name = proxy["server_name"] if proxy else None

        user_agent = self.config.proxy_user_agents.get(
            proxy_name, self.config.default_user_agent
        )
        accept_language = self.config.proxy_accept_languages.get(
            proxy_name, self.config.default_accept_language
        )
        logger.info(
            f"Worker {worker_id} using proxy {proxy_name}, "
            f"user agent {user_agent}, language {accept_language}"
        )

        session_options = {
            "timeout": aiohttp.ClientTimeout(total=self.config.timeout),
            "cookies": self.cookies,
            "headers": {
                "User-Agent": user_agent,
                "Accept-Language": accept_language,
            },
        }

        if proxy:
            session_options["proxy"] = proxy["server"]

        session = aiohttp.ClientSession(**session_options)
        session._worker_id = worker_id
        session._proxy_name = proxy_name

        return session

    @performance_tracker
    async def _process_task(self, session, task: dict):
        """Process a single HTTP request using the session"""
        request = task["request"]
        url = request["url"]
        method = request.get("method", "GET").upper()
        params = request.get("params", None)
        data = request.get("data", None)
        headers = request.get("headers", None)
        request_id = request.get("request_id", url)

        # Circuit breaker: if the endpoint has rate-limited us repeatedly,
        # short-circuit without hitting the network. Lets the phase finish
        # in seconds instead of grinding through retries against a banned IP.
        if self._circuit_open:
            return {
                "request_id": request_id,
                "url": url,
                "error": "circuit_open: prior 429s tripped rate-limit breaker",
                "status": 429,
            }, False

        try:
            async with getattr(session, method.lower())(
                url, params=params, data=data, headers=headers
            ) as response:
                # 429 → don't retry, count toward circuit-breaker threshold.
                # Retries on 429 just compound the abuse signal at the
                # server and extend the ban.
                if response.status == 429:
                    self._consecutive_429s += 1
                    if (
                        not self._circuit_open
                        and self._consecutive_429s >= self._RATE_LIMIT_BREAKER_THRESHOLD
                    ):
                        self._circuit_open = True
                        logger.warning(
                            f"Rate-limit circuit opened after "
                            f"{self._consecutive_429s} consecutive 429s — "
                            f"all subsequent requests will fail-fast."
                        )
                    await self._add_random_delay()
                    return {
                        "request_id": request_id,
                        "url": url,
                        "error": f"HTTP 429 Too Many Requests",
                        "status": 429,
                    }, False

                response.raise_for_status()
                # Successful response — reset consecutive 429 counter.
                self._consecutive_429s = 0

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

                await self._add_random_delay()
                return result, False

        except Exception as e:
            self._increment_retry_count(task)
            needs_retry = self._should_retry_task(task)

            error_result = {
                "request_id": request_id,
                "url": url,
                "error": str(e),
                "retries": task["retries"] - 1,
            }

            await self._add_random_delay()
            return error_result, needs_retry

    async def _close_client(self, session):
        """Close HTTP session"""
        await session.close()

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

        try:
            async with getattr(session, method.lower())(
                url, params=params, data=data, headers=headers
            ) as response:
                response.raise_for_status()

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

import asyncio
import random
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from .performance_tracker import ProgressTracker

logger = logging.getLogger(__name__)


class BaseAsyncProcessor(ABC):
    """Base class for async processors that handle queued tasks with proxy support"""

    def __init__(self, config):
        self.config = config
        self.working_proxies = config.proxy_configs[: self.config.max_concurrent + 2]
        self.cookies = self._load_cookies()
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.proxy_lock = asyncio.Lock()
        self.used_proxies = set()
        self.progress_tracker = None

    def _load_cookies(self):
        """Load cookies from config.cookies_path if provided"""
        if self.config.cookies_path and self.config.use_cookies:
            try:
                import json

                with open(self.config.cookies_path, "r") as f:
                    return json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.warning(
                    f"Failed to load cookies from {self.config.cookies_path}: {e}"
                )
        return None

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

    def _calculate_worker_count(self, task_count: int) -> int:
        """Calculate optimal number of workers based on tasks and resources"""
        return min(
            task_count,
            max(2, min(self.config.max_concurrent, len(self.working_proxies))),
        )

    async def process_all(self, items: List[Any]) -> List[Dict]:
        """Main entry point - process all items using workers"""
        if not items:
            return []

        # Create task queue
        task_queue = asyncio.Queue()
        for item in items:
            task = self._create_task(item)
            await task_queue.put(task)

        # Calculate worker count and start workers
        worker_count = self._calculate_worker_count(len(items))
        worker_tasks = [self.worker(i, task_queue) for i in range(worker_count)]

        # Track progress
        self.progress_tracker = ProgressTracker(len(items), track_memory=True)

        try:
            # Execute workers in parallel
            worker_results = await asyncio.gather(*worker_tasks)

            # Combine results
            all_results = []
            for worker_result in worker_results:
                all_results.extend(worker_result)

            return all_results
        finally:
            # Stop progress tracking and show summary
            if self.progress_tracker:
                await self.progress_tracker.stop()

    @abstractmethod
    def _create_task(self, item: Any) -> Dict:
        """Convert input item to task dictionary"""
        pass

    @abstractmethod
    async def worker(self, worker_id: int, queue: asyncio.Queue) -> List[Dict]:
        """Worker that processes tasks from the queue"""
        pass

    @abstractmethod
    async def _create_client(self, worker_id: int, *args, **kwargs):
        """Create client instance (browser/session) for worker"""
        pass

    async def _worker_loop(
        self, worker_id: int, queue: asyncio.Queue, *client_args, **client_kwargs
    ) -> List[Dict]:
        """Common worker loop logic that both processors can use"""
        results = []
        processed_count = 0

        client = await self._create_client(worker_id, *client_args, **client_kwargs)
        await self._add_random_delay()

        try:
            while True:
                if self._should_recreate_client(processed_count):
                    client = await self._handle_client_recreation(
                        client, worker_id, *client_args, **client_kwargs
                    )
                    processed_count = 0

                try:
                    task = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                result, needs_retry = await self._process_task(client, task)
                processed_count += 1

                if not needs_retry:
                    results.append(result)
                else:
                    await queue.put(task)

                queue.task_done()

        finally:
            await self._cleanup_client(client)

        return results

    @abstractmethod
    async def _process_task(self, client, task: Dict):
        """Process a single task using the client"""
        pass

    @abstractmethod
    async def _close_client(self, client):
        """Close/cleanup client instance"""
        pass

    def _should_recreate_client(self, processed_count: int) -> bool:
        """Check if client should be recreated based on usage"""
        max_per_client = getattr(self.config, "max_tasks_per_client", 20)
        return processed_count >= max_per_client

    async def _cleanup_client(self, client):
        """Release proxy and close client"""
        proxy_name = getattr(client, '_proxy_name', None)
        if proxy_name:
            self.used_proxies.discard(proxy_name)
        await self._close_client(client)

    async def _handle_client_recreation(
        self, client, worker_id: int, *client_args, **client_kwargs
    ):
        """Handle client recreation and proxy cleanup"""
        # Cleanup old client
        await self._cleanup_client(client)

        # Create new client
        return await self._create_client(worker_id, *client_args, **client_kwargs)

    def _should_retry_task(self, task: Dict) -> bool:
        """Check if task should be retried based on retry count"""
        return task.get("retries", 0) < self.config.max_retries

    def _increment_retry_count(self, task: Dict):
        """Increment the retry counter for a task"""
        task["retries"] = task.get("retries", 0) + 1

    async def _add_random_delay(self, min_delay: float = 0.2, max_delay: float = 0.5):
        """Add random delay to avoid overwhelming servers"""
        delay = min_delay + random.uniform(0, max_delay - min_delay)
        await asyncio.sleep(delay)
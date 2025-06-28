import asyncio
import json
from datetime import datetime
import time
import logging
import os

logger = logging.getLogger(__name__)


class ProgressTracker:
    def __init__(self, total_requests, track_memory=True):
        self.total_requests = total_requests
        self.unique_processed = set()
        self.processed_count = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.retried_requests = 0
        self.lock = asyncio.Lock()
        self.start_time = time.time()

        # Memory tracking
        self.track_memory = track_memory
        self.process_mem_current = 0
        self.process_mem_peak = 0
        self.system_mem_current = 0
        self.system_mem_percent = 0
        self.tracking_task = None

        if track_memory:
            try:
                import psutil

                self.process = psutil.Process(os.getpid())
                self.tracking_task = asyncio.create_task(self._memory_tracking_loop())
            except (ImportError, Exception) as e:
                logger.warning(f"Memory tracking disabled: {e}")
                self.track_memory = False

            # Print header
            logger.info("\n{:-^100}".format(" SCRAPING PROGRESS "))
            if self.track_memory:
                logger.info(
                    "{:<4}|{:<7}|{:<7}|{:<7}|{:<7}|{:<10}|{:<9}|{:<12}".format(
                        "%",
                        "Proc",
                        "Succ",
                        "Fail",
                        "Retry",
                        "items/sec",
                        "Proc GB",
                        "Sys GB (%)",
                    )
                )
            else:
                logger.info(
                    "{:<4}|{:<7}|{:<7}|{:<7}|{:<7}|{:<10}".format(
                        "%", "Proc", "Succ", "Fail", "Retry", "items/sec"
                    )
                )
            logger.info("-" * 100 if self.track_memory else "-" * 45)

    async def _memory_tracking_loop(self, interval=2):
        """Background task to monitor memory usage"""
        try:
            import psutil

            while True:
                # Calculate total memory including child processes
                total_memory = self.process.memory_info().rss
                try:
                    children = self.process.children(recursive=True)
                    for child in children:
                        try:
                            total_memory += child.memory_info().rss
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass  # Child process may have terminated
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

                self.process_mem_current = total_memory / (1024**3)  # GB
                if self.process_mem_current > self.process_mem_peak:
                    self.process_mem_peak = self.process_mem_current

                # System memory
                system_info = psutil.virtual_memory()
                self.system_mem_current = system_info.used / (1024**3)  # GB
                self.system_mem_percent = system_info.percent

                # Wait for next interval
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error in memory tracking: {e}")

    async def update(self, request, success=True, retry=False):
        async with self.lock:
            self.processed_count += 1
            # Use request_id or URL as hashable identifier for unique tracking
            request_id = request.get("request_id") or request.get("url", str(request))
            self.unique_processed.add(request_id)  # Add to set of unique requests

            if success:
                self.successful_requests += 1
            else:
                self.failed_requests += 1
            if retry:
                self.retried_requests += 1

            # Calculate progress based on unique requests
            percent = (len(self.unique_processed) / self.total_requests) * 100
            elapsed = time.time() - self.start_time
            speed = (self.processed_count / elapsed) if elapsed > 0 else 0

            # Print progress with memory info if enabled
            if self.track_memory:
                logger.info(
                    "{:4.1f}|{:<7}|{:<7}|{:<7}|{:<7}|{:<10.2f}|{:<9.2f}|{:<5.1f} ({:4.1f}%)".format(
                        min(percent, 100.0),  # Cap at 100%
                        self.processed_count,
                        self.successful_requests,
                        self.failed_requests,
                        self.retried_requests,
                        speed,
                        self.process_mem_current,
                        self.system_mem_current,
                        self.system_mem_percent,
                    )
                )
            else:
                logger.info(
                    "{:4.1f}|{:<7}|{:<7}|{:<7}|{:<7}|{:<10.2f}".format(
                        min(percent, 100.0),  # Cap at 100%
                        self.processed_count,
                        self.successful_requests,
                        self.failed_requests,
                        self.retried_requests,
                        speed,
                    )
                )

            # Log to file
            logging.getLogger("performance").debug(
                json.dumps(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "percent": min(percent, 100.0),
                        "processed": self.processed_count,
                        "successful": self.successful_requests,
                        "failed": self.failed_requests,
                        "retries": self.retried_requests,
                        "speed": speed,
                        "process_mem_mb": self.process_mem_current,
                        "process_mem_peak_mb": self.process_mem_peak,
                        "system_mem_gb": self.system_mem_current,
                        "system_mem_percent": self.system_mem_percent,
                    }
                )
            )

    def get_summary(self):
        """Get summary statistics as a dictionary"""
        elapsed = time.time() - self.start_time
        return {
            "total_requests": self.total_requests,
            "processed": self.processed_count,
            "successful": self.successful_requests,
            "failed": self.failed_requests,
            "retries": self.retried_requests,
            "elapsed_seconds": elapsed,
            "speed": (self.processed_count / elapsed) if elapsed > 0 else 0,
            "process_mem_peak_mb": self.process_mem_peak,
            "system_mem_gb": self.system_mem_current,
            "system_mem_percent": self.system_mem_percent,
        }

    async def stop(self):
        """Stop the memory tracking task"""
        if self.tracking_task:
            self.tracking_task.cancel()
            try:
                await self.tracking_task
            except asyncio.CancelledError:
                pass

        # Print final summary
        logger.info("\n{:-^120}".format(" SCRAPING SUMMARY "))
        logger.info(f"Total requests: {self.total_requests}")
        logger.info(
            f"Successful: {self.successful_requests} ({self.successful_requests/self.total_requests*100:.1f}%)"
        )
        logger.info(
            f"Failed: {self.failed_requests} ({self.failed_requests/self.total_requests*100:.1f}%)"
        )
        logger.info(f"Retries: {self.retried_requests}")

        elapsed = time.time() - self.start_time
        logger.info(f"Total time: {elapsed:.2f} seconds")
        logger.info(
            f"Average speed: {(self.processed_count / elapsed) * 60 if elapsed > 0 else 0:.2f} requests/minute"
        )

        if self.track_memory:
            logger.info(f"Peak process memory: {self.process_mem_peak:.2f} GB")
            logger.info(
                f"Final system memory: {self.system_mem_current:.2f} GB ({self.system_mem_percent:.1f}%)"
            )


def performance_tracker(func):
    """Enhanced decorator to track performance and handle retry logic"""

    async def wrapper(self, resource, task, *args, **kwargs):
        # Handle both async_scraper (url directly) and async_http_processor (request dict)
        if "request" in task:
            request = task["request"]
            url = request.get("url", "unknown")
        else:
            # For async_scraper.py compatibility
            url = task.get("url", "unknown")
            request = {"url": url}

        retry_count = task["retries"]

        start_time = time.time()
        success = False
        error_message = None
        needs_retry = False

        try:
            result, needs_retry = await func(self, resource, task, *args, **kwargs)

            # Key change: Only mark as success if we got a valid result without error
            # and no retry is needed
            success = not needs_retry and "error" not in result

            # Capture error message if present
            if "error" in result:
                error_message = result["error"]

            # Update progress tracker
            if hasattr(self, "progress_tracker"):
                await self.progress_tracker.update(
                    request=request, success=success, retry=needs_retry
                )

            return result, needs_retry

        except Exception as e:
            error_message = str(e)

            # Update progress tracker for exceptions too
            if hasattr(self, "progress_tracker"):
                await self.progress_tracker.update(
                    request=request, success=False, retry=False
                )

            raise

        finally:
            # Performance tracking
            duration = time.time() - start_time
            # Works for both browser and session objects
            worker_id = getattr(resource, "_worker_id", "unknown")

            # Handle different proxy attribute names
            proxy_info = getattr(
                resource, "_proxy_info", getattr(resource, "_proxy_name", "unknown")
            )

            logging.getLogger("performance").debug(
                json.dumps(
                    {
                        "request": request if "request" in task else {"url": url},
                        "attempt": retry_count + 1,
                        "worker_id": worker_id,
                        "proxy": proxy_info,
                        "duration": duration,
                        "success": success,  # Now correctly reflects true success
                        "error": error_message,
                        "retry_scheduled": needs_retry,
                        "timestamp": datetime.now().isoformat(),
                    }
                )
            )

    return wrapper

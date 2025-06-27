import subprocess
import schedule
import time
from datetime import datetime
import os
import asyncio
import logging
from parse_data import ScraperPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    # handlers=[logging.FileHandler("scheduler.log"), logging.StreamHandler()],
)
logger = logging.getLogger("scheduler")


class PipelineOrchestrator:
    """Orchestrates the complete data pipeline including scraping, git operations, and workflows."""

    def __init__(self, mode="update", search="wide"):
        self.logger = logging.getLogger("scheduler.orchestrator")
        self.data_dir = "/Users/klim/local-storage/app/data/cian_data"
        self.use_proxies = True
        self.git_repo_dir = "/Users/klim/local-storage/app"
        self.logger.info(f"mode={mode}, search={search}")

        # Set parameters based on mode
        if mode == "new":
            self.update_current_search_listings = True
            self.check_if_unpublished = False
            self.check_missing_estimations = False
        elif mode == "update":
            self.update_current_search_listings = False
            self.check_if_unpublished = True
            self.check_missing_estimations = False
        elif mode == "revise":
            self.update_current_search_listings = False
            self.check_if_unpublished = True
            self.check_missing_estimations = True

        else:
            raise ValueError(f"Unknown mode: {mode}")

        # Set search config path
        if search == "narrow":
            self.search_config_path = "search_configs/search_narrow.yaml"
        elif search == "wide":
            self.search_config_path = "search_configs/search_wide.yaml"
        else:
            raise ValueError(f"Unknown search: {search}")

        self.logger.info(f"Initialized with mode={mode}, search={search}")

    async def run_scraper_pipeline(self):
        """Run the scraper pipeline"""
        self.logger.info("Starting scraper pipeline...")

        try:
            pipeline = ScraperPipeline(
                data_dir=self.data_dir,
                use_proxies=self.use_proxies,
                search_config_path=self.search_config_path,
                check_missing_estimations=self.check_missing_estimations,
                check_if_unpublished=self.check_if_unpublished,
                update_current_search_listings=self.update_current_search_listings,
            )
            await pipeline.run()
            self.logger.info("✅ Scraper pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"❌ Scraper pipeline failed: {e}", exc_info=True)
            return False

    def _run_git_command(self, command, check=True):
        """Run a git command and return the result"""
        try:
            return subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=check,
                cwd=self.git_repo_dir,
            )
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Git command failed: {command[0]} {command[1]}")
            self.logger.error(f"Error: {e.stderr}")
            if check:
                raise
            return e

    def git_commit_and_push(self):
        """Commit and push data changes to git repository"""
        self.logger.info("Starting git commit and push...")

        # Check if data directory exists
        if not os.path.exists(self.data_dir):
            self.logger.warning(f"Data directory {self.data_dir} does not exist")
            return False

        # Add data files from git repo directory
        self._run_git_command(["git", "add", self.data_dir])

        # Check if there are changes to commit
        result = self._run_git_command(
            ["git", "diff", "--cached", "--quiet"], check=False
        )
        if result.returncode == 0:
            self.logger.info("ℹ️ No changes to commit")
            return True

        # Show changes
        result = self._run_git_command(["git", "diff", "--cached", "--name-status"])
        self.logger.info(f"Changes to be committed:\n{result.stdout}")

        # Commit
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"Auto-update data on {timestamp}"
        result = self._run_git_command(["git", "commit", "-m", commit_message])

        if result.returncode != 0:
            self.logger.error(f"❌ Git commit failed: {result.stderr}")
            return False

        # Check and handle remote changes before pushing
        return self._sync_and_push()

    def _sync_and_push(self, max_network_retries=3):
        """Sync with remote and push changes - matching GitHub Actions workflow strategy"""

        # Handle potential remote changes (matching workflow logic)
        self.logger.info("Fetching latest changes...")
        self._run_git_command(["git", "fetch", "origin", "main"])

        # Stash any unstaged changes before rebase
        stash_result = self._run_git_command(["git", "diff", "--quiet"], check=False)
        stashed = False
        if stash_result.returncode != 0:
            self.logger.info("Stashing unstaged changes...")
            self._run_git_command(
                ["git", "stash", "push", "-m", "WIP: unstaged changes before rebase"]
            )
            stashed = True

        # Check if we need to rebase
        ancestor_check = self._run_git_command(
            ["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"], check=False
        )
        if ancestor_check.returncode != 0:
            self.logger.info("Remote has changes, rebasing...")
            rebase_result = self._run_git_command(
                ["git", "rebase", "origin/main"], check=False
            )

            if rebase_result.returncode != 0:
                self.logger.info(
                    "Rebase conflicts detected, using merge strategy instead..."
                )
                self._run_git_command(["git", "rebase", "--abort"], check=False)
                self._run_git_command(
                    [
                        "git",
                        "merge",
                        "origin/main",
                        "-X",
                        "ours",
                        "-m",
                        "Merge remote changes (keeping local data updates)",
                    ]
                )

        # Restore stashed changes if any
        if stashed:
            self.logger.info("Restoring stashed changes...")
            pop_result = self._run_git_command(["git", "stash", "pop"], check=False)
            if pop_result.returncode != 0:
                self.logger.info("No changes to restore")

        # Now push with retries only for network issues
        for attempt in range(max_network_retries):
            try:
                self.logger.info(
                    f"Pushing changes to remote (attempt {attempt + 1}/{max_network_retries})..."
                )
                self._run_git_command(["git", "push"])
                self.logger.info("✅ Changes pushed successfully")
                return True
            except subprocess.CalledProcessError as e:
                # Only retry for network-related issues
                if (
                    "Connection timed out" in e.stderr
                    or "Could not resolve host" in e.stderr
                ):
                    if attempt < max_network_retries - 1:
                        wait_time = 5 * (attempt + 1)  # Progressive backoff
                        self.logger.warning(
                            f"Network issue, retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        self.logger.error(
                            f"Push failed after {max_network_retries} attempts due to network issues"
                        )
                else:
                    # Non-network error - likely a Git issue that retrying won't fix
                    self.logger.error(f"Git push failed with error: {e.stderr}")
                    return False

        return False

    def trigger_image_processing(self):
        """Trigger image processing workflow - only for wide/update mode"""
        # Only trigger for wide search and update mode
        if not (
            hasattr(self, "search_config_path")
            and "wide" in self.search_config_path
            and hasattr(self, "check_if_unpublished")
            and self.check_if_unpublished
            and hasattr(self, "update_current_search_listings")
            and not self.update_current_search_listings
        ):
            self.logger.info(
                "Skipping image processing (only runs for wide/update mode)"
            )
            return True

        self.logger.info("Triggering image processing workflow...")

        try:
            result = subprocess.run(
                [
                    "gh",
                    "workflow",
                    "run",
                    "process-images.yml",
                    "--repo",
                    "klimmm/image-utils",
                ],
                capture_output=True,
                text=True,
                cwd=self.git_repo_dir,
            )

            if result.returncode == 0:
                self.logger.info("✅ Image processing workflow triggered successfully")
                if result.stdout:
                    self.logger.info(f"Workflow output: {result.stdout}")
                return True
            else:
                self.logger.warning(f"⚠️ Could not trigger workflow: {result.stderr}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error triggering workflow: {e}")
            return False

    async def run_full_pipeline(self):
        """Run the complete pipeline sequence"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING FULL PIPELINE RUN")
        self.logger.info("=" * 60)

        # Step 1: Run scraper pipeline
        if not await self.run_scraper_pipeline():
            self.logger.error("❌ Pipeline aborted: Scraper failed")
            return False

        # Step 2: Commit and push changes
        git_success = self.git_commit_and_push()
        if not git_success:
            self.logger.warning("⚠️ Git operations failed, but continuing pipeline")

        # Step 3: Trigger image processing
        workflow_success = self.trigger_image_processing()

        # Report completion status
        pipeline_status = (
            "✅ Full pipeline completed successfully"
            if workflow_success
            else "⚠️ Pipeline completed with warnings"
        )
        self.logger.info(pipeline_status)
        self.logger.info("=" * 60)

        return True


def run_once(mode="update", search="wide"):
    """Run the pipeline once and exit"""
    orchestrator = PipelineOrchestrator(mode=mode, search=search)
    asyncio.run(orchestrator.run_full_pipeline())


def run_scheduler(mode="update", search="wide"):
    """Run the scheduler continuously"""
    orchestrator = PipelineOrchestrator(mode=mode, search=search)

    # Schedule pipeline to run every hour
    schedule.every(1).hours.do(lambda: asyncio.run(orchestrator.run_full_pipeline()))

    # Display schedule information
    logger.info("📅 Scheduler configured:")
    logger.info("  - Full pipeline: Every 1 hour")
    logger.info("  - Use Ctrl+C to stop")

    next_runs = []
    for job in schedule.jobs:
        next_run_str = job.next_run.strftime("%Y-%m-%d %H:%M:%S")
        next_runs.append(f"   - {next_run_str}")

    logger.info(f"⏭️ Next scheduled runs:\n" + "\n".join(next_runs))

    # Run immediately on startup
    logger.info("🚀 Running full pipeline immediately on startup...")
    asyncio.run(orchestrator.run_full_pipeline())

    # Main scheduler loop
    try:
        logger.info("⏳ Scheduler active and waiting for next run...")
        last_status_time = datetime.now()

        while True:
            schedule.run_pending()

            # Periodic status updates
            current_time = datetime.now()
            if (current_time - last_status_time).total_seconds() >= 600:  # 10 minutes
                logger.info(
                    f"💤 Scheduler still running... ({current_time.strftime('%H:%M:%S')})"
                )

                if schedule.jobs:
                    next_run = min(job.next_run for job in schedule.jobs)
                    time_until = next_run - current_time
                    hours, remainder = divmod(int(time_until.total_seconds()), 3600)
                    minutes, _ = divmod(remainder, 60)
                    logger.info(f"⏰ Next run in: {hours}h {minutes}m")

                last_status_time = current_time

            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("👋 Scheduler stopped by user")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cian Data Pipeline Scheduler")
    parser.add_argument(
        "--once", action="store_true", help="Run pipeline once and exit"
    )
    parser.add_argument(
        "--schedule", action="store_true", help="Run scheduler continuously"
    )
    parser.add_argument(
        "--mode",
        choices=["new", "update", "revise"],
        default="update",
        help="Scraper mode: new (only new listings), update (default), revise (full revision)",
    )
    parser.add_argument(
        "--search",
        choices=["narrow", "wide"],
        default="wide",
        help="Search config: narrow (limited), wide (default)",
    )

    args = parser.parse_args()

    if args.once:
        run_once(mode=args.mode, search=args.search)
    elif args.schedule:
        run_scheduler(mode=args.mode, search=args.search)
    else:
        print("Usage:")
        print(
            "  python scheduler.py --once [--mode MODE] [--search SEARCH]       # Run full pipeline once"
        )
        print(
            "  python scheduler.py --schedule [--mode MODE] [--search SEARCH]   # Run scheduler continuously"
        )
        print("")
        print("Options:")
        print("  --mode {new,update,revise}    Scraper mode (default: update)")
        print("  --search {narrow,wide}        Search config (default: wide)")

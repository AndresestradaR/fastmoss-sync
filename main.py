"""Entry point for FastMoss sync service."""
import logging
import sys
from datetime import datetime

from sync import run_sync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    start_time = datetime.now()
    logger.info(f"Starting FastMoss sync at {start_time.isoformat()}")

    try:
        total_synced, errors = run_sync()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"Sync completed in {duration:.1f} seconds")
        logger.info(f"Products synced: {total_synced}")

        if errors:
            logger.warning(f"Completed with {len(errors)} errors")
            sys.exit(1)
        else:
            logger.info("Completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

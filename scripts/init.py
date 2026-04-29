"""
Master initialization script that orchestrates all setup tasks.

This script should be run before starting the API server or workers.
It ensures all required database structures, users, and keys are in place.
"""

import asyncio
import argparse
import sys
import structlog
from typing import Callable, Awaitable

# Configure logging early
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

log = structlog.get_logger()


async def run_init_indexes() -> bool:
    """Initialize database indexes."""
    from scripts.init_indexes import init_indexes

    return await init_indexes()


async def run_init_keys() -> bool:
    """Initialize encryption keys."""
    from scripts.init_keys import init_encryption_keys

    return await init_encryption_keys()


async def run_init_users() -> bool:
    """Initialize default user."""
    from scripts.init_users import init_default_user

    return await init_default_user()


async def run_init_scrapers() -> bool:
    """Sync scrapers to database."""
    from scripts.init_scraper import init_scrapers

    return await init_scrapers()


async def run_all() -> bool:
    """Run all initialization steps in order."""
    log.info("=" * 60)
    log.info("INITIALIZATION STARTED")
    log.info("=" * 60)

    steps: list[tuple[str, Callable[[], Awaitable[bool]]]] = [
        ("Indexes", run_init_indexes),
        ("Encryption Keys", run_init_keys),
        ("Default User", run_init_users),
        ("Scrapers", run_init_scrapers),
    ]

    results = {}
    all_success = True

    for step_name, step_func in steps:
        log.info("-" * 40)
        log.info(f"Step: {step_name}")
        log.info("-" * 40)

        try:
            success = await step_func()
            results[step_name] = "SUCCESS" if success else "FAILED"
            if not success:
                all_success = False
        except Exception as e:
            log.exception("step.failed", step=step_name, error=str(e))
            results[step_name] = f"ERROR: {str(e)}"
            all_success = False

    log.info("=" * 60)
    log.info("INITIALIZATION SUMMARY")
    log.info("=" * 60)
    for step, status in results.items():
        log.info(f"  {step}: {status}")
    log.info("=" * 60)

    if all_success:
        log.info("All initialization steps completed successfully")
    else:
        log.error("Some initialization steps failed")

    return all_success


def main():
    parser = argparse.ArgumentParser(
        description="Initialize the scraping microservice",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m scripts.init --all          # Run all initialization
    python -m scripts.init --indexes      # Create indexes only
    python -m scripts.init --users        # Create default user only
    python -m scripts.init --keys         # Initialize encryption keys only
    python -m scripts.init --scrapers     # Sync scrapers to DB only
    python -m scripts.init --indexes --users  # Multiple steps
        """,
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all initialization steps",
    )
    parser.add_argument(
        "--indexes",
        action="store_true",
        help="Create database indexes",
    )
    parser.add_argument(
        "--keys",
        action="store_true",
        help="Initialize encryption keys",
    )
    parser.add_argument(
        "--users",
        action="store_true",
        help="Create default user and API key",
    )
    parser.add_argument(
        "--scrapers",
        action="store_true",
        help="Sync scrapers to database",
    )

    args = parser.parse_args()

    # If no args specified, show help
    if not any([args.all, args.indexes, args.keys, args.users, args.scrapers]):
        parser.print_help()
        sys.exit(1)

    async def run():
        success = True

        if args.all:
            success = await run_all()
        else:
            if args.indexes:
                success = await run_init_indexes() and success
            if args.keys:
                success = await run_init_keys() and success
            if args.users:
                success = await run_init_users() and success
            if args.scrapers:
                success = await run_init_scrapers() and success

        return success

    try:
        result = asyncio.run(run())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        log.info("Initialization cancelled by user")
        sys.exit(130)
    except Exception as e:
        log.exception("initialization.fatal_error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

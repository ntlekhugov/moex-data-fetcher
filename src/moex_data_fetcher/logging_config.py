"""Logging configuration for the package."""

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Configure basic logging for the library."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

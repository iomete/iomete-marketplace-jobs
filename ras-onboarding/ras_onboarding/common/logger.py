"""Logger configuration for compute onboarding migration job."""

import logging
import sys


def init_logger(debug_mode: bool = False):
    """Initialize logging configuration."""
    log_level = logging.DEBUG if debug_mode else logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set specific log levels for third-party libraries
    if debug_mode:
        # In debug mode, show more details from third-party libraries
        logging.getLogger("urllib3").setLevel(logging.INFO)
        logging.getLogger("requests").setLevel(logging.INFO)
        logging.getLogger("psycopg2").setLevel(logging.INFO)
    else:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("psycopg2").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get logger instance with specified name."""
    return logging.getLogger(name)
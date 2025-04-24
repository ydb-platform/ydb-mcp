import asyncio
import logging
import re
from typing import Optional, Tuple

import ydb

logger = logging.getLogger(__name__)


class YDBConnection:
    """Manages YDB connection with async support."""

    def __init__(self, connection_string: str):
        """Initialize YDB connection.

        Args:
            connection_string: YDB connection string
        """
        self.connection_string = connection_string
        self.driver: Optional[ydb.Driver] = None
        self.session_pool: Optional[ydb.SessionPool] = None

    async def connect(self) -> Tuple[ydb.Driver, ydb.SessionPool]:
        """Connect to YDB and setup session pool asynchronously.

        Returns:
            Tuple of (driver, session_pool)
        """
        logger.info(f"Connecting to YDB with connection string: {self.connection_string}")

        # Run driver initialization in a separate thread
        loop = asyncio.get_event_loop()

        # Create driver in thread to not block
        self.driver = await loop.run_in_executor(
            None,
            lambda: ydb.Driver(
                endpoint=self.connection_string, database=self._extract_database_path()
            ),
        )

        # Wait for driver to be ready
        ready = await loop.run_in_executor(None, self.driver.wait, 10)

        if not ready:
            raise RuntimeError(
                f"YDB driver failed to connect: {self.driver.discovery_debug_details()}"
            )

        logger.info("Connected to YDB successfully")

        # Create session pool
        self.session_pool = ydb.SessionPool(self.driver)

        return self.driver, self.session_pool

    async def close(self) -> None:
        """Close YDB connection."""
        logger.info("Closing YDB connection")

        if self.session_pool:
            await asyncio.get_event_loop().run_in_executor(None, self.session_pool.stop)
            self.session_pool = None

        if self.driver:
            await asyncio.get_event_loop().run_in_executor(None, self.driver.stop)
            self.driver = None

        logger.info("YDB connection closed")

    def _extract_database_path(self, connection_string: Optional[str] = None) -> str:
        """Extract database path from connection string.

        Args:
            connection_string: YDB connection string, or None to use the instance's connection string

        Returns:
            Database path
        """
        # Use instance connection string if none provided
        if connection_string is None:
            connection_string = self.connection_string

        # Handle connection string with query parameters
        if "?" in connection_string:
            connection_string = connection_string.split("?")[0]

        # Extract path using regex
        match = re.match(r"^(?:[^:]+://[^/]+)?(/.*)?$", connection_string)
        return match.group(1) if match and match.group(1) else "/"

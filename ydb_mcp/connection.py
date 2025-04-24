import asyncio
import logging
import re
from typing import Optional, Tuple
from urllib.parse import urlparse

import ydb
from ydb.aio import QuerySessionPool

logger = logging.getLogger(__name__)


class YDBConnection:
    """Manages YDB connection with async support."""

    def __init__(self, connection_string: str, database: str = None):
        """Initialize YDB connection.

        Args:
            connection_string: YDB connection string
            database: Optional database path. If not provided, will be extracted from connection_string if present
        """
        self.connection_string = connection_string
        self.driver: Optional[ydb.Driver] = None
        self.session_pool: Optional[ydb.aio.QuerySessionPool] = None
        self._database = database
        self.last_error = None

    def _parse_endpoint_and_database(self) -> Tuple[str, str]:
        """Parse endpoint and database from connection string.

        Returns:
            Tuple of (endpoint, database)

        Raises:
            RuntimeError: If no database is specified either in connection string or explicitly
        """
        # Parse the URL
        connection_string = self.connection_string
        if not connection_string.startswith(("grpc://", "grpcs://")):
            # If no scheme, assume grpc:// and parse as host:port
            if "/" in connection_string:
                host_port, path = connection_string.split("/", 1)
                connection_string = f"grpc://{host_port}/{path}"
            else:
                connection_string = f"grpc://{connection_string}"

        parsed = urlparse(connection_string)

        # Extract endpoint (scheme + netloc)
        endpoint = f"{parsed.scheme}://{parsed.netloc}"

        # Extract database path
        database = self._database
        if not database:
            if parsed.path:
                database = parsed.path
                # Remove query parameters if present
                if "?" in database:
                    database = database.split("?")[0]

        # Ensure database starts with /
        if database and not database.startswith("/"):
            database = f"/{database}"

        # Raise error if no database specified
        if not database:
            raise RuntimeError("Database not specified in connection string or explicitly")

        return endpoint, database

    async def connect(self) -> Tuple[ydb.Driver, ydb.aio.QuerySessionPool]:
        """Connect to YDB and setup session pool asynchronously.

        Returns:
            Tuple of (driver, session_pool)

        Raises:
            RuntimeError: If connection fails
        """
        try:
            endpoint, database = self._parse_endpoint_and_database()
            logger.info(f"Connecting to YDB endpoint: {endpoint}, database: {database}")

            # Create driver with direct parameters instead of config
            self.driver = ydb.aio.Driver(
                endpoint=endpoint,
                database=database,
                credentials=ydb.credentials.AnonymousCredentials(),
            )

            # Wait for driver to be ready with timeout
            try:
                await asyncio.wait_for(self.driver.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                self.last_error = "Connection timeout"
                raise RuntimeError(f"YDB driver connection timeout after 10 seconds")

            # Check if we connected successfully
            if not self.driver.discovery_debug_details().startswith("Resolved endpoints"):
                debug_details = self.driver.discovery_debug_details()
                self.last_error = f"Driver not ready: {debug_details}"
                raise RuntimeError(f"YDB driver failed to connect: {debug_details}")

            logger.info("Connected to YDB successfully")

            # Create session pool
            self.session_pool = ydb.aio.QuerySessionPool(self.driver)

            return self.driver, self.session_pool

        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Failed to connect to YDB: {e}")
            raise RuntimeError(f"Failed to connect to YDB: {e}")

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

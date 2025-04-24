"""Model Context Protocol server for YDB DBMS proxy."""

import asyncio
import base64
import datetime
import decimal
import gc
import json
import logging
import os
import sys
import types
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import ydb
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent
from ydb.aio import Driver as AsyncDriver
from ydb.aio import QuerySessionPool

from ydb_mcp.tool_manager import ToolManager

logger = logging.getLogger(__name__)

# Authentication mode constants
AUTH_MODE_ANONYMOUS = "anonymous"
AUTH_MODE_LOGIN_PASSWORD = "login-password"


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles non-serializable types properly."""

    def default(self, obj):
        # Handle datetime objects
        if isinstance(obj, datetime.datetime):
            # Convert to UTC if timezone-aware
            if obj.tzinfo is not None:
                obj = obj.astimezone(datetime.UTC)
            return obj.isoformat()

        # Handle date objects
        if isinstance(obj, datetime.date):
            return obj.isoformat()

        # Handle time objects
        if isinstance(obj, datetime.time):
            return obj.isoformat()

        # Handle timedelta objects
        if isinstance(obj, datetime.timedelta):
            # Convert to total seconds and format as string
            return f"{obj.total_seconds()}s"

        # Handle decimal objects
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        # Handle bytes objects - try UTF-8 first, fall back to base64
        if isinstance(obj, bytes):
            try:
                return obj.decode("utf-8")
            except UnicodeDecodeError:
                # If it's not valid UTF-8, base64 encode it
                return base64.b64encode(obj).decode("ascii")

        # Use the parent class's default method for other types
        return super().default(obj)


class YDBMCPServer(FastMCP):
    """Model Context Protocol server for YDB DBMS.

    Features:
    - Execute SQL queries against YDB database
    - Support for multiple SQL statements in a single query
    - Support for anonymous and login-password authentication modes
    """

    # YDB entry type mapping
    ENTRY_TYPE_MAP = {
        1: "DIRECTORY",
        2: "TABLE",
        3: "PERS_QUEUE",
        4: "DATABASE",
        5: "RTMR_VOLUME",
        6: "BLOCK_STORE_VOLUME",
        7: "COORDINATION",
        8: "SEQUENCE",
        9: "REPLICATION",
        10: "TOPIC",
        11: "EXTERNAL_DATA_SOURCE",
        12: "EXTERNAL_TABLE",
    }

    def __init__(
        self,
        endpoint: str = None,
        database: str = None,
        credentials_factory: Optional[Callable[[], ydb.Credentials]] = None,
        ydb_connection_string: str = "",
        tool_manager: Optional[ToolManager] = None,
        auth_mode: str = None,
        login: str = None,
        password: str = None,
        root_certificates: str = None,
        *args,
        **kwargs,
    ):
        """Initialize YDB MCP server.

        Args:
            endpoint: YDB endpoint
            database: YDB database
            credentials_factory: YDB credentials factory
            ydb_connection_string: YDB connection string (alternative to endpoint+database)
            tool_manager: External tool manager (optional)
            auth_mode: Authentication mode (anonymous, login_password)
            login: Login for authentication
            password: Password for authentication
            root_certificates: Root certificates for YDB
        """
        super().__init__(*args, **kwargs)

        # Initialize YDB-specific attributes
        self.driver = None
        self.endpoint = endpoint or os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136")
        self.database = database or os.environ.get("YDB_DATABASE", "/local")
        self.credentials_factory = credentials_factory
        self.ydb_connection_string = ydb_connection_string
        self.auth_error = None
        self._loop = None
        self.pool = None
        self.tool_manager = tool_manager or ToolManager()
        self._driver_lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()
        self.root_certificates = root_certificates
        self._original_methods = {}

        # Authentication settings
        supported_auth_modes = {AUTH_MODE_ANONYMOUS, AUTH_MODE_LOGIN_PASSWORD}
        self.auth_mode = (auth_mode or AUTH_MODE_ANONYMOUS)
        if self.auth_mode not in supported_auth_modes:
            raise ValueError(f"Unsupported auth mode: {self.auth_mode}. Supported modes: {', '.join(supported_auth_modes)}")
        self.login = login
        self.password = password

        # Initialize logging
        logging.basicConfig(level=logging.INFO)

        # Register YDB tools
        self.register_tools()

    def _restore_ydb_patches(self):
        """Restore original YDB methods that were patched."""
        # Restore topic client __del__ method
        if (
            "topic_client_del" in self._original_methods
            and hasattr(ydb, "topic")
            and hasattr(ydb.topic, "TopicClient")
        ):
            if self._original_methods["topic_client_del"] is not None:
                ydb.topic.TopicClient.__del__ = self._original_methods["topic_client_del"]
            else:
                # If there was no original method, try to remove our patched one
                if hasattr(ydb.topic.TopicClient, "__del__"):
                    delattr(ydb.topic.TopicClient, "__del__")
            logger.info("Restored original YDB TopicClient __del__ method")

    def _anonymous_credentials(self) -> ydb.Credentials:
        """Create anonymous credentials."""
        logger.info("Using anonymous authentication")
        return ydb.credentials.AnonymousCredentials()

    def _login_password_credentials(self) -> ydb.Credentials:
        """Create login-password credentials."""
        logger.info(f"Using login-password authentication with login: {self.login}")
        return ydb.credentials.StaticCredentials.from_user_password(self.login, self.password)

    async def create_driver(self):
        """Create a YDB driver with the current settings.

        Returns:
            ydb.aio.Driver or None: The created driver instance if successful, None if failed
        """
        try:
            # Get credentials
            credentials_factory = self.get_credentials_factory()
            if not credentials_factory:
                return None

            # Ensure we use the current event loop
            self._loop = asyncio.get_event_loop()

            # Determine endpoint and database
            endpoint = self.endpoint
            database = self.database

            # If we have a connection string, parse it
            if self.ydb_connection_string:
                conn = YDBConnection(self.ydb_connection_string)
                endpoint, database = conn._parse_endpoint_and_database()

            # Validate we have required parameters
            if not endpoint:
                self.auth_error = "YDB endpoint not specified"
                logger.error(self.auth_error)
                return None

            if not database:
                self.auth_error = "YDB database not specified"
                logger.error(self.auth_error)
                return None

            logger.info(f"Connecting to YDB at {endpoint}, database: {database}")

            # Create the driver config
            driver_config = ydb.DriverConfig(
                endpoint=endpoint,
                database=database,
                credentials=credentials_factory(),
                root_certificates=self.root_certificates,
            )

            # Create and initialize the driver
            self.driver = ydb.aio.Driver(driver_config)

            # Initialize driver with latest API
            await self.driver.wait(timeout=5.0)
            # Check if we connected successfully
            debug_details = await self._loop.run_in_executor(
                None, lambda: self.driver.discovery_debug_details()
            )
            if not debug_details.startswith("Resolved endpoints"):
                self.auth_error = f"Failed to connect to YDB server: {debug_details}"
                logger.error(self.auth_error)
                return None

            logger.info(f"Successfully connected to YDB at {endpoint}")
            return self.driver

        except Exception as e:
            self.auth_error = str(e)
            logger.error(f"Error creating YDB driver: {e}")
            return None

    async def _close_topic_client(self, topic_client):
        """Properly close a topic client."""
        if topic_client is not None and hasattr(topic_client, "close"):
            try:
                logger.info("Closing YDB topic client")
                # Ensure we wait for the close operation to complete
                await topic_client.close()
                return True
            except Exception as e:
                logger.warning(f"Error closing topic client: {e}")
        return False

    async def _terminate_discovery(self, discovery):
        """Properly terminate a discovery process and wait for tasks to complete."""
        if discovery is not None:
            try:
                # First check for the discovery task
                if hasattr(discovery, "_discovery_task") and discovery._discovery_task is not None:
                    task = discovery._discovery_task
                    if not task.done() and not task.cancelled():
                        logger.info("Cancelling discovery task")
                        task.cancel()
                        try:
                            # Wait for task cancellation to complete
                            await asyncio.wait_for(asyncio.shield(task), timeout=0.5)
                        except (asyncio.CancelledError, asyncio.TimeoutError, Exception) as e:
                            logger.warning(f"Error waiting for discovery task cancellation: {e}")

                # Handle any streaming response generators that might be running
                if hasattr(discovery, "_fetch_stream_responses") and callable(
                    discovery._fetch_stream_responses
                ):
                    # This is a generator method that might be active
                    # Nothing to do directly - the generator will be GC'ed when the driver is destroyed
                    pass

                # Then call terminate if available, but be careful of recursion
                if hasattr(discovery, "terminate"):
                    logger.info("Terminating YDB discovery process")
                    # Don't call our own terminate method to avoid recursion
                    original_terminate = discovery.terminate
                    if original_terminate.__name__ != "_terminate_discovery":
                        await original_terminate()
                    return True
            except Exception as e:
                logger.warning(f"Error terminating discovery: {e}")
        return False

    async def _cancel_ydb_related_tasks(self):
        """Find and cancel YDB-related tasks to prevent conflicts during shutdown."""
        discovery_tasks = []

        # Find YDB discovery-related tasks
        for task in asyncio.all_tasks(self._loop):
            task_str = str(task)
            if "Discovery.run" in task_str and not task.done() and not task.cancelled():
                discovery_tasks.append(task)

        if discovery_tasks:
            logger.info(f"Cancelling {len(discovery_tasks)} discovery tasks before restart")

            # Cancel all discovery tasks
            for task in discovery_tasks:
                task.cancel()

            # Wait briefly for tasks to cancel
            if discovery_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*discovery_tasks, return_exceptions=True), timeout=0.5
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

            # Wait a moment to allow task cancellation to complete
            await asyncio.sleep(0.1)

    async def get_pool(self) -> QuerySessionPool:
        """Get or create YDB session pool."""
        # Check for authentication errors first
        if self.auth_error:
            # Raise an exception with the auth error message which query() will catch
            raise ValueError(self.auth_error)

        async with self._pool_lock:
            if self.driver is None:
                await self.create_driver()

            if self.pool is None:
                self.pool = QuerySessionPool(self.driver)

            return self.pool

    def _stringify_dict_keys(self, obj):
        """Recursively convert all dict keys to strings for JSON serialization."""
        if isinstance(obj, dict):
            return {str(k): self._stringify_dict_keys(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._stringify_dict_keys(i) for i in obj]
        else:
            return obj

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[TextContent]:
        """Run a SQL query against YDB.

        Args:
            sql: SQL query to execute
            params: Optional query parameters

        Returns:
            List of TextContent objects with JSON-formatted results
        """
        # Check if there's an authentication error
        if self.auth_error:
            return [TextContent(type="text", text=json.dumps({"error": self.auth_error}, indent=2))]

        try:
            pool = await self.get_pool()
            ydb_params = None
            if params:
                ydb_params = {}
                for key, value in params.items():
                    param_key = key if key.startswith("$") else f"${key}"
                    ydb_params[param_key] = value
            result_sets = await pool.execute_with_retries(sql, ydb_params)
            all_results = []
            for result_set in result_sets:
                processed = self._process_result_set(result_set)
                all_results.append(processed)
            # Convert all dict keys to strings for JSON serialization
            safe_result = self._stringify_dict_keys({"result_sets": all_results})
            return [
                TextContent(
                    type="text", text=json.dumps(safe_result, indent=2, cls=CustomJSONEncoder)
                )
            ]
        except Exception as e:
            error_message = str(e)
            safe_error = self._stringify_dict_keys({"error": error_message})
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

    def _process_result_set(self, result_set):
        """Process YDB result set into a dictionary format.

        Args:
            result_set: YDB result set object

        Returns:
            Processed result set as a dictionary
        """
        try:
            # Extract columns
            columns = []
            try:
                # Get column names from the columns attribute
                columns_attr = getattr(result_set, "columns")
                columns = [col.name for col in columns_attr]
            except Exception as e:
                logger.exception(f"Error getting columns: {e}")
                return {"error": str(e), "columns": [], "rows": []}

            # Extract rows
            rows = []
            try:
                rows_attr = getattr(result_set, "rows")
                for row in rows_attr:
                    row_values = []
                    for i in range(len(columns)):
                        row_values.append(row[i])
                    rows.append(row_values)
            except Exception as e:
                logger.exception(f"Error getting rows: {e}")
                return {"error": str(e), "columns": columns, "rows": []}

            return {"columns": columns, "rows": rows}
        except Exception as e:
            logger.exception(f"Error processing result set: {e}")
            return {"error": str(e), "columns": [], "rows": []}

    async def query_with_params(self, sql: str, params: str) -> List[TextContent]:
        """Run a parameterized SQL query with JSON parameters.

        Args:
            sql: SQL query to execute
            params: Parameters as a JSON string

        Returns:
            Query results as a list of TextContent objects or a dictionary
        """
        # Handle authentication errors
        if self.auth_error:
            logger.error(f"Authentication error: {self.auth_error}")
            safe_error = self._stringify_dict_keys(
                {"error": f"Authentication error: {self.auth_error}"}
            )
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]
        parsed_params = {}
        try:
            if params and params.strip():
                parsed_params = json.loads(params)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON parameters: {str(e)}")
            safe_error = self._stringify_dict_keys(
                {"error": f"Error parsing JSON parameters: {str(e)}"}
            )
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]
        # Convert [value, type] to YDB type if needed
        ydb_params = {}
        for key, value in parsed_params.items():
            param_key = key if key.startswith("$") else f"${key}"
            if isinstance(value, (list, tuple)) and len(value) == 2:
                param_value, type_name = value
                if isinstance(type_name, str) and hasattr(ydb.PrimitiveType, type_name):
                    ydb_type = getattr(ydb.PrimitiveType, type_name)
                    ydb_params[param_key] = (param_value, ydb_type)
                else:
                    ydb_params[param_key] = param_value
            else:
                ydb_params[param_key] = value
        try:
            return await self.query(sql, ydb_params)
        except Exception as e:
            error_message = f"Error executing parameterized query: {str(e)}"
            logger.error(error_message)
            safe_error = self._stringify_dict_keys({"error": error_message})
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

    def register_tools(self):
        """Register YDB query tools.

        Note: Tools are registered with both the FastMCP framework and our tool_manager.
        The FastMCP.add_tool method doesn't support parameters, so we only provide
        the handler, name, and description to it. The complete tool specification
        including parameters is registered with our tool_manager.
        """
        # Define tool specifications
        tool_specs = [
            {
                "name": "ydb_query",
                "description": "Run a SQL query against YDB database",
                "handler": self.query,  # Use real handler
                "parameters": {
                    "properties": {"sql": {"type": "string", "title": "Sql"}},
                    "required": ["sql"],
                    "type": "object",
                },
            },
            {
                "name": "ydb_query_with_params",
                "description": "Run a parameterized SQL query with JSON parameters",
                "handler": self.query_with_params,  # Use real handler
                "parameters": {
                    "properties": {
                        "sql": {"type": "string", "title": "Sql"},
                        "params": {"type": "string", "title": "Params"},
                    },
                    "required": ["sql", "params"],
                    "type": "object",
                },
            },
            {
                "name": "ydb_status",
                "description": "Get the current status of the YDB connection",
                "handler": self.get_connection_status,  # Use real handler
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
            {
                "name": "ydb_list_directory",
                "description": "List directory contents in YDB",
                "handler": self.list_directory,
                "parameters": {
                    "properties": {"path": {"type": "string", "title": "Path"}},
                    "required": ["path"],
                    "type": "object",
                },
            },
            {
                "name": "ydb_describe_path",
                "description": "Get detailed information about a YDB path (table, directory, etc.)",
                "handler": self.describe_path,
                "parameters": {
                    "properties": {"path": {"type": "string", "title": "Path"}},
                    "required": ["path"],
                    "type": "object",
                },
            },
        ]

        # Register all tools with FastMCP framework
        for spec in tool_specs:
            self.add_tool(spec["handler"], name=spec["name"], description=spec["description"])

            # Also register with our tool manager
            self.tool_manager.register_tool(
                name=spec["name"],
                handler=spec["handler"],
                description=spec["description"],
                parameters=spec.get("parameters"),
            )

    async def get_connection_status(self) -> List[TextContent]:
        """Get the current status of the YDB connection.

        Returns:
            List of TextContent objects
        """
        connection_status = "disconnected"
        error_message = None

        try:
            # Force create driver to ensure up-to-date status
            if self.driver is None:
                logger.info("Creating new driver for connection status check")
                await self.create_driver()

            if self.driver:
                try:
                    discovery = self.driver.discovery_debug_details()
                    if discovery.startswith("Resolved endpoints"):
                        connection_status = "connected"
                    else:
                        error_message = f"Discovery error: {discovery}"
                except Exception as conn_error:
                    error_message = f"Error checking connection via discovery: {conn_error}"
            else:
                error_message = "No driver available for connection status check"
        except Exception as e:
            error_message = str(e)

        status_info = {
            "status": "running",
            "ydb_endpoint": self.endpoint,
            "ydb_database": self.database,
            "auth_mode": self.auth_mode,
            "ydb_connection": connection_status,
            "error": error_message,
        }

        # Format the result as a TextContent object
        safe_status = self._stringify_dict_keys(status_info)
        formatted_result = json.dumps(safe_status, indent=2, cls=CustomJSONEncoder)
        logger.info(f"Connection status: {formatted_result}")
        return [TextContent(type="text", text=formatted_result)]

    async def list_directory(self, path: str) -> List[TextContent]:
        """List the contents of a YDB directory.

        Args:
            path: Path to the directory to list

        Returns:
            List of TextContent objects with JSON-formatted directory contents
        """
        # Check for authentication errors
        if self.auth_error:
            return [TextContent(type="text", text=json.dumps({"error": self.auth_error}, indent=2))]

        try:
            # Create driver if needed
            if self.driver is None:
                await self.create_driver()

            if self.driver is None:
                return [
                    TextContent(
                        type="text", text=json.dumps({"error": "Failed to create driver"}, indent=2)
                    )
                ]

            # Access the scheme client
            scheme_client = self.driver.scheme_client

            # List the directory
            logger.info(f"Listing directory contents for path: {path}")
            dir_response = await scheme_client.list_directory(path)

            # Process the response
            result = {"path": path, "items": []}

            if dir_response.children:
                for entry in dir_response.children:
                    item = {
                        "name": entry.name,
                        "type": self.ENTRY_TYPE_MAP.get(entry.type, str(entry.type)),
                        "owner": entry.owner,
                    }

                    # Add permissions if available
                    if hasattr(entry, "permissions") and entry.permissions:
                        item["permissions"] = []
                        for perm in entry.permissions:
                            item["permissions"].append(
                                {
                                    "subject": perm.subject,
                                    "permission_names": list(perm.permission_names),
                                }
                            )

                    result["items"].append(item)

                # Sort items by name for consistency
                result["items"].sort(key=lambda x: x["name"])

            # Convert all dict keys to strings for JSON serialization
            safe_result = self._stringify_dict_keys(result)
            return [
                TextContent(
                    type="text", text=json.dumps(safe_result, indent=2, cls=CustomJSONEncoder)
                )
            ]

        except Exception as e:
            logger.exception(f"Error listing directory {path}: {e}")
            safe_error = self._stringify_dict_keys(
                {"error": f"Error listing directory {path}: {str(e)}"}
            )
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

    async def describe_path(self, path: str) -> List[TextContent]:
        """Describe a path in YDB.

        Args:
            path: Path to describe

        Returns:
            List of TextContent objects with path description
        """
        # Check for authentication errors
        if self.auth_error:
            safe_error = {"error": self.auth_error}
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

        try:
            # Create driver if needed
            if self.driver is None:
                await self.create_driver()

            if self.driver is None:
                safe_error = {"error": "Failed to create driver"}
                return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

            # Access the scheme client
            scheme_client = self.driver.scheme_client

            # Describe the path
            logger.info(f"Describing path: {path}")
            path_response = await scheme_client.describe_path(path)

            # Process the response
            if path_response is None:
                safe_error = {"error": f"Path '{path}' not found"}
                return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

            # Format the result
            result = {
                "path": path,
                "type": str(path_response.type),
                "name": path_response.name,
                "owner": path_response.owner,
            }

            # Add permissions if available
            if hasattr(path_response, "permissions") and path_response.permissions:
                result["permissions"] = []
                for perm in path_response.permissions:
                    result["permissions"].append(
                        {"subject": perm.subject, "permission_names": list(perm.permission_names)}
                    )

            # Add table specific information if it's a table
            if str(path_response.type) == "TABLE" or path_response.type == 2:
                try:
                    # Get table client for more detailed table info
                    table_client = self.driver.table_client
                    session = await table_client.session().create()
                    try:
                        # Get detailed table description
                        table_desc = await session.describe_table(path)
                        result["table"] = {
                            "columns": [],
                            "primary_key": table_desc.primary_key,
                            "indexes": [],
                            "partitioning_settings": {},
                            "storage_settings": {},
                            "key_bloom_filter": table_desc.key_bloom_filter,
                            "read_replicas_settings": table_desc.read_replicas_settings,
                            "column_families": [],
                        }

                        # Add columns with more details
                        for column in table_desc.columns:
                            col_info = {
                                "name": column.name,
                                "type": str(column.type),
                                "family": column.family,
                            }
                            result["table"]["columns"].append(col_info)

                        # Add indexes with more details
                        for index in table_desc.indexes:
                            index_info = {
                                "name": index.name,
                                "index_columns": list(index.index_columns),
                                "cover_columns": (
                                    list(index.cover_columns)
                                    if hasattr(index, "cover_columns")
                                    else []
                                ),
                                "index_type": (
                                    str(index.index_type) if hasattr(index, "index_type") else None
                                ),
                            }
                            result["table"]["indexes"].append(index_info)

                        # Add column families if present
                        if hasattr(table_desc, "column_families"):
                            for family in table_desc.column_families:
                                family_info = {
                                    "name": family.name,
                                    "data": family.data,
                                    "compression": (
                                        str(family.compression)
                                        if hasattr(family, "compression")
                                        else None
                                    ),
                                }
                                result["table"]["column_families"].append(family_info)

                        # Add storage settings if present
                        if hasattr(table_desc, "storage_settings"):
                            ss = table_desc.storage_settings
                            if ss:
                                result["table"]["storage_settings"] = {
                                    "tablet_commit_log0": ss.tablet_commit_log0,
                                    "tablet_commit_log1": ss.tablet_commit_log1,
                                    "external": ss.external,
                                    "store_external": ss.store_external,
                                }

                        # Add partitioning settings if present
                        if hasattr(table_desc, "partitioning_settings"):
                            ps = table_desc.partitioning_settings
                            if ps:
                                if hasattr(ps, "partition_at_keys"):
                                    result["table"]["partitioning_settings"][
                                        "partition_at_keys"
                                    ] = ps.partition_at_keys
                                if hasattr(ps, "partition_by_size"):
                                    result["table"]["partitioning_settings"][
                                        "partition_by_size"
                                    ] = ps.partition_by_size
                                if hasattr(ps, "min_partitions_count"):
                                    result["table"]["partitioning_settings"][
                                        "min_partitions_count"
                                    ] = ps.min_partitions_count
                                if hasattr(ps, "max_partitions_count"):
                                    result["table"]["partitioning_settings"][
                                        "max_partitions_count"
                                    ] = ps.max_partitions_count

                    finally:
                        # Always release the session
                        await session.close()

                except Exception as table_error:
                    logger.warning(f"Error getting detailed table info: {table_error}")
                    # Fallback to basic table info from path_response
                    if hasattr(path_response, "table") and path_response.table:
                        result["table"] = {
                            "columns": [],
                            "primary_key": (
                                path_response.table.primary_key
                                if hasattr(path_response.table, "primary_key")
                                else []
                            ),
                            "indexes": [],
                            "partitioning_settings": {},
                        }

                        # Add basic columns
                        if hasattr(path_response.table, "columns"):
                            for column in path_response.table.columns:
                                result["table"]["columns"].append(
                                    {"name": column.name, "type": str(column.type)}
                                )

                        # Add basic indexes
                        if hasattr(path_response.table, "indexes"):
                            for index in path_response.table.indexes:
                                result["table"]["indexes"].append(
                                    {
                                        "name": index.name,
                                        "index_columns": (
                                            list(index.index_columns)
                                            if hasattr(index, "index_columns")
                                            else []
                                        ),
                                    }
                                )

                        # Add basic partitioning settings
                        if hasattr(path_response.table, "partitioning_settings"):
                            ps = path_response.table.partitioning_settings
                            if ps:
                                if hasattr(ps, "partition_at_keys"):
                                    result["table"]["partitioning_settings"][
                                        "partition_at_keys"
                                    ] = ps.partition_at_keys
                                if hasattr(ps, "partition_by_size"):
                                    result["table"]["partitioning_settings"][
                                        "partition_by_size"
                                    ] = ps.partition_by_size
                                if hasattr(ps, "min_partitions_count"):
                                    result["table"]["partitioning_settings"][
                                        "min_partitions_count"
                                    ] = ps.min_partitions_count
                                if hasattr(ps, "max_partitions_count"):
                                    result["table"]["partitioning_settings"][
                                        "max_partitions_count"
                                    ] = ps.max_partitions_count

            # Convert to JSON string and return as TextContent
            formatted_result = json.dumps(result, indent=2, cls=CustomJSONEncoder)
            return [TextContent(type="text", text=formatted_result)]

        except Exception as e:
            logger.exception(f"Error describing path {path}: {e}")
            safe_error = {"error": f"Error describing path {path}: {str(e)}"}
            return [TextContent(type="text", text=json.dumps(safe_error, indent=2))]

    async def restart(self):
        """Restart the YDB connection by closing and recreating the driver."""
        logger.info("Restarting YDB connection")

        # Close session pool first
        if self.pool is not None:
            logger.info("Closing YDB session pool")
            try:
                await asyncio.shield(self.pool.stop())
            except Exception as e:
                logger.warning(f"Error closing session pool: {e}")
            self.pool = None

        # Stop the driver
        if self.driver is not None:
            logger.info("Stopping YDB driver")
            try:
                # Cancel any pending discovery tasks first
                if hasattr(self.driver, "discovery") and self.driver.discovery is not None:
                    try:
                        # Stop discovery process
                        if hasattr(self.driver.discovery, "stop"):
                            self.driver.discovery.stop()

                        # Cancel discovery task if it exists
                        if hasattr(self.driver.discovery, "_discovery_task"):
                            task = self.driver.discovery._discovery_task
                            if task and not task.done() and not task.cancelled():
                                task.cancel()
                                try:
                                    await asyncio.shield(asyncio.wait_for(task, timeout=1))
                                except (asyncio.CancelledError, asyncio.TimeoutError):
                                    pass

                    except Exception as e:
                        logger.warning(f"Error handling discovery task: {e}")

                # Stop the driver with proper error handling
                try:
                    # Use shield to prevent cancellation of the stop operation
                    await asyncio.shield(asyncio.wait_for(self.driver.stop(), timeout=5))
                except asyncio.TimeoutError:
                    logger.warning("Driver stop timed out")
                except asyncio.CancelledError:
                    logger.warning("Driver stop was cancelled")
                except Exception as e:
                    logger.warning(f"Error stopping driver: {e}")

            except Exception as e:
                logger.warning(f"Error during driver cleanup: {e}")
            finally:
                self.driver = None

        # Create new driver
        logger.info("Creating new YDB driver")
        try:
            new_driver = await self.create_driver()
            if new_driver is None:
                logger.error("Failed to create new driver during restart")
                return False
            return True
        except Exception as e:
            logger.error(f"Failed to create new driver during restart: {e}")
            return False

    def _text_content_to_dict(self, text_content_list):
        """Convert TextContent objects to serializable dictionaries.

        Args:
            text_content_list: List of TextContent objects

        Returns:
            List of dictionaries
        """
        result = []
        for item in text_content_list:
            if isinstance(item, TextContent):
                result.append({"type": item.type, "text": item.text})
            else:
                result.append(item)
        return result

    async def call_tool(self, tool_name: str, params: Dict[str, Any]) -> List[TextContent]:
        """Call a registered tool.

        Args:
            tool_name: Name of the tool to call
            params: Parameters to pass to the tool

        Returns:
            List of TextContent objects or serializable dicts

        Raises:
            ValueError: If the tool is not found
        """
        tool = self.tool_manager.get(tool_name)
        if not tool:
            raise ValueError(f"Tool not found: {tool_name}")

        logger.info(f"Calling tool: {tool_name} with params: {params}")
        try:
            result = None

            # Special handling for YDB tools to directly call methods with correct parameters
            if tool_name == "ydb_query" and "sql" in params:
                result = await self.query(sql=params["sql"])
            elif tool_name == "ydb_query_with_params" and "sql" in params and "params" in params:
                result = await self.query_with_params(sql=params["sql"], params=params["params"])
            elif tool_name == "ydb_status":
                result = await self.get_connection_status()
            elif tool_name == "ydb_list_directory" and "path" in params:
                result = await self.list_directory(path=params["path"])
            elif tool_name == "ydb_describe_path" and "path" in params:
                result = await self.describe_path(path=params["path"])
            else:
                # For other tools, use the standard handler
                result = await tool.handler(**params)

            # Convert TextContent objects to dictionaries if needed
            if isinstance(result, list) and any(isinstance(item, TextContent) for item in result):
                serializable_result = self._text_content_to_dict(result)
                return serializable_result

            # Handle any other result type
            if result is None:
                return [TextContent(type="text", text="Operation completed successfully but returned no data")]

            return result

        except Exception as e:
            logger.exception(f"Error calling tool {tool_name}: {e}")
            error_msg = f"Error executing {tool_name}: {str(e)}"
            return [TextContent(type="text", text=error_msg)]

    def get_tool_schema(self) -> List[Dict[str, Any]]:
        """Get JSON schema for all registered tools.

        Returns:
            List of tool schema definitions
        """
        return self.tool_manager.get_schema()

    def run(self):
        """Run the YDB MCP server using the FastMCP server implementation."""
        print(f"Starting YDB MCP server")
        print(f"YDB endpoint: {self.endpoint or 'Not set'}")
        print(f"YDB database: {self.database or 'Not set'}")
        logger.info(f"Starting YDB MCP server")

        # Use FastMCP's built-in run method with stdio transport
        super().run(transport="stdio")

    def get_credentials_factory(self) -> Optional[Callable[[], ydb.Credentials]]:
        """Get YDB credentials factory based on authentication mode.

        Returns:
            Callable that creates YDB credentials, or None if authentication fails
        """
        # Clear any previous auth errors
        self.auth_error = None

        supported_auth_modes = {AUTH_MODE_ANONYMOUS, AUTH_MODE_LOGIN_PASSWORD}
        if self.auth_mode not in supported_auth_modes:
            self.auth_error = f"Unsupported auth mode: {self.auth_mode}. Supported modes: {', '.join(supported_auth_modes)}"
            return None

        # If auth_mode is login_password and we have both login and password, use them
        if self.auth_mode == AUTH_MODE_LOGIN_PASSWORD:
            if not self.login or not self.password:
                self.auth_error = "Login and password must be provided for login-password authentication mode."
                return None
            logger.info(f"Using login/password authentication with user '{self.login}'")
            return self._login_password_credentials
        else:
            # Default to anonymous auth
            logger.info("Using anonymous authentication")
            return self._anonymous_credentials

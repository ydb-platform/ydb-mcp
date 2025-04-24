# YDB MCP

[Model Context Protocol server](https://modelcontextprotocol.io/) for [YDB](https://ydb.tech). It allows to work with YDB databases from any [LLM](https://en.wikipedia.org/wiki/Large_language_model) that supports MCP. This integration enables AI-powered database operations and natural language interactions with your YDB instances.

## Installation

YDB MCP can be installed using `pip`, [Python's package installer](https://pypi.org/project/pip/). The package is [available on PyPI](https://pypi.org/) and includes all necessary dependencies.

```bash
pip install ydb-mcp
```

## Usage

To get started with YDB MCP, you'll need to configure your MCP client to communicate with the YDB instance. Below is an example configuration file that you can customize according to your setup and then put into MCP client's settings.

```json
{
  "mcpServers": {
    "ydb": {
      "command": "python3",
      "args": [
        "-m", "ydb_mcp",
        "--ydb-endpoint", "grpc://localhost:2136/local"
      ]
    }
  }
}
```

### Example: Using login/password authentication

To use login/password authentication, specify the `--ydb-auth-mode`, `--ydb-login`, and `--ydb-password` arguments:

```json
{
  "mcpServers": {
    "ydb": {
      "command": "python3",
      "args": [
        "-m", "ydb_mcp",
        "--ydb-endpoint", "grpc://localhost:2136/local",
        "--ydb-auth-mode", "login-password",
        "--ydb-login", "<your-username>",
        "--ydb-password", "<your-password>"
      ]
    }
  }
}
```

## Available tools

YDB MCP provides the following tools for interacting with YDB databases:

- `ydb_query`: Run a SQL query against a YDB database
  - Parameters:
    - `sql`: SQL query string to execute

- `ydb_query_with_params`: Run a parameterized SQL query with JSON parameters
  - Parameters:
    - `sql`: SQL query string with parameter placeholders
    - `params`: JSON string containing parameter values

- `ydb_list_directory`: List directory contents in YDB
  - Parameters:
    - `path`: YDB directory path to list

- `ydb_describe_path`: Get detailed information about a YDB path (table, directory, etc.)
  - Parameters:
    - `path`: YDB path to describe

- `ydb_status`: Get the current status of the YDB connection

## Development

The project uses [Make](https://www.gnu.org/software/make/) as its primary development tool, providing a consistent interface for common development tasks.

### Available Make Commands

The project includes a comprehensive Makefile with various commands for development tasks. Each command is designed to streamline the development workflow and ensure code quality:

- `make all`: Run clean, lint, and test in sequence (default target)
- `make clean`: Remove all build artifacts and temporary files
- `make test`: Run all tests using pytest
  - Can be configured with environment variables:
    - `LOG_LEVEL` (default: WARNING) - Control test output verbosity (DEBUG, INFO, WARNING, ERROR)
- `make unit-tests`: Run only unit tests with verbose output
  - Can be configured with environment variables:
    - `LOG_LEVEL` (default: WARNING) - Control test output verbosity (DEBUG, INFO, WARNING, ERROR)
- `make integration-tests`: Run only integration tests with verbose output
  - Can be configured with environment variables:
    - `YDB_ENDPOINT` (default: grpc://localhost:2136)
    - `YDB_DATABASE` (default: /local)
    - `MCP_HOST` (default: 127.0.0.1)
    - `MCP_PORT` (default: 8989)
    - `LOG_LEVEL` (default: WARNING) - Control test output verbosity (DEBUG, INFO, WARNING, ERROR)
- `make run-server`: Start the YDB MCP server
  - Can be configured with environment variables:
    - `YDB_ENDPOINT` (default: grpc://localhost:2136)
    - `YDB_DATABASE` (default: /local)
  - Additional arguments can be passed using `ARGS="your args"`
- `make lint`: Run all linting checks (flake8, mypy, black, isort)
- `make format`: Format code using black and isort
- `make install`: Install the package in development mode
- `make dev`: Install the package in development mode with all development dependencies

### Test Verbosity Control

By default, tests run with minimal output (WARNING level) to keep the output clean. You can control the verbosity of test output using the `LOG_LEVEL` environment variable:

```bash
# Run all tests with debug output
make test LOG_LEVEL=DEBUG

# Run integration tests with info output
make integration-tests LOG_LEVEL=INFO

# Run unit tests with warning output (default)
make unit-tests LOG_LEVEL=WARNING
```

Available log levels:
- `DEBUG`: Show all debug messages, useful for detailed test flow
- `INFO`: Show informational messages and above
- `WARNING`: Show only warnings and errors (default)
- `ERROR`: Show only error messages
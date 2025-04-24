.PHONY: all clean test lint format install dev unit-tests integration-tests run-server

# Default target
all: clean lint test

# Clean build files
clean:
	rm -rf build/ dist/ *.egg-info/ __pycache__/ .pytest_cache/ .coverage htmlcov/
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete

# Run tests
test: dev
	$(eval LOG_LEVEL ?= WARNING)
	PYTHONPATH=. pytest --log-cli-level=$(LOG_LEVEL)

# Run unit tests only
unit-tests: dev
	$(eval LOG_LEVEL ?= WARNING)
	PYTHONPATH=. python -m pytest -m unit -v --log-cli-level=$(LOG_LEVEL)

# Run integration tests
integration-tests: dev
	$(eval YDB_ENDPOINT ?= grpc://localhost:2136)
	$(eval YDB_DATABASE ?= /local)
	$(eval MCP_HOST ?= 127.0.0.1)
	$(eval MCP_PORT ?= 8989)
	$(eval LOG_LEVEL ?= WARNING)
	@echo "Running integration tests with the following configuration:"
	@echo "YDB Endpoint: $(YDB_ENDPOINT)"
	@echo "YDB Database: $(YDB_DATABASE)"
	@echo "MCP Host: $(MCP_HOST)"
	@echo "MCP Port: $(MCP_PORT)"
	@echo "Log Level: $(LOG_LEVEL)"
	@echo "Note: Tests will automatically create YDB in Docker if no YDB server is running at the endpoint"
	YDB_ENDPOINT=$(YDB_ENDPOINT) YDB_DATABASE=$(YDB_DATABASE) MCP_HOST=$(MCP_HOST) MCP_PORT=$(MCP_PORT) PYTHONPATH=. python -m pytest -m integration -v --log-cli-level=$(LOG_LEVEL)

# Run server
run-server:
	$(eval YDB_ENDPOINT ?= grpc://localhost:2136)
	$(eval YDB_DATABASE ?= /local)
	YDB_ENDPOINT=$(YDB_ENDPOINT) YDB_DATABASE=$(YDB_DATABASE) python -m ydb_mcp $(ARGS)

# Run lint checks
lint: dev
	flake8 ydb_mcp tests
	mypy ydb_mcp
	black --check ydb_mcp tests
	isort --check-only --profile black ydb_mcp tests

# Format code
format: dev
	black ydb_mcp tests
	isort --profile black ydb_mcp tests

# Install package
install:
	pip install -e .

# Install development dependencies
dev:
	pip install -e ".[dev]"
	pip install -r requirements-dev.txt
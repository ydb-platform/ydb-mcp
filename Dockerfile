FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ydb_mcp ./ydb_mcp
COPY setup.py .

# Install the package
RUN pip install --no-cache-dir -e .

# Expose the server port
EXPOSE 8080

# Set environment variables
ENV YDB_ENDPOINT=""
ENV YDB_DATABASE=""

# Run the server
CMD ["ydb-mcp", "--host", "0.0.0.0", "--port", "8080"]
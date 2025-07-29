"""Main entry point for running the YDB MCP server."""

import argparse
import logging
import os
import sys

from ydb_mcp.server import (
    AUTH_MODE_ACCESS_TOKEN,
    AUTH_MODE_ANONYMOUS,
    AUTH_MODE_LOGIN_PASSWORD,
    AUTH_MODE_SERVICE_ACCOUNT,
    YDBMCPServer,
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Model Context Protocol server for YDB")

    parser.add_argument(
        "--ydb-endpoint",
        type=str,
        default=os.environ.get("YDB_ENDPOINT"),
        help="YDB endpoint (overrides YDB_ENDPOINT env var)",
    )
    parser.add_argument(
        "--ydb-database",
        type=str,
        default=os.environ.get("YDB_DATABASE"),
        help="YDB database path (overrides YDB_DATABASE env var)",
    )
    parser.add_argument(
        "--ydb-login",
        type=str,
        default=os.environ.get("YDB_LOGIN"),
        help="YDB login (overrides YDB_LOGIN env var)",
    )
    parser.add_argument(
        "--ydb-password",
        type=str,
        default=os.environ.get("YDB_PASSWORD"),
        help="YDB password (overrides YDB_PASSWORD env var)",
    )
    parser.add_argument(
        "--ydb-auth-mode",
        type=str,
        default=os.environ.get("YDB_AUTH_MODE"),
        choices=["anonymous", "login-password", "access-token", "service-account"],
        help="YDB authentication mode (overrides YDB_AUTH_MODE env var)",
    )
    parser.add_argument(
        "--ydb-access-token",
        type=str,
        default=os.environ.get("YDB_ACCESS_TOKEN"),
        help="YDB access token (overrides YDB_ACCESS_TOKEN env var)",
    )
    parser.add_argument(
        "--ydb-sa-key-file",
        type=str,
        default=os.environ.get("YDB_SA_KEY_FILE"),
        help="YDB service account key file (overrides YDB_SA_KEY_FILE env var)",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    return parser.parse_args()


def main():
    """Run the YDB MCP server."""
    args = parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Validate auth mode and required credentials
    supported_auth_modes = {
        AUTH_MODE_ANONYMOUS,
        AUTH_MODE_LOGIN_PASSWORD,
        AUTH_MODE_ACCESS_TOKEN,
        AUTH_MODE_SERVICE_ACCOUNT,
    }
    auth_mode = args.ydb_auth_mode or AUTH_MODE_ANONYMOUS
    if auth_mode not in supported_auth_modes:
        print(
            f"Error: Unsupported auth mode: {auth_mode}. Supported modes: {', '.join(supported_auth_modes)}",
            file=sys.stderr,
        )
        exit(1)
    if auth_mode == AUTH_MODE_LOGIN_PASSWORD:
        if not args.ydb_login or not args.ydb_password:
            print(
                "Error: --ydb-login and --ydb-password are required for login-password authentication mode.",
                file=sys.stderr,
            )
            exit(1)
    if auth_mode == AUTH_MODE_ACCESS_TOKEN:
        if not args.ydb_access_token:
            print(
                "Error: --ydb-access-token is required for access-token authentication mode.",
                file=sys.stderr,
            )
            exit(1)
    if auth_mode == AUTH_MODE_SERVICE_ACCOUNT:
        if not args.ydb_sa_key_file:
            print(
                "Error: --ydb-sa-key-file is required for service-account authentication mode.",
                file=sys.stderr,
            )
            exit(1)

    # Set environment variables for YDB if provided via arguments
    if args.ydb_endpoint:
        os.environ["YDB_ENDPOINT"] = args.ydb_endpoint
    if args.ydb_database:
        os.environ["YDB_DATABASE"] = args.ydb_database
    if args.ydb_auth_mode:
        os.environ["YDB_AUTH_MODE"] = args.ydb_auth_mode
    if args.ydb_login:
        os.environ["YDB_LOGIN"] = args.ydb_login
    if args.ydb_password:
        os.environ["YDB_PASSWORD"] = args.ydb_password
    if args.ydb_sa_key_file:
        os.environ["YDB_SA_KEY_FILE"] = args.ydb_sa_key_file
    if args.ydb_access_token:
        os.environ["YDB_ACCESS_TOKEN"] = args.ydb_access_token

    # Create and run the server
    server = YDBMCPServer(
        endpoint=args.ydb_endpoint,
        database=args.ydb_database,
        auth_mode=auth_mode,
        login=args.ydb_login,
        password=args.ydb_password,
        access_token=args.ydb_access_token,
        sa_key_file=args.ydb_sa_key_file,
    )

    print("Starting YDB MCP server with stdio transport")
    print(f"YDB endpoint: {args.ydb_endpoint or 'Not set'}")
    print(f"YDB database: {args.ydb_database or 'Not set'}")
    print(f"YDB login: {'Set' if args.ydb_login else 'Not set'}")
    print(f"YDB auth mode: {args.ydb_auth_mode or 'Default (anonymous)'}")

    server.run()


if __name__ == "__main__":
    main()

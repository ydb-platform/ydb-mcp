"""Main entry point for running the YDB MCP server."""

import argparse
import logging
import os
import sys

from ydb_mcp.server import YDBMCPServer


def main() -> None:
    parser = argparse.ArgumentParser(description="Model Context Protocol server for YDB")
    parser.add_argument(
        "--ydb-endpoint",
        default=os.environ.get("YDB_ENDPOINT"),
        help="YDB endpoint (overrides YDB_ENDPOINT env var)",
    )
    parser.add_argument(
        "--ydb-database",
        default=os.environ.get("YDB_DATABASE"),
        help="YDB database path (overrides YDB_DATABASE env var)",
    )
    parser.add_argument(
        "--ydb-auth-mode",
        default=os.environ.get("YDB_AUTH_MODE", "anonymous"),
        choices=["anonymous", "login-password", "access-token", "service-account"],
        help="Authentication mode (overrides YDB_AUTH_MODE env var)",
    )
    parser.add_argument(
        "--ydb-login",
        default=os.environ.get("YDB_LOGIN"),
        help="Login for login-password auth (overrides YDB_LOGIN env var)",
    )
    parser.add_argument(
        "--ydb-password",
        default=os.environ.get("YDB_PASSWORD"),
        help="Password for login-password auth (overrides YDB_PASSWORD env var)",
    )
    parser.add_argument(
        "--ydb-access-token",
        default=os.environ.get("YDB_ACCESS_TOKEN"),
        help="Access token (overrides YDB_ACCESS_TOKEN env var)",
    )
    parser.add_argument(
        "--ydb-sa-key-file",
        default=os.environ.get("YDB_SA_KEY_FILE"),
        help="Service account key file (overrides YDB_SA_KEY_FILE env var)",
    )
    parser.add_argument(
        "--ydb-root-certificates",
        default=os.environ.get("YDB_ROOT_CERTIFICATES"),
        help="Path to root CA certificate file for TLS (overrides YDB_ROOT_CERTIFICATES env var)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )

    parser.add_argument(
        "--ydb-disable-discovery",
        action="store_true",
        help="Disable discovery of endpoints",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        server = YDBMCPServer(
            endpoint=args.ydb_endpoint,
            database=args.ydb_database,
            auth_mode=args.ydb_auth_mode,
            login=args.ydb_login,
            password=args.ydb_password,
            access_token=args.ydb_access_token,
            sa_key_file=args.ydb_sa_key_file,
            root_certificates=args.ydb_root_certificates,
            disable_discovery=args.ydb_disable_discovery,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    server.run()


if __name__ == "__main__":
    main()

"""Mock classes for testing."""

from typing import Any, Callable, Dict, Optional, Type


class MockRequestHandler:
    """Mock for mcp.server.handler.RequestHandler class."""

    def __init__(self):
        """Initialize the mock handler."""
        self.config = None


def mock_register_handler(name: str) -> Callable[[Type], Type]:
    """Mock for mcp.server.handler.register_handler decorator.

    Args:
        name: Name of the handler

    Returns:
        Decorator function
    """

    def decorator(cls):
        """Decorator function.

        Args:
            cls: Class to decorate

        Returns:
            The decorated class
        """
        return cls

    return decorator

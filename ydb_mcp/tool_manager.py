from typing import Any, Callable, Dict, List, Optional


class ToolDefinition:
    """Defines a tool that can be called by the MCP."""

    def __init__(
        self, name: str, handler: Callable, description: str = "", parameters: Optional[Dict] = None
    ):
        """Initialize a tool definition.

        Args:
            name: Name of the tool
            handler: Async callable that handles the tool execution
            description: Tool description
            parameters: JSON schema for the tool parameters
        """
        self.name = name
        self.handler = handler
        self.description = description
        self.parameters = parameters or {}


class ToolManager:
    """Manages MCP tools for YDB interactions."""

    def __init__(self):
        """Initialize the tool manager."""
        self._tools: Dict[str, ToolDefinition] = {}

    def register_tool(
        self, name: str, handler: Callable, description: str = "", parameters: Optional[Dict] = None
    ) -> None:
        """Register a tool with the manager.

        Args:
            name: Name of the tool
            handler: Async callable that handles the tool execution
            description: Tool description
            parameters: JSON schema for tool parameters
        """
        self._tools[name] = ToolDefinition(
            name=name, handler=handler, description=description, parameters=parameters
        )

    def get(self, name: str) -> Optional[ToolDefinition]:
        """Get a tool by name.

        Args:
            name: Name of the tool to retrieve

        Returns:
            Tool definition if found, None otherwise
        """
        return self._tools.get(name)

    def get_all_tools(self) -> Dict[str, ToolDefinition]:
        """Get all registered tools.

        Returns:
            Dictionary of tool name to tool definition
        """
        return self._tools

    def get_schema(self) -> List[Dict[str, Any]]:
        """Get JSON schema for all registered tools.

        Returns:
            List of tool schema definitions
        """
        result = []
        for name, tool in self._tools.items():
            tool_schema = {
                "name": name,
                "description": tool.description,
            }

            if tool.parameters:
                tool_schema["parameters"] = tool.parameters

            result.append(tool_schema)

        return result

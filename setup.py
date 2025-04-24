#!/usr/bin/env python3
"""YDB MCP - Model Context Protocol server for YDB."""

import os
from setuptools import setup, find_packages


def read(fname):
    """Read file content."""
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


setup(
    name="ydb-mcp",
    version="0.1.0",
    author="MCP Team",
    author_email="mcp@example.com",
    description="Model Context Protocol server for YDB",
    long_description=read("README.md") if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    url="https://github.com/blinkov/ydb-mcp",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "ydb>=2.11.0",
        "mcp-server>=0.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.3.1",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.3.0",
            "isort>=5.12.0",
            "mypy>=1.3.0",
            "flake8>=6.0.0",
        ],
    }
)
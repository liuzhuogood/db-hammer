from pathlib import Path

from setuptools import find_packages, setup

with open(Path(__file__).parent / "README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="db-hammer",
    version="0.1.0",
    description="db-hammer database toolkit with MCP server",
    author="hammer",
    author_email="liuzhuogood@foxmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests", "docs")),
    include_package_data=True,
    package_data={
        "db_hammer": [
            "mcp/templates/*.yaml",
            "mcp/web/templates/*.html",
            "mcp/web/static/css/*.css",
            "mcp/web/static/js/*.js",
            "mcp/web/downloads/*.js",
        ]
    },
    install_requires=[],
    extras_require={
        "mcp": [
            "fastmcp>=0.1.0",
            "pydantic>=2.0.0",
            "PyYAML>=6.0.0",
            "Flask>=2.3.0",
            "requests>=2.31.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "db_hammer_mcp=db_hammer.cli.mcp_server:main",
        ]
    },
)

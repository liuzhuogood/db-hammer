from setuptools import find_packages, setup

with open("./README.md", "rb") as fh:
    long_description = fh.read()

setup(
    name="db-hammer",
    version="0.0.50",
    description="database tools；数据库操作",
    author="hammer",
    author_email="liuzhuogood@foxmail.com",
    long_description=str(long_description, encoding="utf-8"),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    package_data={
        "db_hammer": [
            "mcp/templates/*.yaml",
            "mcp/web/templates/*.html",
            "mcp/web/static/css/*.css",
            "mcp/web/static/js/*.js",
        ]
    },
    entry_points={
        "console_scripts": [
            "db_hammer_mcp=db_hammer.cli.mcp_server:main",
        ]
    },
    extras_require={
        "mcp": [
            "fastmcp>=0.1.0",
            "pydantic>=2.0.0",
            "PyYAML>=6.0.0",
            "cryptography>=3.4.0",
            "python-jose[cryptography]>=3.3.0",
            "Flask>=2.3.0",
            "requests>=2.31.0",
        ]
    },
)

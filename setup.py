from setuptools import find_packages, setup

with open("./README.md", "rb") as fh:
    long_description = fh.read().decode("utf-8")

setup(
    name="db-hammer",
    version="0.0.50",
    description="database tools；数据库操作",
    author="hammer",
    author_email="liuzhuogood@foxmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=["db_hammer", "db_hammer.*"]),
    package_data={
        "db_hammer": [
            "mcp/templates/*.yaml",
            "web/templates/*.html",
            "web/static/css/*.css",
            "web/static/js/*.js",
            "web/downloads/*.js",
        ]
    },
    install_requires=[],
    extras_require={
        "mcp": [
            "fastmcp>=0.1.0",
            "PyYAML>=6.0.0",
            "requests>=2.31.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "db_hammer_mcp=db_hammer.cli.mcp_server:main",
        ]
    },
)

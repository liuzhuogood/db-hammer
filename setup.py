from setuptools import setup

with open("./README.md", "rb") as fh:
    long_description = fh.read()

setup(
    name='db-hammer',
    version='0.0.31',
    description='database tools；数据库操作',
    author='hammer',
    author_email='liuzhuogood@foxmail.com',
    long_description=str(long_description, encoding='utf-8'),
    long_description_content_type="text/markdown",
    packages=['db_hammer', 'db_hammer.util', 'db_hammer.auto'],
    package_data={'db_hammer': ['README.md', 'LICENSE']},
    install_requires=[]
)

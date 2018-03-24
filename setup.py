"""
Setup file describing how to install.
"""

from setuptools import setup

setup(
    name=u'sqlcollection',
    version=u'0.1.1',
    packages=[u'sqlcollection', u'sqlcollection.results'],
    install_requires=[u'SQLAlchemy'],
    download_url = 'https://github.com/knlambert/sqlcollection/archive/0.1.1.tar.gz',
    url=u'https://github.com/knlambert/sql-collection.git',
    keywords=[]
)
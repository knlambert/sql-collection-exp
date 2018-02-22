"""
Setup file describing how to install.
"""

from setuptools import setup

setup(
    name=u'sqlcollection',
    version=u'0.1',
    packages=[u'sqlcollection', u'sqlcollection.results'],
    install_requires=[u'SQLAlchemy'],
    url=u'https://github.com/knlambert/sql-collection.git',
    keywords=[]
)
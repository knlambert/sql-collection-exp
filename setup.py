"""
Setup file describing how to install.
"""

from setuptools import setup

setup(
    name='sqlcollection',
    version='0.2.1',
    packages=['sqlcollection', 'sqlcollection.results'],
    install_requires=["SQLAlchemy>=1.2,<2"],
    url='https://github.com/knlambert/sql-collection.git',
    keywords=[]
)
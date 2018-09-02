"""
Setup file describing how to install.
"""

from setuptools import setup

setup(
<<<<<<< HEAD
    name=u'sqlcollection',
    version=u'0.1.4',
    packages=[u'sqlcollection', u'sqlcollection.results'],
    install_requires=[
        u'SQLAlchemy>=1.2,<2'
    ],
    download_url = u'https://github.com/knlambert/sqlcollection/archive/0.1.4.tar.gz',
    url=u'https://github.com/knlambert/sql-collection.git',
=======
    name='sqlcollection',
    version='0.2.0',
    packages=['sqlcollection', 'sqlcollection.results'],
    install_requires=["SQLAlchemy>=1.2,<2"],
    url='https://github.com/knlambert/sql-collection.git',
>>>>>>> Update requirements.
    keywords=[]
)
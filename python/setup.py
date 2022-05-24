#!/usr/bin/env python
# coding=utf-8


import pathlib
from setuptools import setup

current_dir = pathlib.Path(__file__).parent

readme = (current_dir / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="raphtory-client",
    version="0.1.4",
    description="Raphtory Python Client - Temporal Graph Analytics",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/Raphtory/Raphtory/tree/master/python",
    author="Haaroon Yousaf",
    author_email="haaroon.yousaf@pometry.com",
    license="Apache License 2.0",
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Libraries',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities'
    ],
    keywords='raphtory pulsar raphtory-client client graph analytics temporal',
    packages=['raphtoryclient'],
    include_package_data=True,
    install_requires= [
        'certifi',
        'charset-normalizer',
        'fastavro',
        'idna',
        'networkx',
        'numpy',
        'pandas',
        'pulsar-client',
        'python-dateutil',
        'pytz',
        'requests',
        'six',
        'urllib3',
        'py4j'
    ],
)
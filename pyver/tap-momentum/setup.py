#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-momentum",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_momentum"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-momentum=tap_momentum:main
    """,
    packages=["tap_momentum"],
    package_data = {
        "schemas": ["tap_momentum/schemas/*.json"]
    },
    include_package_data=True,
)

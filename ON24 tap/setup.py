#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-on24",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_on24"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-on24=tap_on24:main
    """,
    packages=["tap_on24"],
    package_data = {
        "schemas": ["tap_on24/schemas/*.json"]
    },
    include_package_data=True,
)

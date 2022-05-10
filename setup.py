#!/usr/bin/env python
import os
import os.path
from setuptools import setup, find_packages

version_file = os.path.join(os.path.dirname(__file__), "dbting", "VERSION")
with open(version_file) as f:
    version = f.read().strip()


setup(
    name="dbt-ing",
    version=version,
    description="dbt Ingestion Framework",
    long_description="dbt Ingestion Framework",
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="dbt",
    author="Andrea Bonomi",
    author_email="andrea.bonomi@gmail.com",
    url="http://github.com/andreax79/dbt-ing",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples"]),
    include_package_data=True,
    zip_safe=True,
    install_requires=[line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))],
    entry_points={
        "console_scripts": [
            "dbting=dbting.cli:cli",
        ],
    },
    test_suite="tests",
    tests_require=["nose"],
)

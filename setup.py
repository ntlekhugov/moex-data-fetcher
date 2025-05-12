#!/usr/bin/env python3
"""
Setup script for MOEX Data Fetcher package.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="moex-data-fetcher",
    version="0.1.0",
    author="Nik",
    author_email="ntlekhugov@gmail.com",
    description="A Python module for fetching data from the Moscow Exchange (MOEX) ISS API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ntlekhugov/moex-data-fetcher",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=2.0.0",
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "tqdm>=4.66.1",
    ],
)
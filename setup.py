#!/usr/bin/env python3
# coding=utf-8
import json
import os
from os import path

from setuptools import setup

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    # read file but remove first 4 lines
    long_description = f.read().split("\n", 4)[4]

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "sfctss", "meta.json"), "r") as f:
    meta = json.load(f)

setup(
    name=meta["title"],
    version=meta["version"],
    description='A Service Function Chain (SFC) Traffic Scheduling Simulator',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/mblo/sfctss',
    license=meta["license"],
    author=meta["author"],
    package_data={
        'sfctss': ['meta.json'],
    },
    install_requires=[
        "numpy>=1.19.5",
        "sortedcontainers>=2.3.0",
        "terminaltables>=3.1.0"
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=["sfctss", "sfctss.scheduler", "sfctss.model"],
    python_requires='>=3.6, <4'
)

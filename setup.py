#!/usr/bin/env python
import os

from redispipeline import __version__

from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    long_description = f.read()

setup(
    name='redispipeline',
    version=__version__,
    description='Aync pipelined Python client for Redis',
    long_description=long_description,
    url='http://github.com/RedisLabs/redis-pipeline',
    author='Yoav Steinberg',
    author_email='yoav@redislabs.com',
    maintainer='Yoav Steinebrg',
    maintainer_email='yoav@redislabs.com',
    keywords=['Redis'],
    license='MIT',
    packages=['redispipeline'],
    test_suite='test.suite'
)


#!/usr/bin/env python

# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

readme = open('README.md').read()

setup(
    # Change these when cloning this repo!
    name='leanplum_data_export',
    description='A job to export leanplum data to BigQuery',
    author='Frank Bertsch',
    author_email='frank@mozilla.com',
    url='https://github.com/mozilla/leanplum_data_export',
    packages=find_packages(include=['leanplum_data_export']),
    package_dir={'leanplum-data-export': 'leanplum_data_export'},
    entry_points={
        'console_scripts': [
            'leanplum-data-export=leanplum_data_export.__main__:main',
        ],
    },

    # These don't necessarily need changed
    python_requires='>=3.6.0',
    version='0.0.0',
    long_description=readme,
    include_package_data=True,
    install_requires=[
        'click',
    ],
    license='Mozilla',
)

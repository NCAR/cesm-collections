#!/usr/bin/env python3

"""The setup script."""

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().strip().split('\n')

with open('README.md') as f:
    long_description = f.read()

setup(
    maintainer='NCAR XDev Team',
    maintainer_email='xdev@ucar.edu',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
    ],
    description='CESM Data Collections',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=long_description,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='cesm,prefect,cesm-collections',
    name='esm_collections',
    packages=find_packages(include=['esm_collections']),
    entry_points={},
    url='https://github.com/NCAR/esm-collections',
    project_urls={
        'Documentation': 'https://github.com/NCAR/esm-collections',
        'Source': 'https://github.com/NCAR/esm-collections',
        'Tracker': 'https://github.com/NCAR/esm-collections/issues',
    },
    use_scm_version={
        'version_scheme': 'post-release',
        'local_scheme': 'dirty-tag',
    },
    setup_requires=['setuptools_scm', 'setuptools>=30.3.0'],
    zip_safe=False,
)

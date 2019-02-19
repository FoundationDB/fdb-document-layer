from setuptools import setup, find_packages

VERSION = '0.0.1'

setup(
    name='correctness',
    version=VERSION,

    packages=find_packages(),

    install_requires=[
        'pytest',
        'pymongo==3.6.1',
        'python-dateutil',
        'PyYAML==3.12',
        'psutil',
        'coloredlogs==4.0.0'
    ],
)
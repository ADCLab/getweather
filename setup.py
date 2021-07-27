import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "getweather",
    version = "0.0.1",
    author = "Adan E Vela",
    author_email = "adan.vela@ucf.edu",
    description = ("Simple collection of code to get weather data from local databases"),
    license = "Apache License 2.0",
    keywords = "asos weather database mysql",
    url = "https://github.com/ADCLab/getweather",
    packages=['asos', 'tests'],
    install_requires=['numpy', 'pandas','geopy','methodtools','remoteDBconnector'], #external packages as dependencies
    long_description=read('README.md'),
)
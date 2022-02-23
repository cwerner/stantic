# -*- coding: utf-8 -*-

import glob
import os

from setuptools import find_packages, setup

DISTNAME = "stantic"
LICENSE = "MIT"
AUTHOR = "Christian Werner"
AUTHOR_EMAIL = "christian.werner@kit.edu"
URL = "https://github.com/cwerner/stanic"
DESCRIPTION = "STA client using pydantic magic"
CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "License :: OSI Approved :: MIT",
    "Operating System :: OS Independent",
    "Intended Audience :: Science/Research",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Topic :: Scientific/Engineering",
]

PYTHON_REQUIRES = ">=3.7"

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

# get the dependencies and installs
with open(os.path.join(here, "requirements.txt"), encoding="utf-8") as f:
    all_reqs = f.read().split("\n")

INSTALL_REQUIRES = [x.strip() for x in all_reqs if "git+" not in x]
dependency_links = [x.strip().replace("git+", "") for x in all_reqs if "git+" not in x]

setup(
    name=DISTNAME,
    version="0.1",
    license=LICENSE,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    classifiers=CLASSIFIERS,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    url=URL,
    packages=find_packages("src", exclude=["docs", "tests"]),
    install_requires=INSTALL_REQUIRES,
    package_dir={"": "src"},
    py_modules=[
        os.path.splitext(os.path.basename(p))[0] for p in glob.glob("src/*.py")
    ],
    include_package_data=True,
    # package_dir={"ldndctools": "ldndctools"},
    # package_data={"ldndctools": ["data/catalog.yml"]},
    # include_package_data=True,
    # entry_points={
    #    "console_scripts": [
    #        "dlsc=ldndctools.dlsc:main",
    #        "nlcc=ldndctools.nlcc:main",
    #        "nlcc_split4db=ldndctools.nlcc_split4db:main",
    #    ]
    # },
    dependency_links=dependency_links,
)

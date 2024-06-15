import os
from setuptools import setup, find_packages

# Read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="dagpipe",
    version="0.1.0",
    author="Wojciech Bogobowicz",
    author_email="wojciech.bogobowicz@gmail.com",
    description="Package that allows user to define lazy evaluated dag pipeline.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WojciechBogobowicz/dagpipe",
    packages=find_packages(exclude=['notebooks']),
    include_package_data=True,
    package_data={
        'dagpipe': ['resources/*.png'],
    },
    install_requires=[],
    extras_require={
        'viz': ["graphviz", "matplotlib"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

# run setup
# python setup.py sdist bdist_wheel
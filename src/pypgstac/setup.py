"""Fake pypgstac setup.py for github."""
import sys

from setuptools import setup

sys.stderr.write(
    """
===============================
Unsupported installation method
===============================
pypgstac no longer supports installation with `python setup.py install`.
Please use `python -m pip install .` instead.
""",
)
sys.exit(1)


# The below code will never execute, however GitHub is particularly
# picky about where it finds Python packaging metadata.
# See: https://github.com/github/feedback/discussions/6456
#
# To be removed once GitHub catches up.

setup(
    name="pypgstac",
    install_requires=[
        "smart-open>=4.2,<7.0",
        "orjson>=3.5.2",
        "python-dateutil==2.8.*",
        "fire==0.4.*",
        "plpygis==0.2.*",
        "pydantic[dotenv]==1.10.*",
        "tenacity==8.1.*",
        "version-parser>= 1.0.1",
    ],
)

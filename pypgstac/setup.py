"""pypgstac: python utilities for working with pgstac."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "smart-open==4.2.*",
    "orjson>=3.5.2",
    "python-dateutil==2.8.*",
    "fire==0.4.*",
    "psycopg[binary]==3.0.*",
    "psycopg-pool==3.1.*",
    "plpygis==0.2.*",
    "pydantic[dotenv]==1.9.*",
    "tenacity==8.0.*",
]

extra_reqs = {
    "dev": [
        "pytest==5.*",
        "flake8==3.9.*",
        "black>=21.7b0",
        "mypy>=0.910",
        "types-orjson==0.1.1",
    ],
}


setup(
    name="pypgstac",
    description="Planetary Computer Tasks framework.",
    long_description=desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="stac, postgres",
    author="David Bitner",
    author_email="bitner@dbspatial.com",
    url="https://github.com/Microsoft/planetary-computer",
    license="MIT",
    packages=find_namespace_packages(exclude=["tests", "scripts"]),
    package_data={"": ["py.typed"], "migrations": ["pypgstac/migrations/pgstac*.sql"]},
    zip_safe=False,
    install_requires=install_requires,
    tests_require=extra_reqs["dev"],
    extras_require=extra_reqs,
)

FROM python:3.9-slim-bullseye as pybase
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
RUN \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        postgresql-client \
        tini \
    && python3 -m pip install --upgrade \
        pip \
        jupyter \
        pypgstac[psycopg] \
        stac-fastapi-pgstac \
        pystac \
        pystac-client \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*
COPY docker/starttutorial /usr/bin/starttutorial
COPY tutorial.ipynb /tutorial/tutorial.ipynb
COPY collections.ndjson.gz /data/collections.ndjson.gz
COPY items.ndjson.gz /data/items.ndjson.gz

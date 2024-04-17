FROM rust:1-slim-bullseye as pyrustbase
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV PYTHONPATH="/opt/src/pypgstac:$PYTHONPATH"
ENV PATH="/opt/docker/pypgstac/bin:$PATH"
RUN \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        postgresql-client \
        python3 python-is-python3 python3-pip python3-venv \
        build-essential clang clang-11 gcc git gnupg libssl-dev llvm-11 lsb-release make pkg-config \
    && python3 -m pip install --upgrade pip toml-to-requirements wheel setuptools \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

FROM pyrustbase as pypgstac
COPY ./src/pypgstac/pyproject.toml /tmp/pyproject.toml
WORKDIR /tmp
RUN \
    toml-to-req --optional-lists test,build,dev,psycopg,migrations \
    && pip install -r /tmp/requirements.txt
COPY docker/pypgstac/bin /opt/docker/pypgstac/bin
COPY src/pypgstac /opt/src/pypgstac
COPY src/pgstac/migrations /opt/src/pypgstac/pypgstac/migrations
WORKDIR /opt/src/pypgstac
RUN pip install -e . && rm -rf /usr/local/cargo/registry

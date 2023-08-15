
FROM python:3-slim-bullseye AS pyreqs
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
RUN pip3 install --upgrade pip toml-to-requirements
COPY ./src/pypgstac/pyproject.toml /tmp/pyproject.toml
RUN toml-to-req --toml-file /tmp/pyproject.toml --include-optional >/tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt --target /py


FROM rust:1-slim-bullseye as rustbuilder
WORKDIR /workspace
COPY src/pypgstac/Cargo.toml .
COPY src/pypgstac/src src
RUN ls && ls src
RUN cargo build

FROM python:3-slim-bullseye AS pypgstac1
ENV PTYHONPATH=/pypgstac:/py:$PYTHONPATH
COPY --from=pyreqs /py /py
COPY src/pypgstac /pypgstac
COPY --from=rustbuilder /workspace/target/debug/libpgstacrs.so /pypgstac/pgstacrs/libpgstacrs.so

FROM rust:1-slim-bullseye as pypgstac
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV PYTHONPATH="/opt/py:/opt/pypgstac:$PYTHONPATH"
ENV PATH="/opt/bin:/opt/py/bin:$PATH"
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
COPY ./src/pypgstac/pyproject.toml /tmp/pyproject.toml
WORKDIR /tmp
RUN \
    toml-to-req --include-optional \
    && pip install -r /tmp/requirements.txt
COPY docker/pypgstac/bin /opt/bin
COPY src/pypgstac /opt/pypgstac
WORKDIR /opt/pypgstac
RUN pip install -e .

# FROM ghcr.io/pyo3/maturin:latest as maturin
# COPY src/pypgstac /opt/pypgstac
# WORKDIR /opt/pypgstac
# RUN maturin build --release

# FROM python:3-slim-bullseye AS pypgstac
# ENV PYTHONPATH=/opt/pypgstac:$PYTHONPATH
# WORKDIR /workspace
# COPY --from=maturin /opt/pypgstac/target/wheels/*.whl .
# RUN pip install *.whl --target /opt/pypgstac

# syntax=docker/dockerfile:1.7
FROM rust:1-slim-trixie AS pyrustbase
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV PYTHONPATH="/opt/src/pypgstac:$PYTHONPATH"
ENV PATH="/opt/pgstac/container-scripts:$PATH"
ENV UV_CACHE_DIR=/root/.cache/uv
ARG PG_MAJOR=17
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        postgresql-client-${PG_MAJOR} \
        python3 python-is-python3 python3-pip python3-venv \
        build-essential clang gcc git libssl-dev llvm make pkg-config \
    && curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

FROM pyrustbase AS pypgstac
COPY ./src/pypgstac/pyproject.toml /tmp/pyproject.toml
WORKDIR /tmp
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv pip compile /tmp/pyproject.toml \
    --extra dev \
    --extra test \
    --extra psycopg \
    --extra migrations \
    >/tmp/requirements.txt \
    && uv pip install --system -r /tmp/requirements.txt
COPY scripts/container-scripts /opt/pgstac/container-scripts
COPY src/pypgstac /opt/src/pypgstac
COPY src/pgstac /opt/src/pgstac
WORKDIR /opt/src/pypgstac
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv pip install --system -e . \
    && rm -rf /usr/local/cargo/registry

RUN addgroup --gid 1000 user && \
    adduser --uid 1000 --gid 1000 --disabled-password --gecos "" --home /home/user user && \
    chown -R user:user /opt/src/pypgstac /opt/src/pgstac
USER user

# Optional runtime-optimized image: no build toolchain, only pypgstac package + runtime deps.
FROM python:3.13-slim-trixie AS pypgstac-runtime
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PATH="/opt/pgstac/container-scripts:$PATH"
ENV UV_CACHE_DIR=/root/.cache/uv
ARG PG_MAJOR=17
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        postgresql-client-${PG_MAJOR} \
    && curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY ./src/pypgstac/pyproject.toml /tmp/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv pip compile /tmp/pyproject.toml \
    --extra psycopg \
    --extra migrations \
    >/tmp/requirements.txt \
    && uv pip install --system -r /tmp/requirements.txt
COPY scripts/container-scripts /opt/pgstac/container-scripts
COPY src/pypgstac /opt/src/pypgstac
COPY src/pgstac /opt/src/pgstac
WORKDIR /opt/src/pypgstac
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv pip install --system .

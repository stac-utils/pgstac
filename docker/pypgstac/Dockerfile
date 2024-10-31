# Image used to build wheels for pypgstac
FROM ghcr.io/pyo3/maturin AS pypgstacbuilder
WORKDIR /io
RUN --mount=type=bind,source=./src/pypgstac,target=/io \
    --mount=type=bind,source=./src/pgstac/migrations,target=/io/python/pypgstac/migrations \
    --mount=type=cache,target=/target,sharing=locked \
    maturin build --release --out /wheels --target-dir /target
ENTRYPOINT []
CMD []

# Image that contains pypgstac but not psql, dev, or test
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS pypgstac-slim-base
ENV \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/venv \
    UV_PYTHON_INSTALL_DIR=/venv/bin \
    VIRTUAL_ENVIRONMENT=/venv \
    PATH=/venv/bin:$PATH
WORKDIR /opt/src/pypgstac
RUN \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./src/pypgstac/uv.lock,target=/opt/src/pypgstac/uv.lock \
    --mount=type=bind,source=./src/pypgstac/pyproject.toml,target=/opt/src/pypgstac/pyproject.toml \
    uv venv --allow-existing --relocatable /venv \
    && uv sync --frozen --no-install-project --extra psycopg

ENTRYPOINT []
CMD ["/bin/true"]

# Base image with psql and all dependencies
FROM pypgstac-slim-base AS pypgstac-base
ENV \
    PATH=/scripts:/venv/bin:$PATH \
    SCRIPTS=/scripts \
    PGSTACDIR=/src/pgstac \
    PYPGSTACDIR=/src/pypgstac \
    MIGRATIONSDIR=/src/pgstac/migrations \
    TESTS=/src/pypgstac/tests
WORKDIR /opt/src/pypgstac
RUN \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./src/pypgstac/uv.lock,target=/opt/src/pypgstac/uv.lock \
    --mount=type=bind,source=./src/pypgstac/pyproject.toml,target=/opt/src/pypgstac/pyproject.toml \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client libpq-dev python3-dev \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && uv sync --frozen --no-install-project --extra test --extra dev --extra migrations --extra psycopg \
    && uv pip install setuptools
COPY ./docker/pypgstac/bin /scripts


# slim image for running pypgstac
FROM pypgstac-slim-base AS pypgstac-slim
COPY ./docker/pypgstac/bin/ /scripts/
COPY ./src /src
RUN --mount=type=bind,from=pypgstacbuilder,source=/wheels,target=/tmp/wheels \
    uv pip install --offline --find-links /tmp/wheels pypgstac

# Loaded image for running pypgstac and test suite
FROM pypgstac-base AS pypgstac
COPY ./docker/pypgstac/bin/ /scripts/
COPY ./src /src
RUN --mount=type=bind,from=pypgstacbuilder,source=/wheels,target=/tmp/wheels \
    uv pip install --offline --find-links /tmp/wheels pypgstac





# # Image with tooling for compiling rust hydration code
# FROM rust:1-slim-bullseye AS pypgstacbuilder
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
# ENV \
#     UV_LINK_MODE=copy \
#     UV_COMPILE_BYTECODE=1 \
#     UV_PROJECT_ENVIRONMENT=/venv \
#     UV_PYTHON_INSTALL_DIR=/venv/bin \
#     VIRTUAL_ENVIRONMENT=/venv \
#     PATH=/venv/bin:$PATH
# RUN --mount=type=cache,target=/root/.cache/uv \
#     --mount=type=cache,target=/var/cache/apt,sharing=locked \
#     --mount=type=cache,target=/var/lib/apt,sharing=locked \
#     uv python install 3.12 \
#     && uv venv --allow-existing --relocatable /venv \
#     && apt-get update \
#     && apt-get upgrade -y \
#     && apt-get install -y --no-install-recommends \
#         ca-certificates \
#         postgresql-client \
#         build-essential clang clang-11 gcc git gnupg libssl-dev llvm-11 lsb-release make pkg-config \
#     && apt-get remove -y apt-transport-https \
#     && apt-get clean && apt-get -y autoremove

# WORKDIR /opt/src/pypgstac

# RUN \
#     --mount=type=cache,target=/root/.cache/uv \
#     --mount=type=bind,source=./src/pypgstac/uv.lock,target=/opt/src/pypgstac/uv.lock \
#     --mount=type=bind,source=./src/pypgstac/pyproject.toml,target=/opt/src/pypgstac/pyproject.toml \
#     uv sync --frozen --no-install-project

# ADD ./src /opt/src
# WORKDIR /opt/src/pypgstac

# RUN \
#     --mount=type=cache,target=/root/.cache/uv \
#     uv pip install .[psycopg]


# # Image to use to run tests and migrations
# FROM debian:bullseye-slim AS pypgstac-dev
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
# ENV \
#     UV_LINK_MODE=copy \
#     UV_COMPILE_BYTECODE=1 \
#     UV_PROJECT_ENVIRONMENT=/venv \
#     UV_PYTHON_INSTALL_DIR=/venv/bin \
#     VIRTUAL_ENVIRONMENT=/venv \
#     PATH=/venv/bin:$PATH
# RUN --mount=type=cache,target=/root/.cache/uv \
#     --mount=type=cache,target=/var/cache/apt,sharing=locked \
#     --mount=type=cache,target=/var/lib/apt,sharing=locked \
#     uv python install 3.12 \
#     && uv venv --allow-existing --relocatable /venv \
#     && apt-get update \
#     && apt-get upgrade -y \
#     && apt-get install -y --no-install-recommends postgresql-client \
#     && apt-get remove -y apt-transport-https \
#     && apt-get clean && apt-get -y autoremove

# WORKDIR /opt/src/pypgstac

# RUN \
#     --mount=type=cache,target=/root/.cache/uv \
#     --mount=type=bind,source=./src/pypgstac/uv.lock,target=/opt/src/pypgstac/uv.lock \
#     --mount=type=bind,source=./src/pypgstac/pyproject.toml,target=/opt/src/pypgstac/pyproject.toml \
#     uv sync --frozen --no-install-project

# ADD ./src /opt/src
# WORKDIR /opt/src/pypgstac

# RUN \
#     --mount=type=cache,target=/root/.cache/uv \
#     uv pip install .[psycopg]

# COPY --from=pypgstacbuilder /venv /venv

# # Slimmed down image with pypgstac installed
# FROM debian:bullseye-slim AS pypgstac
# ENV \
#     VIRTUAL_ENVIRONMENT=/venv \
#     PATH=/venv/bin:$PATH
# COPY --from=pypgstacbuilder /venv /venv

FROM rust:1-slim-bookworm AS pyrustbase
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_BREAK_SYSTEM_PACKAGES=1
ENV PYTHONPATH="/opt/src/pypgstac:$PYTHONPATH"
ENV PATH="/opt/docker/pypgstac/bin:$PATH"
RUN \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl \
        postgresql-client \
        python3 python-is-python3 python3-venv \
        build-essential clang gcc git gnupg libssl-dev lsb-release make pkg-config \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

FROM pyrustbase AS pypgstac
COPY ./src/pypgstac/pyproject.toml /tmp/pyproject.toml
WORKDIR /tmp
RUN \
    uv pip compile --all-extras /tmp/pyproject.toml >/tmp/requirements.txt \
    && uv pip install --system -r /tmp/requirements.txt
COPY docker/pypgstac/bin /opt/docker/pypgstac/bin
COPY src/pypgstac /opt/src/pypgstac
COPY src/pgstac /opt/src/pgstac
WORKDIR /opt/src/pypgstac
RUN uv pip install --system -e . && rm -rf /usr/local/cargo/registry

RUN addgroup --gid 1000 user && \
    adduser --uid 1000 --gid 1000 --disabled-password --gecos "" --home /home/user user && \
    chown -R user:user /opt/src/pypgstac /opt/src/pgstac \
        /usr/local/lib/python3.11/dist-packages /usr/local/bin
USER user

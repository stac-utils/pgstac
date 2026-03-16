FROM rust:1-slim-bullseye AS pyrustbase
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore
ENV PYTHONPATH="/opt/src/pypgstac:$PYTHONPATH"
ENV PATH="/opt/docker/pypgstac/bin:$PATH"
ARG PG_MAJOR=17
RUN \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg lsb-release \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
       | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client-${PG_MAJOR} \
        python3 python-is-python3 python3-pip python3-venv \
        build-essential clang clang-11 gcc git libssl-dev llvm-11 make pkg-config \
    && python3 -m pip install --upgrade pip uv wheel setuptools \
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
    chown -R user:user /opt/src/pypgstac /opt/src/pgstac
USER user

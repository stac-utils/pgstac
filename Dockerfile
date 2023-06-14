FROM postgres:15-bullseye as pg
ENV PGSTACDOCKER=1
ENV POSTGIS_MAJOR 3
ENV POSTGIS_VERSION 3.3.3+dfsg-1~exp1.pgdg110+1
ENV PYTHONPATH=/opt/src/pypgstac:/opt/python:${PYTHONPATH}
ENV PATH=/var/lib/postgresql/.cargo/bin:/opt/bin:${PATH}
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1
ENV PLRUSTURL=https://github.com/tcdi/plrust/releases/download/v1.1.3/plrust-trusted-1.1.3_1.67.1-debian-pg15-amd64.deb

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR

RUN set -ex \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 python-is-python3 python3-pip \
        postgresql-server-dev-$PG_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-partman \
        postgresql-$PG_MAJOR-plpgsql-check \
        wget \
        build-essential clang clang-11 gcc git gnupg libssl-dev llvm-11 lsb-release make pkg-config

USER postgres
RUN \
    wget -qO- https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain=1.67.1 \
    && rustup toolchain install 1.67.1 \
    && rustup default 1.67.1 \
    && rustup component add rustc-dev

USER root
RUN wget -qO plrust.deb ${PLRUSTURL} \
    && dpkg -i plrust.deb \
    && rm plrust.deb \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/src/pypgstac/pypgstac \
    && touch /opt/src/pypgstac/pypgstac/__init__.py \
    && touch /opt/src/pypgstac/README.md \
    && echo '__version__ = "0.0.0"' > /opt/src/pypgstac/pypgstac/version.py

COPY ./src/pypgstac/pyproject.toml /opt/src/pypgstac/pyproject.toml

RUN cat /opt/src/pypgstac/pypgstac/version.py && \
    pip3 install --upgrade pip \
    && pip3 install /opt/src/pypgstac[dev,test,psycopg]

COPY ./src /opt/src
COPY ./scripts/bin /opt/bin

RUN \
    echo "initpgstac" > /docker-entrypoint-initdb.d/999_initpgstac.sh \
    && chmod +x /docker-entrypoint-initdb.d/* \
    && chmod +x /opt/bin/*

WORKDIR /opt/src

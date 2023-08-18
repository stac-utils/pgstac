ARG PG_MAJOR=15
ARG POSTGIS_MAJOR=3

FROM postgres:${PG_MAJOR}-bullseye as pgstacbase
ARG POSTGIS_MAJOR
RUN  \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-plpgsql-check \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

FROM pgstacbase as pgstacbase-plrust
ENV PLRUSTVERSION=1.2.3
ENV RUSTVERSION=1.70.0
ENV PLRUSTDOWNLOADURL=https://github.com/tcdi/plrust/releases/download/
ENV PLRUSTFILE=plrust-trusted-${PLRUSTVERSION}_${RUSTVERSION}-debian-pg${PG_MAJOR}-amd64.deb
ENV PLRUSTURL=${PLRUSTDOWNLOADURL}v${PLRUSTVERSION}/${PLRUSTFILE}
ADD $PLRUSTURL .
ENV PATH=/home/postgres/.cargo/bin:$PATH
RUN  \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        ca-certificates \
        clang \
        clang-11 \
        gcc \
        git \
        gnupg \
        libssl-dev \
        llvm-11 \
        lsb-release \
        make \
        pkg-config \
        wget \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*
USER postgres
RUN \
    wget -qO- https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain=${RUSTVERSION} \
    && $HOME/.cargo/bin/rustup toolchain install ${RUSTVERSION} \
    && $HOME/.cargo/bin/rustup default ${RUSTVERSION} \
    && $HOME/.cargo/bin/rustup component add rustc-dev

WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac-rust.sh 991_plrust.sh

USER root
RUN apt-get install -y /${PLRUSTFILE}

FROM pgstacbase as pgstac
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac.sh 990_pgstac.sh
COPY src/pgstac/pgstac.sql 999_pgstac.sql

FROM pgstacbase-plrust as pgstac-plrust
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac.sh 990_pgstac.sh
COPY src/pgstac/pgstac.sql 999_pgstac.sql

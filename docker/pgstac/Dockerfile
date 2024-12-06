ARG PG_MAJOR=17
ARG POSTGIS_MAJOR=3

# Base postgres image that pgstac can be installed onto
FROM postgres:${PG_MAJOR}-bullseye AS pgstacbase
ARG POSTGIS_MAJOR
ARG PG_MAJOR
RUN  \
    apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-plpgsql-check \
        postgresql-$PG_MAJOR-partman \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*
COPY docker/pgstac/dbinit/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Base postgres image with plrust installed that can be used for future development using plrust
FROM pgstacbase AS pgstacbase-plrust
ENV PLRUSTVERSION=1.2.7
ENV RUSTVERSION=1.72.0
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
WORKDIR /docker-entrypoint-preinitdb.d
COPY docker/pgstac/dbinit/pgstac-rust-preinit.sh preloadplrust.sh
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac-rust.sh 991_plrust.sh

USER root
RUN apt-get install -y /${PLRUSTFILE}

# The pgstacbase image with latest version of pgstac installed
FROM pgstacbase AS pgstac
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac.sh 990_pgstac.sh
COPY src/pgstac/pgstac.sql 999_pgstac.sql

# The pgstacbase-plrust image with the latest version of pgstac installed
FROM pgstacbase-plrust AS pgstac-plrust
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac.sh 990_pgstac.sh
COPY src/pgstac/pgstac.sql 999_pgstac.sql

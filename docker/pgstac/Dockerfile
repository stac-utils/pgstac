# syntax=docker/dockerfile:1.7
ARG PG_MAJOR=17
ARG POSTGIS_MAJOR=3
ARG PG_TLE_VERSION=1.5.2
ARG DEBIAN_SUITE=trixie

# Base postgres image that pgstac can be installed onto
FROM postgres:${PG_MAJOR}-${DEBIAN_SUITE} AS pgstacbase
ARG POSTGIS_MAJOR
ARG PG_MAJOR
ARG PG_TLE_VERSION
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,target=/root/.cache/git,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-plpgsql-check \
        postgresql-$PG_MAJOR-partman \
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        ca-certificates \
        curl \
        git \
        flex \
        bison \
        libkrb5-dev \
    && GIT_TERMINAL_PROMPT=0 git clone --branch v${PG_TLE_VERSION} --depth 1 https://github.com/aws/pg_tle.git /tmp/pg_tle \
    && make -C /tmp/pg_tle \
    && make -C /tmp/pg_tle install \
    && rm -rf /tmp/pg_tle \
    && sed -i "s/^#shared_preload_libraries = .*/shared_preload_libraries = 'pg_tle'/" /usr/share/postgresql/$PG_MAJOR/postgresql.conf.sample \
    && sed -i "s/^#shared_preload_libraries = .*/shared_preload_libraries = 'pg_tle'/" /usr/share/postgresql/postgresql.conf.sample \
    && apt-get purge -y --auto-remove \
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        curl \
        git \
        flex \
        bison \
        libkrb5-dev \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

# The pgstacbase image with latest version of pgstac installed
FROM pgstacbase AS pgstac
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac.sh 990_pgstac.sh
COPY src/pgstac/pgstac.sql 999_pgstac.sql

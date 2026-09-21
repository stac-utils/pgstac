ARG PG_MAJOR=17
ARG POSTGIS_MAJOR=3

# Base postgres image that pgstac can be installed onto
FROM postgres:${PG_MAJOR}-trixie AS pgstacbase
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

# The pgstacbase image with latest version of pgstac installed
FROM pgstacbase AS pgstac
WORKDIR /docker-entrypoint-initdb.d
COPY docker/pgstac/dbinit/pgstac.sh 990_pgstac.sh
COPY src/pgstac/pgstac.sql 999_pgstac.sql

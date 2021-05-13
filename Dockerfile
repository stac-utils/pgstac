FROM postgres:13 as pg

LABEL maintainer="David Bitner"

ENV POSTGIS_MAJOR 3
ENV PGUSER postgres
ENV PGDATABASE postgres
ENV PGHOST localhost

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        # curl \
        gnupg \
        apt-transport-https \
        debian-archive-keyring \
        # jq \
        software-properties-common \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-partman \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        build-essential \
        python3 \
        python3-pip \
        python3-setuptools \
        # git \
    && pip3 install -U pip setuptools packaging migra[pg] \
    && apt-get remove -y apt-transport-https software-properties-common build-essential python3-pip python3-setuptools \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 5432

RUN mkdir -p /docker-entrypoint-initdb.d
# COPY ./docker/initpgstac.sh /docker-entrypoint-initdb.d/initpgstac.sh
# COPY ./pgstac.sql /workspaces/pgstac.sql
COPY ./sql /docker-entrypoint-initdb.d/

WORKDIR /workspaces

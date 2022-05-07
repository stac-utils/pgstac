FROM postgres:13 as pg

LABEL maintainer="David Bitner"

ENV POSTGIS_MAJOR 3
ENV PGUSER postgres
ENV PGDATABASE postgres
ENV PGHOST localhost
ENV \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        gnupg \
        apt-transport-https \
        debian-archive-keyring \
        software-properties-common \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-partman \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        build-essential \
        python3 \
        python3-pip \
        python3-setuptools \
    && pip3 install -U pip setuptools packaging \
    && pip3 install -U psycopg2-binary \
    && pip3 install -U psycopg[binary] \
    && pip3 install -U migra[pg] \
    && apt-get remove -y apt-transport-https \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 5432

RUN mkdir -p /docker-entrypoint-initdb.d
RUN echo "#!/bin/bash \n unset PGHOST \n pypgstac migrate" >/docker-entrypoint-initdb.d/initpgstac.sh && chmod +x /docker-entrypoint-initdb.d/initpgstac.sh

RUN mkdir -p /opt/src/pypgstac

WORKDIR /opt/src/pypgstac

COPY pypgstac /opt/src/pypgstac

RUN pip3 install -e /opt/src/pypgstac[psycopg]

ENV PYTHONPATH=/opt/src/pypgstac:${PYTHONPATH}

WORKDIR /opt/src

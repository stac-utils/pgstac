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
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

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
    && pip3 install -U migra[pg] \
    && pip3 install poetry==1.1.7 \
    && apt-get remove -y apt-transport-https \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 5432

RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./sql /docker-entrypoint-initdb.d/

RUN mkdir -p /opt/src/pypgstac

WORKDIR /opt/src/pypgstac

COPY pypgstac/poetry.lock pypgstac/pyproject.toml ./
RUN poetry install


COPY pypgstac /opt/src/pypgstac
RUN poetry install

ENV PYTHONPATH=/opt/src/pypgstac:${PYTHONPATH}

WORKDIR /opt/src

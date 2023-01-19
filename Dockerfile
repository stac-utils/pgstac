ARG POSTGRES_VERSION=14

FROM postgres:${POSTGRES_VERSION} as pg

LABEL maintainer="David Bitner"

ENV POSTGIS_MAJOR 3
ENV PYTHONPATH=/opt/src/pypgstac:${PYTHONPATH}
ENV PATH=/opt/src/pgstac/scripts:${PATH}

ENV POSTGRES_USER username
ENV POSTGRES_DB postgis
ENV POSTGRES_PASSWORD password

ENV PGUSER=${POSTGRES_USER}
ENV PGDATABASE=${POSTGRES_DB}
ENV PGPASSWORD=${POSTGRES_PASSWORD}

ENV POSTGRES_VERSION=${POSTGRES_VERSION}

ENV PGISDOCKER=1

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        gnupg \
        apt-transport-https \
        debian-archive-keyring \
        software-properties-common \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        python3 \
        python3-pip \
        python-is-python3 \
    && apt-get remove -y apt-transport-https \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*


# EXPOSE 5432

RUN pip install --upgrade pip && \
    pip install --upgrade psycopg[binary] psycopg-pool

COPY ./src /opt/src

RUN pip3 install -e /opt/src/pypgstac[dev,test]


RUN echo "initpgstac" > /docker-entrypoint-initdb.d/999_initpgstac.sh
RUN chmod +x /docker-entrypoint-initdb.d/999_initpgstac.sh

WORKDIR /opt/src

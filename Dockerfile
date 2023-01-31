FROM postgres:15-bullseye as pg
ENV PGSTACDOCKER=1
ENV POSTGIS_MAJOR 3
ENV POSTGIS_VERSION 3.3.2+dfsg-1.pgdg110+1

ENV PYTHONPATH=/opt/src/pypgstac:${PYTHONPATH}
ENV PATH=/opt/bin:${PATH}
ENV PYTHONWRITEBYTECODE=1
ENV PYTHONBUFFERED=1

RUN set -ex \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gnupg \
        python3 python3-pip python-is-python3 \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        postgresql-$PG_MAJOR-pgtap \
        postgresql-$PG_MAJOR-plpgsql-check \
        postgresql-$PG_MAJOR-plprofiler \
        postgresql-$PG_MAJOR-pldebugger \
        postgresql-$PG_MAJOR-partman \
    && apt-get remove -y apt-transport-https \
    && apt-get clean && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip \
    && pip3 install --upgrade migra

COPY ./src/pypgstac/pyproject.toml /opt/src/pypgstac/pyproject.toml
COPY ./src/pypgstac/setup.py /opt/src/pypgstac/setup.py
COPY ./src/pypgstac/README.md /opt/src/pypgstac/README.md
COPY ./src/pypgstac/pypgstac/__init__.py /opt/src/pypgstac/pypgstac/__init__.py
COPY ./src/pypgstac/pypgstac/version.py /opt/src/pypgstac/pypgstac/version.py

RUN pip3 install -e /opt/src/pypgstac[dev,test,psycopg]

COPY ./src /opt/src
COPY ./scripts/bin /opt/bin

RUN echo "initpgstac" > /docker-entrypoint-initdb.d/999_initpgstac.sh
RUN chmod +x /docker-entrypoint-initdb.d/999_initpgstac.sh
RUN chmod +x /opt/bin/*

WORKDIR /opt/src

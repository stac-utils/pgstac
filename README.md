
<p align="center">
  <img src="https://user-images.githubusercontent.com/10407788/174893876-7a3b5b7a-95a5-48c4-9ff2-cc408f1b6af9.png"/>
  <p align="center">PostgreSQL schema and functions for Spatio-Temporal Asset Catalog (STAC)</p>
</p>

<p align="center">
  <a href="https://github.com/stac-utils/pgstac/actions?query=workflow%3ACI" target="_blank">
      <img src="https://github.com/stac-utils/pgstac/workflows/CI/badge.svg" alt="Test">
  </a>
  <a href="https://pypi.org/project/pypgstac" target="_blank">
      <img src="https://img.shields.io/pypi/v/pypgstac?color=%2334D058&label=pypi%20package" alt="Package version">
  </a>
  <a href="https://github.com/stac-utils/pgstac/blob/master/LICENSE" target="_blank">
      <img src="https://img.shields.io/github/license/stac-utils/pgstac.svg" alt="License">
  </a>
</p>

---

**Documentation**: <a href="https://stac-utils.github.io/pgstac/" target="_blank">https://stac-utils.github.io/pgstac/</a>

**Source Code**: <a href="https://github.com/stac-utils/pgstac" target="_blank">https://github.com/stac-utils/pgstac</a>

---

**PgSTAC** is a set of SQL function and schema to build highly performant database for Spatio-Temporal Asset Catalog ([STAC](https://stacspec.org)). The project also provide **pypgstac** python module to help with the database migration and documents ingestion (collections and items).

PgSTAC provides functionality for STAC Filters and CQL2 search along with utilities to help manage indexing and partitioning of STAC Collections and Items.

PgSTAC is used in production to scale to hundreds of millions of STAC items. PgSTAC implements core data models and functions to provide a STAC API from a PostgreSQL database. As PgSTAC is fully within the database, it does not provide an HTTP facing API. The (Stac FastAPI)[https://github.com/stac-utils/stac-fastapi] PgSTAC backend and (Franklin)[https://github.com/azavea/franklin] can be used to expose a PgSTAC catalog. It is also possible to integrate PgSTAC with any other language that has PostgreSQL drivers.

PgSTAC Documentation: https://stac-utils.github.io/pgstac/pgstac

pyPgSTAC Documentation: https://stac-utils.github.io/pgstac/pypgstac



## Project structure

```
/
 ├── src/pypgstac           - pyPgSTAC python module
 ├── src/pypgstac/tests/    - pyPgSTAC tests
 ├── scripts/               - scripts to set up the environment, create migrations, and run tests
 ├── src/pgstac/sql/        - PgSTAC SQL code
 ├── src/pgstac/migrations/ - Migrations for incremental upgrades
 └── src/pgstac/tests/      - test suite
```

## Contribution & Development

See [CONTRIBUTING.md](https://github.com//stac-utils/pgstac/blob/master/CONTRIBUTING.md)

## License

See [LICENSE](https://github.com//stac-utils/pgstac/blob/master/LICENSE)

## Authors

See [contributors](https://github.com/stac-utils/pgstac/graphs/contributors) for a listing of individual contributors.

## Changes

See [CHANGELOG.md](https://github.com/stac-utils/pgstac/blob/master/CHANGELOG.md).

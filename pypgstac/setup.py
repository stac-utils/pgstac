# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['pypgstac']

package_data = \
{'': ['*'],
 'pypgstac': ['migrations/pgstac.0.2.4-0.2.7.sql',
              'migrations/pgstac.0.2.4-0.2.7.sql',
              'migrations/pgstac.0.2.4-0.2.7.sql',
              'migrations/pgstac.0.2.5-0.2.7.sql',
              'migrations/pgstac.0.2.5-0.2.7.sql',
              'migrations/pgstac.0.2.5-0.2.7.sql',
              'migrations/pgstac.0.2.7.sql',
              'migrations/pgstac.0.2.7.sql',
              'migrations/pgstac.0.2.7.sql']}

install_requires = \
['asyncio>=3.4.3,<4.0.0',
 'asyncpg>=0.22.0,<0.23.0',
 'orjson>=3.5.2,<4.0.0',
 'smart-open>=4.2.0,<5.0.0',
 'typer>=0.3.2,<0.4.0']

entry_points = \
{'console_scripts': ['pypgstac = pypgstac.pypgstac:app']}

setup_kwargs = {
    'name': 'pypgstac',
    'version': '0.2.7',
    'description': '',
    'long_description': 'Python tools for working with PGStac\n',
    'author': 'David Bitner',
    'author_email': 'bitner@dbspatial.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/stac-utils/pgstac',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'entry_points': entry_points,
    'python_requires': '>=3.8,<4.0',
}


setup(**setup_kwargs)

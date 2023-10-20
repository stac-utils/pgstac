# IPython log file

get_ipython().run_line_magic('config', 'Application.log_level="INFO"')
from pypgstac.db import PgstacDB
db = PgstacDB(debug=True)
db.search()
get_ipython().run_line_magic('config', 'Application.log_level="INFO"')
from pypgstac.db import PgstacDB
db = PgstacDB(debug=True)
db.search()
get_ipython().run_line_magic('config', 'Application.log_level="INFO"')
from pypgstac.db import PgstacDB
with PgstacDB(debug=True) as db:
    db.search()
get_ipython().run_line_magic('config', 'Application.log_level="INFO"')
from pypgstac.db import PgstacDB
with PgstacDB(debug=True) as db:
    print(db.search())
get_ipython().run_cell_magic('script', 'psql', 'select true;\n')
get_ipython().run_cell_magic('script', 'psql -H', 'select true;\n')
get_ipython().run_cell_magic('script', 'psql', 'select true;\n')
get_ipython().run_cell_magic('script', 'psql -H', 'select true;\n')
get_ipython().run_cell_magic('script', 'psql -H', '\\d items\n')
get_ipython().run_cell_magic('script', 'psql -H', '\\d items\n\\d collections\n')
get_ipython().run_cell_magic('script', 'psql', '\\d items\n\\d collections\n')
get_ipython().run_cell_magic('script', 'psql', 'set client_min_messages to notice;\nselect search();\n')
query=orjson.dumps({"limit":1})
get_ipython().run_line_magic('%script', 'psql')
set client_min_messages to notice;
select search('{query}');
get_ipython().run_line_magic('logstart', '')
from pypgstac.db import PgstacDB
with PgstacDB(debug=True) as db:
    print(db.search())
from pypgstac.db import PgstacDB
with PgstacDB(debug=True) as db:
    print(db.search())
get_ipython().run_line_magic('config', 'Application.log_level="INFO"')
from pypgstac.db import PgstacDB
with PgstacDB(debug=True) as db:
    print(db.search())
from pypgstac.db import PgstacDB
with PgstacDB(debug=True) as db:
    print(db.search())

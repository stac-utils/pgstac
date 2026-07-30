"""Concurrent read/write behaviour.

Covers both ingest paths: the SQL staging tables, and pypgstac's Loader, which
is the primary way data is loaded and takes a different route entirely --
check_partition, then a COPY (straight into the partition for insert, into a
temp table under LOCK TABLE ... EXCLUSIVE for the other modes), then a stats
update.

Deadlocks are asserted on the delta of pg_stat_database.deadlocks rather than on
a raised exception: Loader.load_partition retries DeadlockDetected up to ten
times, so an exception based assertion passes even when the server is breaking
deadlocks on every attempt.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg
from psycopg import sql

from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods

MONTHS = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06"]


def deadlocks(db: PgstacDB) -> int:
    """Deadlocks the server has broken on this database so far."""
    row = db.query_one(
        "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database();",
    )
    assert isinstance(row, int)
    return row


def item_content(
    collection: str,
    ident: str,
    month: str,
    revision: int = 0,
) -> str:
    """A minimal STAC item as a jsonb literal.

    revision changes the stored content, which the upsert path needs in order
    to consider a row worth replacing.
    """
    return (
        '{"id": "%s", "collection": "%s", "type": "Feature",'
        ' "geometry": {"type": "Point", "coordinates": [0, 0]},'
        ' "properties": {"datetime": "%s-05T00:00:00Z", "gsd": %d}}'
        % (ident, collection, month, revision)
    )


def worker(fn: Callable[[psycopg.Connection], Any]) -> Any:
    """Run fn on its own connection. psycopg connections are not thread safe."""
    with psycopg.connect(autocommit=True) as conn:
        conn.execute("SET search_path TO pgstac, public;")
        conn.execute("SET ROLE pgstac_ingest;")
        conn.execute("SET pgstac.use_queue TO FALSE;")
        return fn(conn)


def run_concurrently(fns: List[Callable[[psycopg.Connection], Any]]) -> List[Any]:
    """Run each callable on its own connection, all at once."""
    with ThreadPoolExecutor(max_workers=len(fns)) as pool:
        futures = [pool.submit(worker, fn) for fn in fns]
        return [f.result() for f in futures]


def make_collection(db: PgstacDB, collection: str, trunc: Optional[str]) -> None:
    """Create a collection, optionally partitioned by datetime."""
    db.query_one(
        """
        INSERT INTO collections (content, partition_trunc)
        VALUES (jsonb_build_object('id', %s::text), %s::text)
        RETURNING id;
        """,
        [collection, trunc],
    )


def stage(conn: psycopg.Connection, table: str, contents: List[str]) -> None:
    """Insert a batch into a staging table in a single statement."""
    conn.execute(
        sql.SQL("INSERT INTO {} (content) VALUES {}").format(
            sql.Identifier(table),
            sql.SQL(",").join([sql.SQL("(%s::jsonb)")] * len(contents)),
        ),
        contents,
    )


def test_concurrent_multi_partition_ingest_no_deadlock(db: PgstacDB) -> None:
    """Statements spanning the same partitions in different orders.

    Each statement's after-trigger takes a partition_stats row lock per
    partition touched and holds it until commit, so every writer has to acquire
    them in the same order.
    """
    collection = "conc-multi"
    make_collection(db, collection, "month")
    before = deadlocks(db)

    orders = [MONTHS, list(reversed(MONTHS)), MONTHS[3:] + MONTHS[:3]]
    fns: List[Callable[[psycopg.Connection], Any]] = [
        lambda conn, order=order, i=i: stage(
            conn,
            "items_staging",
            [item_content(collection, f"w{i}-{m}", m) for m in order],
        )
        for i, order in enumerate(orders)
    ]
    run_concurrently(fns)

    assert deadlocks(db) == before
    count = db.query_one(
        "SELECT count(*) FROM items WHERE collection = %s;",
        [collection],
    )
    assert count == len(orders) * len(MONTHS)


def test_concurrent_same_id_upsert_no_deadlock(db: PgstacDB) -> None:
    """Two upserts over the same ids in opposite order.

    The upsert path deletes the rows it is about to replace. Without a fixed
    lock order the two statements take the same item row locks in opposite
    order and deadlock. The revisions differ from what is stored, otherwise the
    IS DISTINCT FROM filter would skip every row and lock nothing.
    """
    collection = "conc-upsert"
    make_collection(db, collection, None)
    ids = [f"i{n:03d}" for n in range(40)]

    run_concurrently(
        [
            lambda conn: stage(
                conn,
                "items_staging",
                [item_content(collection, i, "2020-01") for i in ids],
            ),
        ],
    )
    before = deadlocks(db)

    forward = [item_content(collection, i, "2020-01", 1) for i in ids]
    backward = list(reversed([item_content(collection, i, "2020-01", 2) for i in ids]))
    run_concurrently(
        [
            lambda conn: stage(conn, "items_staging_upsert", forward),
            lambda conn: stage(conn, "items_staging_upsert", backward),
        ],
    )

    assert deadlocks(db) == before
    count = db.query_one(
        "SELECT count(*) FROM items WHERE collection = %s;",
        [collection],
    )
    assert count == len(ids)


def test_collection_delete_vs_concurrent_partition_create(db: PgstacDB) -> None:
    """Dropping a collection while another session ingests into it.

    Unlike the other tests here this one does not assert zero deadlocks. The
    two operations genuinely conflict: delete_collection takes the collections
    row and then the partition tables, while ingest takes the partition tables
    and then needs KEY SHARE on that same collections row through
    items_collections_fk. Postgres breaking that cycle by aborting one side is
    a correct outcome, and it is observed intermittently.

    What must hold is that it resolves safely: both sessions finish, and the
    database is left consistent either way.
    """
    collection = "conc-delete"
    make_collection(db, collection, "month")
    run_concurrently(
        [
            lambda conn: stage(
                conn,
                "items_staging",
                [item_content(collection, f"seed-{m}", m) for m in MONTHS[:2]],
            ),
        ],
    )

    def ingest(conn: psycopg.Connection) -> Optional[str]:
        try:
            stage(
                conn,
                "items_staging",
                [item_content(collection, f"late-{m}", m) for m in MONTHS[2:]],
            )
        except psycopg.Error as e:
            return type(e).__name__
        return None

    def drop(conn: psycopg.Connection) -> Optional[str]:
        try:
            conn.execute("SELECT delete_collection(%s);", [collection])
        except psycopg.Error as e:
            return type(e).__name__
        return None

    # Neither side may hang; run_concurrently only returns once both have.
    results = run_concurrently([ingest, drop])
    assert len(results) == 2

    survived = db.query_one(
        "SELECT count(*) FROM collections WHERE id = %s;",
        [collection],
    )
    orphans = db.query_one(
        """
        SELECT count(*) FROM partition_stats ps
        WHERE NOT EXISTS (
            SELECT 1 FROM partitions_view pv WHERE pv.partition = ps.partition
        );
        """,
    )
    assert orphans == 0, "partition_stats rows left behind for dropped partitions"

    if survived == 0:
        # Delete won: nothing of the collection may remain.
        assert (
            db.query_one(
                "SELECT count(*) FROM partition_stats WHERE collection = %s;",
                [collection],
            )
            == 0
        )
        assert (
            db.query_one(
                "SELECT count(*) FROM items WHERE collection = %s;",
                [collection],
            )
            == 0
        )
    else:
        # Ingest won: every surviving partition is still visible to search.
        assert (
            db.query_one(
                """
            SELECT count(*) FROM partitions_view pv
            WHERE pv.collection = %s AND NOT EXISTS (
                SELECT 1 FROM partition_stats ps
                WHERE ps.partition = pv.partition AND ps.collection IS NOT NULL
            );
            """,
                [collection],
            )
            == 0
        )


def test_search_sees_all_committed_items_during_partition_creation(
    db: PgstacDB,
) -> None:
    """Search finds partitions through partition_stats.

    A partition whose stats row is missing drops out of the datetime bands and
    its items disappear from results without an error, so a reader must never
    see fewer items than were committed before it started.
    """
    collection = "conc-visibility"
    make_collection(db, collection, "month")

    for committed, month in enumerate(MONTHS, start=1):
        run_concurrently(
            [
                lambda conn, month=month: stage(
                    conn,
                    "items_staging",
                    [item_content(collection, f"v-{month}", month)],
                ),
            ],
        )
        found = db.query_one(
            """
            SELECT jsonb_array_length(
                search(jsonb_build_object(
                    'collections', jsonb_build_array(%s::text), 'limit', 100
                )) -> 'features'
            );
            """,
            [collection],
        )
        assert found == committed, (
            f"search returned {found} of {committed} committed items after "
            f"creating the {month} partition"
        )


def test_fresh_cache_hit_does_not_block_on_locked_stats_row(db: PgstacDB) -> None:
    """A search whose cached counts are current must not wait on that row.

    A fresh hit only bumps usage counters; taking FOR UPDATE for that would
    serialize identical searches. Holding the row locked in another transaction
    makes the outcome deterministic: the reader either skips the lock or waits
    until statement_timeout fires.
    """
    collection = "conc-search"
    make_collection(db, collection, "month")
    run_concurrently(
        [
            lambda conn: stage(
                conn,
                "items_staging",
                [item_content(collection, f"s-{m}", m) for m in MONTHS],
            ),
        ],
    )

    search = sql.SQL(
        "SELECT search(jsonb_build_object("
        "'collections', jsonb_build_array({}), 'limit', 5));",
    ).format(sql.Literal(collection))

    # Populate search_wheres so there is a fresh row to hit.
    run_concurrently(
        [
            lambda conn: (
                conn.execute("SET pgstac.context TO 'on';"),
                conn.execute(search),
                None,
            )[-1],
        ],
    )
    cached = db.query_one(
        "SELECT _where FROM search_wheres WHERE total_count IS NOT NULL LIMIT 1;",
    )
    assert isinstance(cached, str), "where_stats cached no counts to hit"

    holder = psycopg.connect(autocommit=False)
    try:
        holder.execute("SET search_path TO pgstac, public;")
        holder.execute("SELECT * FROM search_wheres FOR UPDATE;")

        def read(conn: psycopg.Connection) -> None:
            conn.execute("SET pgstac.context TO 'on';")
            conn.execute("SET statement_timeout TO '10s';")
            conn.execute("SELECT where_stats(%s);", [cached])

        try:
            run_concurrently([read])
        except psycopg.errors.QueryCanceled:
            raise AssertionError(
                "where_stats blocked on a locked search_wheres row; a fresh "
                "cache hit must not take a row lock",
            ) from None
    finally:
        holder.rollback()
        holder.close()


def constraint_ranges(db: PgstacDB, partition: str) -> Tuple[str, str]:
    """The CHECK constraint ranges the catalog reports for a partition."""
    row = db.query_one(
        """
        SELECT constraint_dtrange::text, constraint_edtrange::text
        FROM partitions_view WHERE partition = %s;
        """,
        [partition],
    )
    assert isinstance(row, tuple)
    return row[0], row[1]


def test_check_constraints_survive_widening(db: PgstacDB) -> None:
    """Widening a partition must not leave it without CHECK constraints.

    check_partition drops them when the incoming batch falls outside the
    current bounds and queues the rebuild. The staging path has nothing else
    that would put them back.
    """
    collection = "conc-widen"
    make_collection(db, collection, None)

    run_concurrently(
        [
            lambda conn: stage(
                conn,
                "items_staging",
                [item_content(collection, "narrow", "2020-06")],
            ),
        ],
    )
    partition = db.query_one(
        "SELECT partition FROM partition_stats WHERE collection = %s;",
        [collection],
    )
    assert isinstance(partition, str)

    # Outside the current bounds in both directions, forcing a widen.
    run_concurrently(
        [
            lambda conn: stage(
                conn,
                "items_staging",
                [
                    item_content(collection, "early", "2019-01"),
                    item_content(collection, "late", "2021-12"),
                ],
            ),
        ],
    )

    validated = db.query_one(
        """
        SELECT count(*) FROM pg_constraint
        WHERE conrelid = format('pgstac.%%I', %s::text)::regclass
          AND contype = 'c' AND convalidated;
        """,
        [partition],
    )
    assert validated == 1, (
        f"{partition} has {validated} validated CHECK constraints after "
        "widening; NOT VALID constraints do not prune"
    )

    dtrange, edtrange = constraint_ranges(db, partition)
    assert "2019-01" in dtrange and "2021-12" in dtrange, dtrange
    assert "infinity" not in edtrange, (
        f"end_datetime constraint is unbounded ({edtrange}); "
        "end_datetime predicates cannot prune this partition"
    )


# --------------------------------------------------------------------------
# Loader path: check_partition, a COPY whose shape depends on the insert mode,
# then update_partition_stats_q. No staging tables involved.
# --------------------------------------------------------------------------


def loader_item(collection: str, ident: str, dt: datetime) -> Dict[str, Any]:
    """A minimal STAC item for the Loader."""
    return {
        "id": ident,
        "type": "Feature",
        "collection": collection,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-85.31, 30.93],
                    [-85.31, 31.00],
                    [-85.38, 31.00],
                    [-85.38, 30.93],
                    [-85.31, 30.93],
                ],
            ],
        },
        "bbox": [-85.38, 30.93, -85.31, 31.00],
        "links": [],
        "assets": {},
        "properties": {"datetime": dt.isoformat()},
        "stac_version": "1.0.0",
        "stac_extensions": [],
    }


def load(
    items: List[Dict[str, Any]],
    insert_mode: Methods = Methods.insert,
) -> Optional[str]:
    """Load with a Loader on its own connection, as a separate process would.

    The connection is closed on the way out; leaking it leaves the backend idle
    in transaction, which blocks the fixture's DROP DATABASE at teardown.
    """
    try:
        with PgstacDB() as pgdb:
            Loader(pgdb).load_items(iter(items), insert_mode=insert_mode)
    except Exception as e:  # noqa: BLE001 - the exception type is the assertion
        return type(e).__name__
    return None


def run_loaders(batches: List[Callable[[], Any]]) -> List[Any]:
    """Run each loader callable concurrently, one connection each."""
    with ThreadPoolExecutor(max_workers=len(batches)) as pool:
        return [f.result() for f in [pool.submit(b) for b in batches]]


def month_dt(month: str, day: int = 5) -> datetime:
    """A datetime inside the given YYYY-MM."""
    year, mon = month.split("-")
    return datetime(int(year), int(mon), day, tzinfo=timezone.utc)


def test_loader_concurrent_multi_partition_no_deadlock(db: PgstacDB) -> None:
    """Concurrent Loaders converging on the same partitions.

    load_items sorts each chunk by partition, so the loaders walk partitions in
    the same order and the contention is per partition rather than a cross
    partition ordering problem: for each one they run update_partition_stats
    concurrently, which reads the partition and then rebuilds its constraints.
    The input orders are varied only so the batches differ.
    """
    collection = "loader-multi"
    make_collection(db, collection, "month")
    before = deadlocks(db)

    orders = [MONTHS, list(reversed(MONTHS)), MONTHS[2:] + MONTHS[:2]]
    batches = [
        (
            lambda order=order, i=i: load(
                [loader_item(collection, f"L{i}-{m}", month_dt(m)) for m in order],
            )
        )
        for i, order in enumerate(orders)
    ]
    errors = [e for e in run_loaders(batches) if e]

    assert errors == [], f"concurrent loads raised: {errors}"
    assert deadlocks(db) == before
    count = db.query_one(
        "SELECT count(*) FROM items WHERE collection = %s;",
        [collection],
    )
    assert count == len(orders) * len(MONTHS)


def test_loader_concurrent_upsert_same_ids_no_deadlock(db: PgstacDB) -> None:
    """Concurrent Loaders upserting the same ids from opposite ends.

    The loader's upsert takes LOCK TABLE ... EXCLUSIVE on the partition before
    writing, so the two serialize rather than interleave; this asserts that
    holds and that no rows are lost or duplicated.
    """
    collection = "loader-upsert"
    make_collection(db, collection, None)
    dt = month_dt("2020-03")
    ids = [f"u{n:03d}" for n in range(50)]

    assert load([loader_item(collection, i, dt) for i in ids]) is None
    before = deadlocks(db)

    forward = [loader_item(collection, i, dt) for i in ids]
    backward = list(reversed([loader_item(collection, i, dt) for i in ids]))
    errors = [
        e
        for e in run_loaders(
            [
                lambda: load(forward, Methods.upsert),
                lambda: load(backward, Methods.upsert),
            ],
        )
        if e
    ]

    assert errors == [], f"concurrent upserts raised: {errors}"
    assert deadlocks(db) == before
    count = db.query_one(
        "SELECT count(*) FROM items WHERE collection = %s;",
        [collection],
    )
    assert count == len(ids)


def test_loader_concurrent_delsert_same_ids_no_deadlock(db: PgstacDB) -> None:
    """delsert deletes the rows it is about to rewrite, then inserts.

    That DELETE is unordered, so it is the loader's equivalent of the staging
    upsert path and worth asserting on directly.
    """
    collection = "loader-delsert"
    make_collection(db, collection, None)
    dt = month_dt("2020-04")
    ids = [f"d{n:03d}" for n in range(50)]

    assert load([loader_item(collection, i, dt) for i in ids]) is None
    before = deadlocks(db)

    forward = [loader_item(collection, i, dt) for i in ids]
    backward = list(reversed([loader_item(collection, i, dt) for i in ids]))
    errors = [
        e
        for e in run_loaders(
            [
                lambda: load(forward, Methods.delsert),
                lambda: load(backward, Methods.delsert),
            ],
        )
        if e
    ]

    assert errors == [], f"concurrent delserts raised: {errors}"
    assert deadlocks(db) == before
    count = db.query_one(
        "SELECT count(*) FROM items WHERE collection = %s;",
        [collection],
    )
    assert count == len(ids)


def test_loader_widening_partition_keeps_check_constraints(db: PgstacDB) -> None:
    """The loader path must also leave validated constraints behind.

    check_partition drops them when a batch falls outside the current bounds.
    NOT VALID constraints do not prune, so what matters is that a validated one
    exists afterwards and covers the data.
    """
    collection = "loader-widen"
    make_collection(db, collection, None)

    assert load([loader_item(collection, "mid", month_dt("2020-06"))]) is None
    partition = db.query_one(
        "SELECT partition FROM partition_stats WHERE collection = %s;",
        [collection],
    )
    assert isinstance(partition, str)

    assert (
        load(
            [
                loader_item(collection, "early", month_dt("2019-01")),
                loader_item(collection, "late", month_dt("2021-12")),
            ],
        )
        is None
    )

    validated = db.query_one(
        """
        SELECT count(*) FROM pg_constraint
        WHERE conrelid = format('pgstac.%%I', %s::text)::regclass
          AND contype = 'c' AND convalidated;
        """,
        [partition],
    )
    assert validated == 1, (
        f"{partition} has {validated} validated CHECK constraints after a "
        "widening load"
    )

    dtrange, edtrange = constraint_ranges(db, partition)
    assert "2019-01" in dtrange and "2021-12" in dtrange, dtrange
    assert "infinity" not in edtrange, edtrange


def test_loader_does_not_starve_readers(db: PgstacDB) -> None:
    """Searches must keep returning while a loader writes.

    Tightening the CHECK constraints takes ACCESS EXCLUSIVE on the partition,
    and the loader runs it outside the transaction holding the load's EXCLUSIVE
    lock so the two windows do not combine.
    """
    collection = "loader-readers"
    make_collection(db, collection, "month")
    assert (
        load([loader_item(collection, f"seed-{m}", month_dt(m)) for m in MONTHS])
        is None
    )

    search = sql.SQL(
        "SELECT search(jsonb_build_object("
        "'collections', jsonb_build_array({}), 'limit', 5));",
    ).format(sql.Literal(collection))
    stop = False
    waits: List[float] = []

    def reader(conn: psycopg.Connection) -> None:
        conn.execute("SET statement_timeout TO '30s';")
        while not stop:
            started = time.perf_counter()
            conn.execute(search)
            waits.append(time.perf_counter() - started)
            time.sleep(0.01)

    with ThreadPoolExecutor(max_workers=2) as pool:
        reading = pool.submit(worker, reader)
        try:
            for round_n in range(6):
                assert (
                    load(
                        [
                            loader_item(
                                collection,
                                f"r{round_n}-{m}",
                                month_dt(m, day=6 + round_n),
                            )
                            for m in MONTHS
                        ],
                        Methods.upsert,
                    )
                    is None
                )
        finally:
            stop = True
            reading.result()

    assert waits, "reader never completed a search"
    assert max(waits) < 10, (
        f"a search waited {max(waits):.1f}s while the loader ran; readers are "
        "being blocked by the loader's locks"
    )

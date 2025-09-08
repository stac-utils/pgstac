# Real-time Notifications

PgSTAC provides real-time notifications for item operations using PostgreSQL's `LISTEN`/`NOTIFY` mechanism. Applications can subscribe to notifications when items are inserted, updated, or deleted.

## Overview

When items are modified in PgSTAC, notifications are automatically sent to the `pgstac_items` channel. Applications can listen to this channel to receive immediate notifications about changes.

## Notification Channel

- **Channel name**: `pgstac_items`
- **Purpose**: Receives notifications for all item operations

## Notification Payload

Each notification contains a JSON payload:

```json
{
  "operation": "INSERT|UPDATE|DELETE",
  "collection": "collection_id",
  "item_id": "item_id",
  "timestamp": "2023-12-01T10:30:00.000Z"
}
```

## Usage

### SQL

```sql
-- Listen to item changes
LISTEN pgstac_items;

-- Wait for notifications
SELECT pg_sleep(1);
```

### Python

```python
import psycopg
import json

conn = psycopg.connect("postgresql://user:pass@host:5432/pgstac")
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("LISTEN pgstac_items")

# Listen for notifications
try:
    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.popleft()
            payload = json.loads(notify.payload)
            print(f"{payload['operation']}: {payload['collection']}/{payload['item_id']}")
except KeyboardInterrupt:
    pass
finally:
    conn.close()
```

### Python Async

```python
import asyncio
import json
import psycopg

async def listen():
    conn = await psycopg.AsyncConnection.connect("postgresql://...")
    async with conn.cursor() as cur:
        await cur.execute("LISTEN pgstac_items")

    async for notify in conn.notifies():
        payload = json.loads(notify.payload)
        print(f"{payload['operation']}: {payload['collection']}/{payload['item_id']}")

asyncio.run(listen())
```

## Management

### Enable/Disable Notifications

```sql
-- Disable notifications (useful during bulk operations)
SELECT set_item_notifications_enabled(false);

-- Re-enable notifications
SELECT set_item_notifications_enabled(true);
```

## Performance

For bulk operations, consider disabling notifications temporarily:

```python
async with db.transaction():
    await db.execute("SELECT set_item_notifications_enabled(false)")
    try:
        # Perform bulk operations
        await db.executemany("SELECT create_item(%s)", items)
    finally:
        await db.execute("SELECT set_item_notifications_enabled(true)")
```

**Note**: Use separate connections for listening vs. regular queries for best performance.

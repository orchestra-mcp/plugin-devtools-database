# Tools Reference (95 tools)

## Connection Management (5)

### db_connect

Connect to a database.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `driver` | string | Yes | Database driver: `postgres`, `sqlite`, `sqlite3`, `mysql`, `redis`, `mongodb` |
| `dsn` | string | Yes | Connection string |

**Examples:**
```json
{"driver": "postgres", "dsn": "postgres://user:pass@localhost:5432/mydb?sslmode=disable"}
{"driver": "sqlite", "dsn": "/tmp/test.db"}
{"driver": "mysql", "dsn": "root:pass@tcp(localhost:3306)/mydb"}
{"driver": "redis", "dsn": "redis://localhost:6379/0"}
{"driver": "mongodb", "dsn": "mongodb://localhost:27017/mydb"}
```

### db_disconnect

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID to close |

### db_list_connections

No arguments. Returns all active connections with ID, driver, and DSN.

### db_create_database

Create a new SQLite database file and auto-connect.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `path` | string | Yes | Path for the new SQLite file |

### db_drop_database

Drop (delete) a SQLite database file. **Destructive** — confirm with user first.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `path` | string | Yes | Path to the SQLite file to delete |

---

## Query & Data (3)

### db_query

Execute a SELECT query and return results as JSON.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `query` | string | Yes | SQL SELECT query |

### db_export

Export a table as CSV or JSON file. Not supported on Redis or MongoDB connections.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |
| `format` | string | Yes | `csv` or `json` |
| `path` | string | No | Output path (default: `./<table>.<format>`) |

### db_import

Import data from CSV/JSON into a table. Not supported on Redis or MongoDB connections.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Target table |
| `path` | string | Yes | Path to CSV/JSON file |
| `format` | string | No | `csv` or `json` (auto-detected from extension) |

---

## Schema Inspection (7)

### db_list_tables

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `schema` | string | No | Schema name (PostgreSQL only) |

### db_describe_table

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |

### db_list_indexes

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |

### db_list_views

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `schema` | string | No | Schema name |

### db_list_constraints

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |

### db_table_size

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |

### db_stats

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |

---

## DDL (7)

### db_create_table

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |
| `columns` | string | Yes | JSON array of column definitions |
| `if_not_exists` | boolean | No | Skip if table exists |

### db_alter_table

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |
| `action` | string | Yes | `add`, `drop`, or `rename` |
| `column` | string | No | Column definition (JSON for add) |
| `column_name` | string | No | Column name (for drop/rename) |
| `new_name` | string | No | New column name (for rename) |

### db_drop_table

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |

### db_create_index / db_drop_index

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `table` | string | Yes | Table name |
| `name` | string | Yes | Index name |
| `columns` | string | No | JSON array of column names (create only) |
| `unique` | boolean | No | Unique index (create only) |

### db_create_view / db_drop_view

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `name` | string | Yes | View name |
| `definition` | string | No | SQL query (create only) |

---

## PostgreSQL Advanced (48 tools)

All PG tools require `connection_id` pointing to a PostgreSQL connection.

### Maintenance (4)

- **pg_vacuum** — `table` (optional), `full` (bool), `analyze` (bool)
- **pg_analyze** — `table` (optional)
- **pg_reindex** — `target` (table/index/database name), `target_type` (`table`/`index`/`database`)
- **pg_cluster** — `table`, `index`

### Schema Management (5)

- **pg_create_schema** — `name`, `authorization` (optional)
- **pg_list_schemas** — no extra args
- **pg_drop_schema** — `name`, `cascade` (bool)
- **pg_set_search_path** — `schemas` (comma-separated)
- **pg_get_search_path** — no extra args

### Partitioning (4)

- **pg_create_partitioned_table** — `name`, `columns` (JSON), `partition_by` (`RANGE`/`LIST`/`HASH`), `partition_key`
- **pg_create_partition** — `parent`, `name`, `bound` (e.g. `FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')`)
- **pg_list_partitions** — `table`
- **pg_detach_partition** — `parent`, `partition`

### Row-Level Security (4)

- **pg_enable_rls** / **pg_disable_rls** — `table`
- **pg_create_policy** — `table`, `name`, `command` (`SELECT`/`INSERT`/`UPDATE`/`DELETE`/`ALL`), `using` (expression), `with_check` (expression), `role` (optional)
- **pg_list_policies** — `table` (optional)

### Replication (3)

- **pg_replication_status** — no extra args
- **pg_list_replication_slots** — no extra args
- **pg_list_publications** — no extra args

### Extensions & Performance (4)

- **pg_list_extensions** — `installed_only` (bool)
- **pg_enable_extension** — `name`
- **pg_table_bloat** — `table`
- **pg_index_bloat** — `table` (optional)

### Triggers (4)

- **pg_create_trigger_function** — `name`, `body` (PL/pgSQL), `returns` (default `trigger`)
- **pg_create_trigger** — `name`, `table`, `function`, `timing` (`BEFORE`/`AFTER`/`INSTEAD OF`), `events` (`INSERT`/`UPDATE`/`DELETE`), `for_each` (`ROW`/`STATEMENT`)
- **pg_list_triggers** — `table`
- **pg_drop_trigger** — `name`, `table`

### LISTEN/NOTIFY (3)

- **pg_notify** — `channel`, `payload`
- **pg_listen** — `channel`
- **pg_list_channels** — no extra args

### pgvector (8)

- **pg_enable_vectors** — enables the `vector` extension
- **pg_add_vector_column** — `table`, `column`, `dimensions`
- **pg_create_vector_index** — `table`, `column`, `type` (`hnsw`/`ivfflat`), `lists` (IVFFlat), `m`/`ef_construction` (HNSW)
- **pg_vector_search** — `table`, `column`, `vector` (JSON array), `limit`, `operator` (`<->`/`<#>`/`<=>`), `where` (optional)
- **pg_upsert_embedding** — `table`, `id_column`, `id_value`, `vector_column`, `vector` (JSON array), `extra` (optional JSON)
- **pg_bulk_upsert_embeddings** — `table`, `id_column`, `vector_column`, `rows` (JSON array)
- **pg_vector_stats** — `table`, `column`
- **pg_delete_embeddings** — `table`, `ids` (JSON array) or `where` (SQL filter)

### Roles & Permissions (4)

- **pg_list_roles** — no extra args
- **pg_create_role** — `name`, `password` (optional), `login` (bool), `superuser` (bool), `createdb` (bool)
- **pg_grant** — `privileges`, `on` (table/schema), `to` (role)
- **pg_revoke** — `privileges`, `on`, `from`

### Materialized Views (2)

- **pg_create_materialized_view** — `name`, `query`
- **pg_refresh_materialized_view** — `name`, `concurrently` (bool)

### Full-Text Search (3)

- **pg_add_tsvector_column** — `table`, `column`, `source_columns` (JSON array), `language` (default `english`)
- **pg_create_gin_index** — `table`, `column`
- **pg_fts_search** — `table`, `column`, `query`, `language` (optional), `limit` (optional)

---

## Redis (16 tools)

All Redis tools require `connection_id` pointing to a Redis connection. Each tool groups related commands under an `action` parameter.

### redis_keys
Actions: `scan`, `type`, `ttl`, `del`, `exists`, `rename`, `persist`, `expire`, `expireat`

### redis_strings
Actions: `get`, `set`, `mget`, `mset`, `incr`, `incrby`, `decr`, `decrby`, `append`, `getrange`, `strlen`, `setnx`, `setex`

### redis_hashes
Actions: `hget`, `hset`, `hmget`, `hmset`, `hgetall`, `hdel`, `hkeys`, `hvals`, `hlen`, `hexists`, `hincrby`

### redis_lists
Actions: `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`, `linsert`, `lset`, `lrem`

### redis_sets
Actions: `sadd`, `srem`, `smembers`, `sismember`, `scard`, `sunion`, `sinter`, `sdiff`, `spop`, `srandmember`

### redis_sorted_sets
Actions: `zadd`, `zrem`, `zrange`, `zrangebyscore`, `zscore`, `zcard`, `zrank`, `zcount`, `zincrby`, `zpopmin`, `zpopmax`

### redis_streams
Actions: `xadd`, `xread`, `xrange`, `xrevrange`, `xlen`, `xinfo_stream`, `xinfo_groups`, `xtrim`, `xdel`

### redis_pubsub
Actions: `publish`, `pubsub_channels`, `pubsub_numsub`, `pubsub_numpat`

### redis_server
Actions: `info`, `dbsize`, `config_get`, `config_set`, `client_list`, `slowlog_get`, `memory_usage`, `time`, `randomkey`

### redis_flushdb
Actions: `db` (flush current), `all` (flush all). **Destructive** — confirm with user first.

### redis_pipeline
Execute multiple commands in a pipeline or MULTI/EXEC transaction.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `commands` | array | Yes | Array of command strings |
| `transaction` | boolean | No | Wrap in MULTI/EXEC |

### redis_ttl_inspect
Bulk scan keys with type and TTL info.

### redis_scan_keys
Deep key scan with type, TTL, and memory usage per key.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `pattern` | string | No | Key pattern (default `*`) |
| `count` | integer | No | Max keys to scan |
| `type_filter` | string | No | Filter by Redis type |

### redis_bitmap
Actions: `setbit`, `getbit`, `bitcount`, `bitpos`, `bitop`

### redis_hyperloglog
Actions: `pfadd`, `pfcount`, `pfmerge`

### redis_geo
Actions: `geoadd`, `geodist`, `geopos`, `geosearch`

---

## MongoDB (9 tools)

All MongoDB tools require `connection_id` pointing to a MongoDB connection.

### mongo_documents

CRUD operations on MongoDB documents.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Collection name |
| `action` | string | Yes | `find`, `find_one`, `insert_one`, `insert_many`, `update_one`, `update_many`, `delete_one`, `delete_many`, `count`, `distinct` |
| `filter` | string | No | JSON filter document |
| `document` | string | No | JSON document (insert_one) |
| `documents` | string | No | JSON array of documents (insert_many) |
| `update` | string | No | JSON update document (update ops) |
| `field` | string | No | Field name (distinct) |
| `sort` | string | No | JSON sort specification |
| `limit` | integer | No | Max documents to return |
| `skip` | integer | No | Documents to skip |

**Examples:**
```json
{"action": "insert_one", "collection": "users", "document": "{\"name\": \"Alice\", \"age\": 25}"}
{"action": "find", "collection": "users", "filter": "{\"age\": {\"$gte\": 18}}", "sort": "{\"name\": 1}", "limit": 10}
{"action": "update_one", "collection": "users", "filter": "{\"name\": \"Alice\"}", "update": "{\"$set\": {\"age\": 26}}"}
{"action": "count", "collection": "users", "filter": "{\"role\": \"admin\"}"}
{"action": "distinct", "collection": "users", "field": "role"}
```

### mongo_aggregate

Execute MongoDB aggregation pipelines.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Collection name |
| `pipeline` | string | Yes | JSON array of pipeline stages |
| `allow_disk_use` | boolean | No | Allow disk for large aggregations |

**Example:**
```json
{"collection": "orders", "pipeline": "[{\"$match\": {\"status\": \"active\"}}, {\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}}}]"}
```

### mongo_indexes

Manage collection indexes.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Collection name |
| `action` | string | Yes | `list`, `create`, `create_text`, `create_ttl`, `drop` |
| `keys` | string | No | JSON index keys (create) |
| `name` | string | No | Index name |
| `unique` | boolean | No | Unique constraint |
| `field` | string | No | Field for text/TTL index |
| `expire_after_seconds` | integer | No | TTL in seconds |

### mongo_collections

Manage collections.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `action` | string | Yes | `list`, `create`, `drop`, `rename`, `stats` |
| `name` | string | No | Collection name |
| `new_name` | string | No | New name (rename) |

### mongo_server

Server administration commands.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `action` | string | Yes | `server_status`, `db_stats`, `list_databases`, `current_op`, `build_info` |

### mongo_schema

Schema inference and validation.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Collection name |
| `action` | string | Yes | `sample` or `validate` |
| `sample_size` | integer | No | Documents to sample (default 10) |

### mongo_bulk

Batch write operations.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Collection name |
| `operations` | string | Yes | JSON array of `{type, document/filter/update}` objects |
| `ordered` | boolean | No | Stop on first error (default true) |

**Example:**
```json
{"collection": "users", "operations": "[{\"type\": \"insert\", \"document\": {\"name\": \"Eve\"}}, {\"type\": \"update\", \"filter\": {\"name\": \"Alice\"}, \"update\": {\"$set\": {\"role\": \"admin\"}}}, {\"type\": \"delete\", \"filter\": {\"name\": \"Bob\"}}]"}
```

### mongo_export

Export a collection to a JSON file.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Collection to export |
| `path` | string | No | Output file (default `./<collection>.json`) |
| `filter` | string | No | JSON filter for subset export |

### mongo_import

Import documents from a JSON file.

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `connection_id` | string | Yes | Connection ID |
| `collection` | string | Yes | Target collection |
| `path` | string | Yes | Path to JSON file (array of objects) |
| `drop_first` | boolean | No | Drop collection before importing |

# Orchestra Plugin: devtools-database

A comprehensive database devtools plugin for [Orchestra MCP](https://github.com/orchestra-mcp/framework) providing **95 MCP tools** across 5 database providers.

## Supported Databases

| Provider | Driver | DSN Example |
|----------|--------|-------------|
| **PostgreSQL** | `postgres` | `postgres://user:pass@localhost:5432/mydb?sslmode=disable` |
| **SQLite** | `sqlite` / `sqlite3` | `/path/to/database.db` or `:memory:` |
| **MySQL** | `mysql` | `user:pass@tcp(localhost:3306)/mydb` |
| **Redis** | `redis` | `redis://localhost:6379/0` |
| **MongoDB** | `mongodb` | `mongodb://localhost:27017/mydb` |

## Install

```bash
go install github.com/orchestra-mcp/plugin-devtools-database/cmd@latest
```

## Usage

Add to your `plugins.yaml`:

```yaml
- id: tools.devtools-database
  binary: ./bin/devtools-database
  enabled: true
```

Connect via MCP:

```json
{"name": "db_connect", "arguments": {"driver": "postgres", "dsn": "postgres://localhost:5432/mydb"}}
```

## Tools (95 total)

### Universal (22 tools)

Work with all SQL providers (PostgreSQL, SQLite, MySQL).

| Tool | Description |
|------|-------------|
| `db_connect` | Connect to a database (sqlite, postgres, mysql, mongodb, redis) |
| `db_disconnect` | Disconnect from a database |
| `db_list_connections` | List all active connections |
| `db_create_database` | Create a new SQLite database file |
| `db_drop_database` | Drop a SQLite database file |
| `db_query` | Execute a SELECT query, return JSON |
| `db_export` | Export a table as CSV or JSON |
| `db_import` | Import data from CSV/JSON into a table |
| `db_list_tables` | List tables in a database |
| `db_describe_table` | Describe table columns and types |
| `db_list_indexes` | List indexes on a table |
| `db_list_views` | List views in a database |
| `db_list_constraints` | List constraints (PK, FK, unique, check) |
| `db_table_size` | Row count and storage size |
| `db_stats` | Database-level statistics |
| `db_create_table` | Create a table with typed columns |
| `db_alter_table` | Add, drop, or rename a column |
| `db_drop_table` | Drop a table |
| `db_create_index` | Create an index |
| `db_drop_index` | Drop an index |
| `db_create_view` | Create a view |
| `db_drop_view` | Drop a view |

### PostgreSQL Advanced (48 tools)

Requires a PostgreSQL connection.

| Category | Tools | Count |
|----------|-------|-------|
| Maintenance | `pg_vacuum`, `pg_analyze`, `pg_reindex`, `pg_cluster` | 4 |
| Schema Management | `pg_create_schema`, `pg_list_schemas`, `pg_drop_schema`, `pg_set_search_path`, `pg_get_search_path` | 5 |
| Partitioning | `pg_create_partitioned_table`, `pg_create_partition`, `pg_list_partitions`, `pg_detach_partition` | 4 |
| Row-Level Security | `pg_enable_rls`, `pg_disable_rls`, `pg_create_policy`, `pg_list_policies` | 4 |
| Replication | `pg_replication_status`, `pg_list_replication_slots`, `pg_list_publications` | 3 |
| Extensions & Perf | `pg_list_extensions`, `pg_enable_extension`, `pg_table_bloat`, `pg_index_bloat` | 4 |
| Triggers | `pg_create_trigger_function`, `pg_create_trigger`, `pg_list_triggers`, `pg_drop_trigger` | 4 |
| LISTEN/NOTIFY | `pg_notify`, `pg_listen`, `pg_list_channels` | 3 |
| pgvector | `pg_enable_vectors`, `pg_add_vector_column`, `pg_create_vector_index`, `pg_vector_search`, `pg_upsert_embedding`, `pg_bulk_upsert_embeddings`, `pg_vector_stats`, `pg_delete_embeddings` | 8 |
| Roles & Permissions | `pg_list_roles`, `pg_create_role`, `pg_grant`, `pg_revoke` | 4 |
| Materialized Views | `pg_create_materialized_view`, `pg_refresh_materialized_view` | 2 |
| Full-Text Search | `pg_add_tsvector_column`, `pg_create_gin_index`, `pg_fts_search` | 3 |

### Redis (16 tools)

Requires a Redis connection.

| Tool | Actions |
|------|---------|
| `redis_keys` | scan, type, ttl, del, exists, rename, persist, expire |
| `redis_strings` | get, set, mget, mset, incr, decr, append, etc. |
| `redis_hashes` | hget, hset, hgetall, hdel, hkeys, hvals, etc. |
| `redis_lists` | lpush, rpush, lpop, rpop, lrange, llen, etc. |
| `redis_sets` | sadd, srem, smembers, sinter, sunion, sdiff, etc. |
| `redis_sorted_sets` | zadd, zrange, zrangebyscore, zscore, zrank, etc. |
| `redis_streams` | xadd, xread, xrange, xlen, xinfo, xtrim, etc. |
| `redis_pubsub` | publish, pubsub_channels, pubsub_numsub, pubsub_numpat |
| `redis_server` | info, dbsize, config, client_list, slowlog, memory_usage |
| `redis_flushdb` | Flush database or all databases (destructive) |
| `redis_pipeline` | Batch commands with optional MULTI/EXEC transaction |
| `redis_ttl_inspect` | Bulk TTL inspection with type info |
| `redis_scan_keys` | Deep key scan with type, TTL, memory usage |
| `redis_bitmap` | setbit, getbit, bitcount, bitpos, bitop |
| `redis_hyperloglog` | pfadd, pfcount, pfmerge |
| `redis_geo` | geoadd, geodist, geopos, geosearch |

### MongoDB (9 tools)

Requires a MongoDB connection.

| Tool | Actions |
|------|---------|
| `mongo_documents` | find, find_one, insert_one, insert_many, update_one, update_many, delete_one, delete_many, count, distinct |
| `mongo_aggregate` | Execute aggregation pipelines ($match, $group, $sort, etc.) |
| `mongo_indexes` | list, create, create_text, create_ttl, drop |
| `mongo_collections` | list, create, drop, rename, stats |
| `mongo_server` | server_status, db_stats, list_databases, current_op, build_info |
| `mongo_schema` | Sample documents to infer field types, validate collection |
| `mongo_bulk` | Batch insert/update/delete operations |
| `mongo_export` | Export collection to JSON file |
| `mongo_import` | Import JSON file into collection |

## Architecture

```
internal/
├── db/
│   ├── manager.go           # Connection manager (SQL + NonSQL factories)
│   ├── provider.go           # Provider interface (20 methods) + types
│   └── providers/
│       ├── postgres.go       # PostgreSQL provider (pgx/v5)
│       ├── sqlite.go         # SQLite provider (modernc.org/sqlite)
│       ├── mysql.go          # MySQL provider (go-sql-driver/mysql)
│       ├── redis.go          # Redis provider (go-redis/v9)
│       └── mongodb.go        # MongoDB provider (mongo-driver/v2)
├── tools/
│   ├── db_*.go               # 22 universal tools
│   ├── pg_*.go               # 48 PostgreSQL-specific tools
│   ├── redis_*.go            # 16 Redis-specific tools
│   └── mongo_*.go            # 9 MongoDB-specific tools
└── plugin.go                 # Tool registration (95 tools)
```

### Provider Pattern

Each database has a Provider implementing the 20-method `db.Provider` interface. SQL providers are created via `ProviderFactory` (from `*sql.DB`), while NoSQL providers use `NonSQLProviderFactory` (from DSN string directly).

Provider-specific tools use guard helpers (`getPostgresProvider`, `getRedisProvider`, `getMongoDBProvider`) that type-assert the connection's provider and return a user-friendly error if the wrong database type is connected.

## Dependencies

- `github.com/jackc/pgx/v5` — PostgreSQL driver
- `modernc.org/sqlite` — Pure-Go SQLite driver
- `github.com/go-sql-driver/mysql` — MySQL driver
- `github.com/redis/go-redis/v9` — Redis client
- `go.mongodb.org/mongo-driver/v2` — MongoDB driver

## Related Packages

- [sdk-go](https://github.com/orchestra-mcp/sdk-go) — Plugin SDK
- [gen-go](https://github.com/orchestra-mcp/gen-go) — Generated Protobuf types

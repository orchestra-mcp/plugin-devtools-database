package internal

import (
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/tools"
	"github.com/orchestra-mcp/sdk-go/plugin"

	// Register provider factories via init().
	_ "github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
)

// ToolsPlugin registers all database devtools.
type ToolsPlugin struct{}

// RegisterTools registers all 95 database tools with the plugin builder.
func (tp *ToolsPlugin) RegisterTools(builder *plugin.PluginBuilder) {
	mgr := db.NewManager()

	// --- Connection management (5) ---
	builder.RegisterTool("db_connect",
		"Connect to a database (sqlite, postgres, mysql, mongodb, redis, firestore)",
		tools.DbConnectSchema(), tools.DbConnect(mgr))

	builder.RegisterTool("db_disconnect",
		"Disconnect from a database",
		tools.DbDisconnectSchema(), tools.DbDisconnect(mgr))

	builder.RegisterTool("db_list_connections",
		"List all active database connections",
		tools.DbListConnectionsSchema(), tools.DbListConnections(mgr))

	builder.RegisterTool("db_create_database",
		"Create a new SQLite database file and auto-connect to it",
		tools.DbCreateDatabaseSchema(), tools.DbCreateDatabase(mgr))

	builder.RegisterTool("db_drop_database",
		"Drop (delete) a SQLite database file. IMPORTANT: Use AskUserQuestion to confirm with the user BEFORE calling this tool.",
		tools.DbDropDatabaseSchema(), tools.DbDropDatabase(mgr))

	// --- Query / Data (3) ---
	builder.RegisterTool("db_query",
		"Execute a SELECT query and return results as JSON",
		tools.DbQuerySchema(), tools.DbQuery(mgr))

	builder.RegisterTool("db_export",
		"Export a table as CSV or JSON",
		tools.DbExportSchema(), tools.DbExport(mgr))

	builder.RegisterTool("db_import",
		"Import data from a CSV or JSON file into a table",
		tools.DbImportSchema(), tools.DbImport(mgr))

	// --- Schema inspection (7) ---
	builder.RegisterTool("db_list_tables",
		"List tables in a database",
		tools.DbListTablesSchema(), tools.DbListTables(mgr))

	builder.RegisterTool("db_describe_table",
		"Describe table columns and types",
		tools.DbDescribeTableSchema(), tools.DbDescribeTable(mgr))

	builder.RegisterTool("db_list_indexes",
		"List indexes on a table",
		tools.DbListIndexesSchema(), tools.DbListIndexes(mgr))

	builder.RegisterTool("db_list_views",
		"List views in a database",
		tools.DbListViewsSchema(), tools.DbListViews(mgr))

	builder.RegisterTool("db_list_constraints",
		"List constraints on a table (PK, FK, unique, check)",
		tools.DbListConstraintsSchema(), tools.DbListConstraints(mgr))

	builder.RegisterTool("db_table_size",
		"Show row count and storage size for a table",
		tools.DbTableSizeSchema(), tools.DbTableSize(mgr))

	builder.RegisterTool("db_stats",
		"Show database-level statistics (size, table count, version)",
		tools.DbStatsSchema(), tools.DbStats(mgr))

	// --- DDL (7) ---
	builder.RegisterTool("db_create_table",
		"Create a new table with typed columns (uses canonical types mapped per provider)",
		tools.DbCreateTableSchema(), tools.DbCreateTable(mgr))

	builder.RegisterTool("db_alter_table",
		"Alter a table: add, drop, or rename a column",
		tools.DbAlterTableSchema(), tools.DbAlterTable(mgr))

	builder.RegisterTool("db_drop_table",
		"Drop a table. Use AskUserQuestion to confirm with the user BEFORE calling this tool.",
		tools.DbDropTableSchema(), tools.DbDropTable(mgr))

	builder.RegisterTool("db_create_index",
		"Create an index on a table",
		tools.DbCreateIndexSchema(), tools.DbCreateIndex(mgr))

	builder.RegisterTool("db_drop_index",
		"Drop an index from a table",
		tools.DbDropIndexSchema(), tools.DbDropIndex(mgr))

	builder.RegisterTool("db_create_view",
		"Create a view from a SQL query",
		tools.DbCreateViewSchema(), tools.DbCreateView(mgr))

	builder.RegisterTool("db_drop_view",
		"Drop a view",
		tools.DbDropViewSchema(), tools.DbDropView(mgr))

	// --- PostgreSQL Advanced: Maintenance (4) ---
	builder.RegisterTool("pg_vacuum",
		"Run VACUUM on a PostgreSQL table or database (requires postgres connection)",
		tools.PgVacuumSchema(), tools.PgVacuum(mgr))

	builder.RegisterTool("pg_analyze",
		"Run ANALYZE on a PostgreSQL table or database to update statistics (requires postgres connection)",
		tools.PgAnalyzeSchema(), tools.PgAnalyze(mgr))

	builder.RegisterTool("pg_reindex",
		"Rebuild indexes on a PostgreSQL table, index, or database (requires postgres connection)",
		tools.PgReindexSchema(), tools.PgReindex(mgr))

	builder.RegisterTool("pg_cluster",
		"Re-order a PostgreSQL table on disk based on an index (requires postgres connection)",
		tools.PgClusterSchema(), tools.PgCluster(mgr))

	// --- PostgreSQL Advanced: Schema Management (5) ---
	builder.RegisterTool("pg_create_schema",
		"Create a new PostgreSQL schema (requires postgres connection)",
		tools.PgCreateSchemaSchema(), tools.PgCreateSchema(mgr))

	builder.RegisterTool("pg_list_schemas",
		"List all schemas in a PostgreSQL database (requires postgres connection)",
		tools.PgListSchemasSchema(), tools.PgListSchemas(mgr))

	builder.RegisterTool("pg_drop_schema",
		"Drop a PostgreSQL schema (requires postgres connection)",
		tools.PgDropSchemaSchema(), tools.PgDropSchema(mgr))

	builder.RegisterTool("pg_set_search_path",
		"Set the PostgreSQL search_path for the current session (requires postgres connection)",
		tools.PgSetSearchPathSchema(), tools.PgSetSearchPath(mgr))

	builder.RegisterTool("pg_get_search_path",
		"Get the current PostgreSQL search_path (requires postgres connection)",
		tools.PgGetSearchPathSchema(), tools.PgGetSearchPath(mgr))

	// --- PostgreSQL Advanced: Partitioning (4) ---
	builder.RegisterTool("pg_create_partitioned_table",
		"Create a partitioned table in PostgreSQL (RANGE, LIST, or HASH) (requires postgres connection)",
		tools.PgCreatePartitionedTableSchema(), tools.PgCreatePartitionedTable(mgr))

	builder.RegisterTool("pg_create_partition",
		"Create a partition of a PostgreSQL partitioned table (requires postgres connection)",
		tools.PgCreatePartitionSchema(), tools.PgCreatePartition(mgr))

	builder.RegisterTool("pg_list_partitions",
		"List partitions of a PostgreSQL partitioned table (requires postgres connection)",
		tools.PgListPartitionsSchema(), tools.PgListPartitions(mgr))

	builder.RegisterTool("pg_detach_partition",
		"Detach a partition from a PostgreSQL partitioned table (requires postgres connection)",
		tools.PgDetachPartitionSchema(), tools.PgDetachPartition(mgr))

	// --- PostgreSQL Advanced: Row-Level Security (4) ---
	builder.RegisterTool("pg_enable_rls",
		"Enable Row-Level Security on a PostgreSQL table (requires postgres connection)",
		tools.PgEnableRLSSchema(), tools.PgEnableRLS(mgr))

	builder.RegisterTool("pg_disable_rls",
		"Disable Row-Level Security on a PostgreSQL table (requires postgres connection)",
		tools.PgDisableRLSSchema(), tools.PgDisableRLS(mgr))

	builder.RegisterTool("pg_create_policy",
		"Create a Row-Level Security policy on a PostgreSQL table (requires postgres connection)",
		tools.PgCreatePolicySchema(), tools.PgCreatePolicy(mgr))

	builder.RegisterTool("pg_list_policies",
		"List Row-Level Security policies in PostgreSQL (requires postgres connection)",
		tools.PgListPoliciesSchema(), tools.PgListPolicies(mgr))

	// --- PostgreSQL Advanced: Replication (3) ---
	builder.RegisterTool("pg_replication_status",
		"Show PostgreSQL replication status (pg_stat_replication) (requires postgres connection)",
		tools.PgReplicationStatusSchema(), tools.PgReplicationStatus(mgr))

	builder.RegisterTool("pg_list_replication_slots",
		"List PostgreSQL replication slots (requires postgres connection)",
		tools.PgListReplicationSlotsSchema(), tools.PgListReplicationSlots(mgr))

	builder.RegisterTool("pg_list_publications",
		"List PostgreSQL logical replication publications (requires postgres connection)",
		tools.PgListPublicationsSchema(), tools.PgListPublications(mgr))

	// --- PostgreSQL Advanced: Extensions & Performance (4) ---
	builder.RegisterTool("pg_list_extensions",
		"List installed or available PostgreSQL extensions (requires postgres connection)",
		tools.PgListExtensionsSchema(), tools.PgListExtensions(mgr))

	builder.RegisterTool("pg_enable_extension",
		"Install/enable a PostgreSQL extension (requires postgres connection)",
		tools.PgEnableExtensionSchema(), tools.PgEnableExtension(mgr))

	builder.RegisterTool("pg_table_bloat",
		"Analyze table bloat (dead tuples, vacuum stats) in PostgreSQL (requires postgres connection)",
		tools.PgTableBloatSchema(), tools.PgTableBloat(mgr))

	builder.RegisterTool("pg_index_bloat",
		"Analyze index bloat in PostgreSQL (requires postgres connection)",
		tools.PgIndexBloatSchema(), tools.PgIndexBloat(mgr))

	// --- PostgreSQL Advanced: Triggers (4) ---
	builder.RegisterTool("pg_create_trigger_function",
		"Create a PL/pgSQL trigger function (requires postgres connection)",
		tools.PgCreateTriggerFunctionSchema(), tools.PgCreateTriggerFunction(mgr))

	builder.RegisterTool("pg_create_trigger",
		"Create a trigger on a PostgreSQL table (requires postgres connection)",
		tools.PgCreateTriggerSchema(), tools.PgCreateTrigger(mgr))

	builder.RegisterTool("pg_list_triggers",
		"List triggers on a PostgreSQL table (requires postgres connection)",
		tools.PgListTriggersSchema(), tools.PgListTriggers(mgr))

	builder.RegisterTool("pg_drop_trigger",
		"Drop a trigger from a PostgreSQL table (requires postgres connection)",
		tools.PgDropTriggerSchema(), tools.PgDropTrigger(mgr))

	// --- PostgreSQL Advanced: Events / LISTEN-NOTIFY (3) ---
	builder.RegisterTool("pg_notify",
		"Send a NOTIFY event on a PostgreSQL channel (requires postgres connection)",
		tools.PgNotifySchema(), tools.PgNotify(mgr))

	builder.RegisterTool("pg_listen",
		"Register a LISTEN on a PostgreSQL channel (requires postgres connection)",
		tools.PgListenSchema(), tools.PgListen(mgr))

	builder.RegisterTool("pg_list_channels",
		"List active LISTEN channels on a PostgreSQL connection (requires postgres connection)",
		tools.PgListChannelsSchema(), tools.PgListChannels(mgr))

	// --- PostgreSQL Advanced: pgvector (8) ---
	builder.RegisterTool("pg_enable_vectors",
		"Enable the pgvector extension for vector similarity search (requires postgres connection)",
		tools.PgEnableVectorsSchema(), tools.PgEnableVectors(mgr))

	builder.RegisterTool("pg_add_vector_column",
		"Add a vector column to a PostgreSQL table (requires postgres + pgvector)",
		tools.PgAddVectorColumnSchema(), tools.PgAddVectorColumn(mgr))

	builder.RegisterTool("pg_create_vector_index",
		"Create an HNSW or IVFFlat index on a vector column (requires postgres + pgvector)",
		tools.PgCreateVectorIndexSchema(), tools.PgCreateVectorIndex(mgr))

	builder.RegisterTool("pg_vector_search",
		"Perform similarity search on a vector column (requires postgres + pgvector)",
		tools.PgVectorSearchSchema(), tools.PgVectorSearch(mgr))

	builder.RegisterTool("pg_upsert_embedding",
		"Insert or update an embedding vector for a row (requires postgres + pgvector)",
		tools.PgUpsertEmbeddingSchema(), tools.PgUpsertEmbedding(mgr))

	builder.RegisterTool("pg_bulk_upsert_embeddings",
		"Bulk insert/update embedding vectors in a transaction (requires postgres + pgvector)",
		tools.PgBulkUpsertEmbeddingsSchema(), tools.PgBulkUpsertEmbeddings(mgr))

	builder.RegisterTool("pg_vector_stats",
		"Get statistics about a vector column (dimensions, row count, index info) (requires postgres + pgvector)",
		tools.PgVectorStatsSchema(), tools.PgVectorStats(mgr))

	builder.RegisterTool("pg_delete_embeddings",
		"Delete embedding rows by ID list or WHERE filter (requires postgres + pgvector)",
		tools.PgDeleteEmbeddingsSchema(), tools.PgDeleteEmbeddings(mgr))

	// --- PostgreSQL Advanced: Roles & Permissions (4) ---
	builder.RegisterTool("pg_list_roles",
		"List all database roles/users in PostgreSQL (requires postgres connection)",
		tools.PgListRolesSchema(), tools.PgListRoles(mgr))

	builder.RegisterTool("pg_create_role",
		"Create a new PostgreSQL database role (requires postgres connection)",
		tools.PgCreateRoleSchema(), tools.PgCreateRole(mgr))

	builder.RegisterTool("pg_grant",
		"Grant privileges to a PostgreSQL role (requires postgres connection)",
		tools.PgGrantSchema(), tools.PgGrant(mgr))

	builder.RegisterTool("pg_revoke",
		"Revoke privileges from a PostgreSQL role (requires postgres connection)",
		tools.PgRevokeSchema(), tools.PgRevoke(mgr))

	// --- PostgreSQL Advanced: Materialized Views (2) ---
	builder.RegisterTool("pg_create_materialized_view",
		"Create a materialized view in PostgreSQL (requires postgres connection)",
		tools.PgCreateMaterializedViewSchema(), tools.PgCreateMaterializedView(mgr))

	builder.RegisterTool("pg_refresh_materialized_view",
		"Refresh a materialized view in PostgreSQL (requires postgres connection)",
		tools.PgRefreshMaterializedViewSchema(), tools.PgRefreshMaterializedView(mgr))

	// --- PostgreSQL Advanced: Full-Text Search (3) ---
	builder.RegisterTool("pg_add_tsvector_column",
		"Add a tsvector column with auto-update trigger for full-text search (requires postgres connection)",
		tools.PgAddTsvectorColumnSchema(), tools.PgAddTsvectorColumn(mgr))

	builder.RegisterTool("pg_create_gin_index",
		"Create a GIN index on a tsvector or JSONB column (requires postgres connection)",
		tools.PgCreateGINIndexSchema(), tools.PgCreateGINIndex(mgr))

	builder.RegisterTool("pg_fts_search",
		"Perform full-text search on a tsvector column with ranking (requires postgres connection)",
		tools.PgFTSSearchSchema(), tools.PgFTSSearch(mgr))

	// --- Redis: Key Management (1) ---
	builder.RegisterTool("redis_keys",
		"Manage Redis keys: scan, type, ttl, del, exists, rename, persist, expire (requires redis connection)",
		tools.RedisKeysSchema(), tools.RedisKeys(mgr))

	// --- Redis: Data Structures (5) ---
	builder.RegisterTool("redis_strings",
		"Redis string operations: get, set, mget, mset, incr, decr, append, etc. (requires redis connection)",
		tools.RedisStringsSchema(), tools.RedisStrings(mgr))

	builder.RegisterTool("redis_hashes",
		"Redis hash operations: hget, hset, hgetall, hdel, hkeys, hvals, etc. (requires redis connection)",
		tools.RedisHashesSchema(), tools.RedisHashes(mgr))

	builder.RegisterTool("redis_lists",
		"Redis list operations: lpush, rpush, lpop, rpop, lrange, llen, etc. (requires redis connection)",
		tools.RedisListsSchema(), tools.RedisLists(mgr))

	builder.RegisterTool("redis_sets",
		"Redis set operations: sadd, srem, smembers, sinter, sunion, sdiff, etc. (requires redis connection)",
		tools.RedisSetsSchema(), tools.RedisSets(mgr))

	builder.RegisterTool("redis_sorted_sets",
		"Redis sorted set operations: zadd, zrange, zrangebyscore, zscore, zrank, etc. (requires redis connection)",
		tools.RedisSortedSetsSchema(), tools.RedisSortedSets(mgr))

	// --- Redis: Streams & Pub/Sub (2) ---
	builder.RegisterTool("redis_streams",
		"Redis stream operations: xadd, xread, xrange, xlen, xinfo, xtrim, etc. (requires redis connection)",
		tools.RedisStreamsSchema(), tools.RedisStreams(mgr))

	builder.RegisterTool("redis_pubsub",
		"Redis pub/sub operations: publish, pubsub_channels, pubsub_numsub, pubsub_numpat (requires redis connection)",
		tools.RedisPubSubSchema(), tools.RedisPubSub(mgr))

	// --- Redis: Server & Admin (3) ---
	builder.RegisterTool("redis_server",
		"Redis server info: info, dbsize, config, client_list, slowlog, memory_usage, etc. (requires redis connection)",
		tools.RedisServerSchema(), tools.RedisServer(mgr))

	builder.RegisterTool("redis_flushdb",
		"Flush Redis database or all databases. DESTRUCTIVE — use AskUserQuestion to confirm first. (requires redis connection)",
		tools.RedisFlushDBSchema(), tools.RedisFlushDB(mgr))

	builder.RegisterTool("redis_pipeline",
		"Execute multiple Redis commands in a pipeline or MULTI/EXEC transaction (requires redis connection)",
		tools.RedisPipelineSchema(), tools.RedisPipeline(mgr))

	// --- Redis: Inspection & Analysis (2) ---
	builder.RegisterTool("redis_ttl_inspect",
		"Bulk TTL inspection — scan keys with type and TTL for debugging cache patterns (requires redis connection)",
		tools.RedisTTLInspectSchema(), tools.RedisTTLInspect(mgr))

	builder.RegisterTool("redis_scan_keys",
		"Deep key scan with type, TTL, and memory usage per key (requires redis connection)",
		tools.RedisScanKeysSchema(), tools.RedisScanKeys(mgr))

	// --- Redis: Specialized Data Types (3) ---
	builder.RegisterTool("redis_bitmap",
		"Redis bitmap operations: setbit, getbit, bitcount, bitpos, bitop (requires redis connection)",
		tools.RedisBitmapSchema(), tools.RedisBitmap(mgr))

	builder.RegisterTool("redis_hyperloglog",
		"Redis HyperLogLog operations: pfadd, pfcount, pfmerge (requires redis connection)",
		tools.RedisHyperLogLogSchema(), tools.RedisHyperLogLog(mgr))

	builder.RegisterTool("redis_geo",
		"Redis geospatial operations: geoadd, geodist, geopos, geosearch (requires redis connection)",
		tools.RedisGeoSchema(), tools.RedisGeo(mgr))

	// --- MongoDB: Document CRUD (1) ---
	builder.RegisterTool("mongo_documents",
		"MongoDB document operations: find, find_one, insert_one, insert_many, update_one, update_many, delete_one, delete_many, count, distinct (requires mongodb connection)",
		tools.MongoDocumentsSchema(), tools.MongoDocuments(mgr))

	// --- MongoDB: Aggregation (1) ---
	builder.RegisterTool("mongo_aggregate",
		"Execute a MongoDB aggregation pipeline on a collection (requires mongodb connection)",
		tools.MongoAggregateSchema(), tools.MongoAggregate(mgr))

	// --- MongoDB: Index Management (1) ---
	builder.RegisterTool("mongo_indexes",
		"MongoDB index management: list, create, create_text, create_ttl, drop (requires mongodb connection)",
		tools.MongoIndexesSchema(), tools.MongoIndexes(mgr))

	// --- MongoDB: Collection Management (1) ---
	builder.RegisterTool("mongo_collections",
		"MongoDB collection management: list, create, drop, rename, stats (requires mongodb connection)",
		tools.MongoCollectionsSchema(), tools.MongoCollections(mgr))

	// --- MongoDB: Server Administration (1) ---
	builder.RegisterTool("mongo_server",
		"MongoDB server info: server_status, db_stats, list_databases, current_op, build_info (requires mongodb connection)",
		tools.MongoServerSchema(), tools.MongoServer(mgr))

	// --- MongoDB: Schema Analysis (1) ---
	builder.RegisterTool("mongo_schema",
		"MongoDB schema analysis: sample documents to infer fields/types, validate collection (requires mongodb connection)",
		tools.MongoSchemaSchema(), tools.MongoSchema(mgr))

	// --- MongoDB: Bulk Operations (1) ---
	builder.RegisterTool("mongo_bulk",
		"Execute bulk write operations (insert, update, delete) on a MongoDB collection (requires mongodb connection)",
		tools.MongoBulkSchema(), tools.MongoBulk(mgr))

	// --- MongoDB: Export & Import (2) ---
	builder.RegisterTool("mongo_export",
		"Export a MongoDB collection to a JSON file (requires mongodb connection)",
		tools.MongoExportSchema(), tools.MongoExport(mgr))

	builder.RegisterTool("mongo_import",
		"Import documents from a JSON file into a MongoDB collection (requires mongodb connection)",
		tools.MongoImportSchema(), tools.MongoImport(mgr))
}

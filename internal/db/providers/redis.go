package providers

import (
	"context"
	"fmt"
	"strings"

	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/redis/go-redis/v9"
)

// RedisProvider implements db.Provider for Redis.
type RedisProvider struct {
	client *redis.Client
}

func init() {
	db.RegisterNonSQLProviderFactory("redis", func(dsn string) (db.Provider, error) {
		opts, err := redis.ParseURL(dsn)
		if err != nil {
			return nil, fmt.Errorf("parse redis URL: %w", err)
		}
		client := redis.NewClient(opts)

		if err := client.Ping(context.Background()).Err(); err != nil {
			client.Close()
			return nil, fmt.Errorf("ping redis: %w", err)
		}

		return &RedisProvider{client: client}, nil
	})
}

// Client returns the underlying redis.Client for use by Redis-specific tools.
func (r *RedisProvider) Client() *redis.Client {
	return r.client
}

func (r *RedisProvider) Kind() db.ProviderKind { return db.ProviderRedis }

func (r *RedisProvider) Close() error { return r.client.Close() }

func (r *RedisProvider) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Query interprets the string as a raw Redis command via Do().
// Returns the result as a single-row map.
func (r *RedisProvider) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	cmdArgs := make([]any, len(parts))
	for i, p := range parts {
		cmdArgs[i] = p
	}
	cmdArgs = append(cmdArgs, args...)

	result, err := r.client.Do(ctx, cmdArgs...).Result()
	if err != nil {
		return nil, err
	}

	return []map[string]any{{"result": result}}, nil
}

// Exec interprets the string as a raw Redis command via Do(). Returns 1 on success.
func (r *RedisProvider) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return 0, fmt.Errorf("empty command")
	}

	cmdArgs := make([]any, len(parts))
	for i, p := range parts {
		cmdArgs[i] = p
	}
	cmdArgs = append(cmdArgs, args...)

	if err := r.client.Do(ctx, cmdArgs...).Err(); err != nil {
		return 0, err
	}
	return 1, nil
}

// ListTables returns keyspace databases from INFO keyspace.
func (r *RedisProvider) ListTables(ctx context.Context, _ string) ([]db.TableInfo, error) {
	info, err := r.client.Info(ctx, "keyspace").Result()
	if err != nil {
		return nil, err
	}

	var tables []db.TableInfo
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "db") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				tables = append(tables, db.TableInfo{Name: parts[0] + " (" + parts[1] + ")"})
			}
		}
	}
	return tables, nil
}

// DatabaseStats returns INFO server + memory + DBSIZE.
func (r *RedisProvider) DatabaseStats(ctx context.Context) (*db.DbStats, error) {
	info, err := r.client.Info(ctx, "server", "memory").Result()
	if err != nil {
		return nil, err
	}

	dbSize, err := r.client.DBSize(ctx).Result()
	if err != nil {
		return nil, err
	}

	extra := map[string]any{"key_count": dbSize}

	// Parse version and memory from INFO.
	var version string
	var memBytes int64
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			version = strings.TrimPrefix(line, "redis_version:")
		}
		if strings.HasPrefix(line, "used_memory:") {
			fmt.Sscanf(strings.TrimPrefix(line, "used_memory:"), "%d", &memBytes)
		}
		if strings.HasPrefix(line, "used_memory_human:") {
			extra["used_memory_human"] = strings.TrimPrefix(line, "used_memory_human:")
		}
		if strings.HasPrefix(line, "maxmemory_human:") {
			extra["maxmemory_human"] = strings.TrimPrefix(line, "maxmemory_human:")
		}
		if strings.HasPrefix(line, "connected_clients:") {
			extra["connected_clients"] = strings.TrimPrefix(line, "connected_clients:")
		}
	}

	return &db.DbStats{
		SizeBytes:  memBytes,
		TableCount: int(dbSize),
		Provider:   "redis",
		Version:    version,
		Extra:      extra,
	}, nil
}

// TableSize returns DBSIZE (key count) as the "row count".
func (r *RedisProvider) TableSize(ctx context.Context, _ string) (*db.TableStats, error) {
	n, err := r.client.DBSize(ctx).Result()
	if err != nil {
		return nil, err
	}
	return &db.TableStats{RowCount: n}, nil
}

// --- Unsupported operations ---

func (r *RedisProvider) DescribeTable(context.Context, string) ([]db.ColumnInfo, error) {
	return nil, db.ErrUnsupported
}

func (r *RedisProvider) ListIndexes(context.Context, string) ([]db.IndexInfo, error) {
	return nil, db.ErrUnsupported
}

func (r *RedisProvider) ListConstraints(context.Context, string) ([]db.ConstraintInfo, error) {
	return nil, db.ErrUnsupported
}

func (r *RedisProvider) ListViews(context.Context, string) ([]db.ViewInfo, error) {
	return nil, db.ErrUnsupported
}

func (r *RedisProvider) CreateTable(context.Context, string, []db.ColumnDef, bool) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) AlterTableAdd(context.Context, string, db.ColumnDef) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) AlterTableDrop(context.Context, string, string) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) AlterTableRename(context.Context, string, string, string) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) DropTable(context.Context, string, bool) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) CreateIndex(context.Context, string, db.IndexDef) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) DropIndex(context.Context, string, string) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) CreateView(context.Context, db.ViewDef) error {
	return db.ErrUnsupported
}

func (r *RedisProvider) DropView(context.Context, string) error {
	return db.ErrUnsupported
}

package db

import (
	"context"
	"errors"
)

// ProviderKind identifies the database provider type.
type ProviderKind string

const (
	ProviderSQLite    ProviderKind = "sqlite"
	ProviderPostgres  ProviderKind = "postgres"
	ProviderMySQL     ProviderKind = "mysql"
	ProviderMongoDB   ProviderKind = "mongodb"
	ProviderRedis     ProviderKind = "redis"
	ProviderFirestore ProviderKind = "firestore"
)

// ErrUnsupported is returned when a provider doesn't support an operation.
var ErrUnsupported = errors.New("operation not supported by this provider")

// ColumnDef describes a column/field for create/alter table operations.
type ColumnDef struct {
	Name          string `json:"name"`
	Type          string `json:"type"`           // Canonical: string, text, integer, bigint, float, decimal, boolean, timestamp, date, json, blob, uuid, serial
	Nullable      bool   `json:"nullable"`
	Default       string `json:"default,omitempty"`
	PrimaryKey    bool   `json:"primary_key,omitempty"`
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	Unique        bool   `json:"unique,omitempty"`
	References    string `json:"references,omitempty"` // "table(column)" for FK
}

// IndexDef describes an index to create.
type IndexDef struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// ViewDef describes a view to create.
type ViewDef struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

// TableInfo is a summary of a table/collection.
type TableInfo struct {
	Name string `json:"name"`
}

// ColumnInfo describes an existing column/field.
type ColumnInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   string `json:"nullable"`
	Default    string `json:"default"`
	PrimaryKey bool   `json:"primary_key"`
	Extra      string `json:"extra,omitempty"`
}

// IndexInfo describes an existing index.
type IndexInfo struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// ConstraintInfo describes a table constraint.
type ConstraintInfo struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"` // "primary_key", "foreign_key", "unique", "check"
	Columns    []string `json:"columns"`
	RefTable   string   `json:"ref_table,omitempty"`
	RefColumns []string `json:"ref_columns,omitempty"`
	Definition string   `json:"definition,omitempty"`
}

// ViewInfo describes an existing view.
type ViewInfo struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

// TableStats holds size/row statistics for a table.
type TableStats struct {
	RowCount  int64 `json:"row_count"`
	SizeBytes int64 `json:"size_bytes"`
	IndexSize int64 `json:"index_size_bytes,omitempty"`
	TotalSize int64 `json:"total_size_bytes,omitempty"`
}

// DbStats holds database-level statistics.
type DbStats struct {
	SizeBytes  int64          `json:"size_bytes"`
	TableCount int            `json:"table_count"`
	IndexCount int            `json:"index_count"`
	Provider   string         `json:"provider"`
	Version    string         `json:"version,omitempty"`
	Extra      map[string]any `json:"extra,omitempty"`
}

// Provider is the core abstraction for all database operations.
type Provider interface {
	Kind() ProviderKind
	Close() error
	Ping(ctx context.Context) error

	// Query/Exec
	Query(ctx context.Context, query string, args ...any) ([]map[string]any, error)
	Exec(ctx context.Context, query string, args ...any) (int64, error)

	// Schema Inspection
	ListTables(ctx context.Context, schema string) ([]TableInfo, error)
	DescribeTable(ctx context.Context, table string) ([]ColumnInfo, error)
	ListIndexes(ctx context.Context, table string) ([]IndexInfo, error)
	ListConstraints(ctx context.Context, table string) ([]ConstraintInfo, error)
	ListViews(ctx context.Context, schema string) ([]ViewInfo, error)
	TableSize(ctx context.Context, table string) (*TableStats, error)
	DatabaseStats(ctx context.Context) (*DbStats, error)

	// DDL
	CreateTable(ctx context.Context, name string, columns []ColumnDef, ifNotExists bool) error
	AlterTableAdd(ctx context.Context, table string, column ColumnDef) error
	AlterTableDrop(ctx context.Context, table string, columnName string) error
	AlterTableRename(ctx context.Context, table, oldCol, newCol string) error
	DropTable(ctx context.Context, name string, ifExists bool) error
	CreateIndex(ctx context.Context, table string, index IndexDef) error
	DropIndex(ctx context.Context, table, indexName string) error
	CreateView(ctx context.Context, view ViewDef) error
	DropView(ctx context.Context, name string) error
}

// MigrationProvider is optionally implemented by SQL providers for migration tracking.
type MigrationProvider interface {
	EnsureMigrationTable(ctx context.Context) error
	AppliedMigrations(ctx context.Context) ([]AppliedMigration, error)
	RecordMigration(ctx context.Context, version, name string) error
	RemoveMigration(ctx context.Context, version string) error
}

// AppliedMigration records a migration that has been applied.
type AppliedMigration struct {
	Version   string `json:"version"`
	Name      string `json:"name"`
	AppliedAt string `json:"applied_at"`
}

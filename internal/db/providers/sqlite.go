package providers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
)

// SQLiteProvider implements db.Provider for SQLite databases.
type SQLiteProvider struct {
	sqlDB *sql.DB
}

func init() {
	db.RegisterProviderFactory("sqlite", func(sqlDB *sql.DB) db.Provider {
		return NewSQLite(sqlDB)
	})
}

// NewSQLite creates a new SQLite provider wrapping the given *sql.DB.
func NewSQLite(sqlDB *sql.DB) *SQLiteProvider {
	return &SQLiteProvider{sqlDB: sqlDB}
}

func (s *SQLiteProvider) Kind() db.ProviderKind {
	return db.ProviderSQLite
}

func (s *SQLiteProvider) Close() error {
	return s.sqlDB.Close()
}

func (s *SQLiteProvider) Ping(ctx context.Context) error {
	return s.sqlDB.PingContext(ctx)
}

func (s *SQLiteProvider) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := s.sqlDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			row[col] = val
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return results, nil
}

func (s *SQLiteProvider) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := s.sqlDB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return affected, nil
}

func (s *SQLiteProvider) ListTables(ctx context.Context, _ string) ([]db.TableInfo, error) {
	rows, err := s.sqlDB.QueryContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var tables []db.TableInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, db.TableInfo{Name: name})
	}
	return tables, rows.Err()
}

func (s *SQLiteProvider) DescribeTable(ctx context.Context, table string) ([]db.ColumnInfo, error) {
	rows, err := s.sqlDB.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}
	defer rows.Close()

	var columns []db.ColumnInfo
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dfltValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("scan column info: %w", err)
		}

		nullable := "YES"
		if notNull == 1 {
			nullable = "NO"
		}

		defVal := ""
		if dfltValue.Valid {
			defVal = dfltValue.String
		}

		columns = append(columns, db.ColumnInfo{
			Name:       name,
			Type:       colType,
			Nullable:   nullable,
			Default:    defVal,
			PrimaryKey: pk > 0,
		})
	}
	return columns, rows.Err()
}

func (s *SQLiteProvider) ListIndexes(ctx context.Context, table string) ([]db.IndexInfo, error) {
	rows, err := s.sqlDB.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}
	defer rows.Close()

	var indexes []db.IndexInfo
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin, partial string

		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, fmt.Errorf("scan index: %w", err)
		}

		// Get the columns for this index.
		cols, err := s.indexColumns(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("index columns for %q: %w", name, err)
		}

		indexes = append(indexes, db.IndexInfo{
			Name:    name,
			Columns: cols,
			Unique:  unique == 1,
		})
	}
	return indexes, rows.Err()
}

// indexColumns returns the column names for the given index.
func (s *SQLiteProvider) indexColumns(ctx context.Context, indexName string) ([]string, error) {
	rows, err := s.sqlDB.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info(%q)", indexName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var seqno, cid int
		var name sql.NullString
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		if name.Valid {
			cols = append(cols, name.String)
		} else {
			cols = append(cols, fmt.Sprintf("col_%d", cid))
		}
	}
	return cols, rows.Err()
}

func (s *SQLiteProvider) ListConstraints(ctx context.Context, table string) ([]db.ConstraintInfo, error) {
	var constraints []db.ConstraintInfo

	// Detect primary key columns from table_info.
	tiRows, err := s.sqlDB.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("table_info for constraints: %w", err)
	}
	defer tiRows.Close()

	var pkCols []string
	for tiRows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := tiRows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("scan pk info: %w", err)
		}
		if pk > 0 {
			pkCols = append(pkCols, name)
		}
	}
	if err := tiRows.Err(); err != nil {
		return nil, err
	}

	if len(pkCols) > 0 {
		constraints = append(constraints, db.ConstraintInfo{
			Name:    fmt.Sprintf("pk_%s", table),
			Type:    "primary_key",
			Columns: pkCols,
		})
	}

	// Detect foreign keys.
	fkRows, err := s.sqlDB.QueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("foreign_key_list: %w", err)
	}
	defer fkRows.Close()

	// foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
	// Group by id because a single FK can span multiple columns.
	type fkEntry struct {
		refTable   string
		fromCols   []string
		toCols     []string
	}
	fkMap := make(map[int]*fkEntry)
	var fkOrder []int

	for fkRows.Next() {
		var id, seq int
		var refTable, from, to, onUpdate, onDelete, match string
		if err := fkRows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, fmt.Errorf("scan fk: %w", err)
		}
		entry, ok := fkMap[id]
		if !ok {
			entry = &fkEntry{refTable: refTable}
			fkMap[id] = entry
			fkOrder = append(fkOrder, id)
		}
		entry.fromCols = append(entry.fromCols, from)
		entry.toCols = append(entry.toCols, to)
	}
	if err := fkRows.Err(); err != nil {
		return nil, err
	}

	for _, id := range fkOrder {
		entry := fkMap[id]
		constraints = append(constraints, db.ConstraintInfo{
			Name:       fmt.Sprintf("fk_%s_%d", table, id),
			Type:       "foreign_key",
			Columns:    entry.fromCols,
			RefTable:   entry.refTable,
			RefColumns: entry.toCols,
		})
	}

	return constraints, nil
}

func (s *SQLiteProvider) ListViews(ctx context.Context, _ string) ([]db.ViewInfo, error) {
	rows, err := s.sqlDB.QueryContext(ctx,
		"SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list views: %w", err)
	}
	defer rows.Close()

	var views []db.ViewInfo
	for rows.Next() {
		var name string
		var definition sql.NullString
		if err := rows.Scan(&name, &definition); err != nil {
			return nil, fmt.Errorf("scan view: %w", err)
		}
		def := ""
		if definition.Valid {
			def = definition.String
		}
		views = append(views, db.ViewInfo{Name: name, Definition: def})
	}
	return views, rows.Err()
}

func (s *SQLiteProvider) TableSize(ctx context.Context, table string) (*db.TableStats, error) {
	// Row count.
	var rowCount int64
	err := s.sqlDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %q", table)).Scan(&rowCount)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	// Size via dbstat virtual table (may not be available on all builds).
	var sizeBytes int64
	err = s.sqlDB.QueryRowContext(ctx, "SELECT SUM(pgsize) FROM dbstat WHERE name=?", table).Scan(&sizeBytes)
	if err != nil {
		// dbstat extension not available; fall back to 0.
		sizeBytes = 0
	}

	return &db.TableStats{
		RowCount:  rowCount,
		SizeBytes: sizeBytes,
	}, nil
}

func (s *SQLiteProvider) DatabaseStats(ctx context.Context) (*db.DbStats, error) {
	// Count tables.
	var tableCount int
	err := s.sqlDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tableCount)
	if err != nil {
		return nil, fmt.Errorf("count tables: %w", err)
	}

	// Count indexes.
	var indexCount int
	err = s.sqlDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index'").Scan(&indexCount)
	if err != nil {
		return nil, fmt.Errorf("count indexes: %w", err)
	}

	// File size = page_count * page_size.
	var pageCount int64
	err = s.sqlDB.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount)
	if err != nil {
		return nil, fmt.Errorf("page_count: %w", err)
	}

	var pageSize int64
	err = s.sqlDB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize)
	if err != nil {
		return nil, fmt.Errorf("page_size: %w", err)
	}

	// SQLite version.
	var version string
	err = s.sqlDB.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("sqlite_version: %w", err)
	}

	return &db.DbStats{
		SizeBytes:  pageCount * pageSize,
		TableCount: tableCount,
		IndexCount: indexCount,
		Provider:   string(db.ProviderSQLite),
		Version:    version,
	}, nil
}

func (s *SQLiteProvider) CreateTable(ctx context.Context, name string, columns []db.ColumnDef, ifNotExists bool) error {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if ifNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	fmt.Fprintf(&b, "%q (", name)

	for i, col := range columns {
		if i > 0 {
			b.WriteString(", ")
		}

		// Special handling for serial type: INTEGER PRIMARY KEY AUTOINCREMENT.
		if strings.EqualFold(col.Type, "serial") {
			fmt.Fprintf(&b, "%q INTEGER PRIMARY KEY AUTOINCREMENT", col.Name)
			if col.Unique {
				b.WriteString(" UNIQUE")
			}
			if col.References != "" {
				fmt.Fprintf(&b, " REFERENCES %s", col.References)
			}
			continue
		}

		nativeType := db.MapCanonicalType(col.Type, db.ProviderSQLite)
		fmt.Fprintf(&b, "%q %s", col.Name, nativeType)

		if col.PrimaryKey {
			b.WriteString(" PRIMARY KEY")
		}
		if col.AutoIncrement {
			b.WriteString(" AUTOINCREMENT")
		}
		if !col.Nullable && !col.PrimaryKey {
			b.WriteString(" NOT NULL")
		}
		if col.Default != "" {
			fmt.Fprintf(&b, " DEFAULT %s", col.Default)
		}
		if col.Unique {
			b.WriteString(" UNIQUE")
		}
		if col.References != "" {
			fmt.Fprintf(&b, " REFERENCES %s", col.References)
		}
	}

	b.WriteString(")")

	_, err := s.sqlDB.ExecContext(ctx, b.String())
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) AlterTableAdd(ctx context.Context, table string, column db.ColumnDef) error {
	nativeType := db.MapCanonicalType(column.Type, db.ProviderSQLite)

	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %q ADD COLUMN %q %s", table, column.Name, nativeType)

	if !column.Nullable {
		b.WriteString(" NOT NULL")
	}
	if column.Default != "" {
		fmt.Fprintf(&b, " DEFAULT %s", column.Default)
	}
	if column.Unique {
		b.WriteString(" UNIQUE")
	}
	if column.References != "" {
		fmt.Fprintf(&b, " REFERENCES %s", column.References)
	}

	_, err := s.sqlDB.ExecContext(ctx, b.String())
	if err != nil {
		return fmt.Errorf("alter table add column: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) AlterTableDrop(ctx context.Context, table string, columnName string) error {
	query := fmt.Sprintf("ALTER TABLE %q DROP COLUMN %q", table, columnName)
	_, err := s.sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("alter table drop column: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) AlterTableRename(ctx context.Context, table, oldCol, newCol string) error {
	query := fmt.Sprintf("ALTER TABLE %q RENAME COLUMN %q TO %q", table, oldCol, newCol)
	_, err := s.sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("alter table rename column: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) DropTable(ctx context.Context, name string, ifExists bool) error {
	var query string
	if ifExists {
		query = fmt.Sprintf("DROP TABLE IF EXISTS %q", name)
	} else {
		query = fmt.Sprintf("DROP TABLE %q", name)
	}
	_, err := s.sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) CreateIndex(ctx context.Context, table string, index db.IndexDef) error {
	var b strings.Builder
	b.WriteString("CREATE ")
	if index.Unique {
		b.WriteString("UNIQUE ")
	}
	fmt.Fprintf(&b, "INDEX %q ON %q (", index.Name, table)
	for i, col := range index.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q", col)
	}
	b.WriteString(")")

	_, err := s.sqlDB.ExecContext(ctx, b.String())
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) DropIndex(ctx context.Context, _ string, indexName string) error {
	query := fmt.Sprintf("DROP INDEX IF EXISTS %q", indexName)
	_, err := s.sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("drop index: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) CreateView(ctx context.Context, view db.ViewDef) error {
	query := fmt.Sprintf("CREATE VIEW %q AS %s", view.Name, view.Definition)
	_, err := s.sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create view: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) DropView(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP VIEW IF EXISTS %q", name)
	_, err := s.sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("drop view: %w", err)
	}
	return nil
}

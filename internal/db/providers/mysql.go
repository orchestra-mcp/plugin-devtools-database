package providers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
)

// MySQLProvider implements db.Provider for MySQL databases.
type MySQLProvider struct {
	sqlDB *sql.DB
}

func init() {
	db.RegisterProviderFactory("mysql", func(sqlDB *sql.DB) db.Provider {
		return NewMySQL(sqlDB)
	})
}

// NewMySQL creates a new MySQLProvider wrapping the given *sql.DB.
func NewMySQL(sqlDB *sql.DB) *MySQLProvider {
	return &MySQLProvider{sqlDB: sqlDB}
}

func (m *MySQLProvider) Kind() db.ProviderKind {
	return db.ProviderMySQL
}

func (m *MySQLProvider) Close() error {
	return m.sqlDB.Close()
}

func (m *MySQLProvider) Ping(ctx context.Context) error {
	return m.sqlDB.PingContext(ctx)
}

func (m *MySQLProvider) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := m.sqlDB.QueryContext(ctx, query, args...)
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

func (m *MySQLProvider) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := m.sqlDB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return affected, nil
}

func (m *MySQLProvider) ListTables(ctx context.Context, _ string) ([]db.TableInfo, error) {
	// MySQL uses the connected database; schema param is ignored.
	rows, err := m.Query(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	tables := make([]db.TableInfo, 0, len(rows))
	for _, row := range rows {
		// SHOW TABLES returns a single column whose name is "Tables_in_<db>".
		for _, v := range row {
			if name, ok := v.(string); ok {
				tables = append(tables, db.TableInfo{Name: name})
				break
			}
		}
	}
	return tables, nil
}

func (m *MySQLProvider) DescribeTable(ctx context.Context, table string) ([]db.ColumnInfo, error) {
	rows, err := m.Query(ctx, fmt.Sprintf("DESCRIBE %s", quoteIdentMySQL(table)))
	if err != nil {
		return nil, err
	}

	columns := make([]db.ColumnInfo, 0, len(rows))
	for _, row := range rows {
		col := db.ColumnInfo{}
		if v, ok := row["Field"].(string); ok {
			col.Name = v
		}
		if v, ok := row["Type"].(string); ok {
			col.Type = v
		}
		if v, ok := row["Null"].(string); ok {
			col.Nullable = v
		}
		if v, ok := row["Default"]; ok && v != nil {
			col.Default = fmt.Sprintf("%v", v)
		}
		if v, ok := row["Key"].(string); ok {
			col.PrimaryKey = v == "PRI"
		}
		if v, ok := row["Extra"].(string); ok {
			col.Extra = v
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func (m *MySQLProvider) ListIndexes(ctx context.Context, table string) ([]db.IndexInfo, error) {
	rows, err := m.Query(ctx, fmt.Sprintf("SHOW INDEX FROM %s", quoteIdentMySQL(table)))
	if err != nil {
		return nil, err
	}

	// Group rows by index name since multi-column indexes produce one row per column.
	type indexData struct {
		Name    string
		Columns []string
		Unique  bool
	}
	grouped := make(map[string]*indexData)
	var order []string

	for _, row := range rows {
		name, _ := row["Key_name"].(string)
		if name == "" {
			continue
		}
		id, exists := grouped[name]
		if !exists {
			nonUnique := toInt64MySQL(row["Non_unique"])
			id = &indexData{
				Name:   name,
				Unique: nonUnique == 0,
			}
			grouped[name] = id
			order = append(order, name)
		}
		if col, ok := row["Column_name"].(string); ok && col != "" {
			id.Columns = append(id.Columns, col)
		}
	}

	indexes := make([]db.IndexInfo, 0, len(order))
	for _, name := range order {
		id := grouped[name]
		indexes = append(indexes, db.IndexInfo{
			Name:    id.Name,
			Columns: id.Columns,
			Unique:  id.Unique,
		})
	}
	return indexes, nil
}

func (m *MySQLProvider) ListConstraints(ctx context.Context, table string) ([]db.ConstraintInfo, error) {
	rows, err := m.Query(ctx,
		`SELECT tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE,
		        kcu.COLUMN_NAME,
		        kcu.REFERENCED_TABLE_NAME,
		        kcu.REFERENCED_COLUMN_NAME
		 FROM information_schema.TABLE_CONSTRAINTS tc
		 LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
		   ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		   AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		   AND tc.TABLE_NAME = kcu.TABLE_NAME
		 WHERE tc.TABLE_NAME = ?
		   AND tc.TABLE_SCHEMA = DATABASE()
		 ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`, table)
	if err != nil {
		return nil, err
	}

	// Group by constraint name.
	type constraintData struct {
		Name       string
		Type       string
		Columns    []string
		RefTable   string
		RefColumns []string
	}
	grouped := make(map[string]*constraintData)
	var order []string

	for _, row := range rows {
		name, _ := row["CONSTRAINT_NAME"].(string)
		if name == "" {
			continue
		}
		cd, exists := grouped[name]
		if !exists {
			ctype, _ := row["CONSTRAINT_TYPE"].(string)
			cd = &constraintData{
				Name: name,
				Type: mapConstraintTypeMySQL(ctype),
			}
			grouped[name] = cd
			order = append(order, name)
		}
		if col, ok := row["COLUMN_NAME"].(string); ok && col != "" {
			if !containsStrMySQL(cd.Columns, col) {
				cd.Columns = append(cd.Columns, col)
			}
		}
		if refTable, ok := row["REFERENCED_TABLE_NAME"].(string); ok && refTable != "" {
			cd.RefTable = refTable
		}
		if refCol, ok := row["REFERENCED_COLUMN_NAME"].(string); ok && refCol != "" {
			if !containsStrMySQL(cd.RefColumns, refCol) {
				cd.RefColumns = append(cd.RefColumns, refCol)
			}
		}
	}

	constraints := make([]db.ConstraintInfo, 0, len(order))
	for _, name := range order {
		cd := grouped[name]
		constraints = append(constraints, db.ConstraintInfo{
			Name:       cd.Name,
			Type:       cd.Type,
			Columns:    cd.Columns,
			RefTable:   cd.RefTable,
			RefColumns: cd.RefColumns,
		})
	}
	return constraints, nil
}

// mapConstraintTypeMySQL converts a MySQL constraint_type to a normalized string.
func mapConstraintTypeMySQL(mysqlType string) string {
	switch strings.ToUpper(mysqlType) {
	case "PRIMARY KEY":
		return "primary_key"
	case "FOREIGN KEY":
		return "foreign_key"
	case "UNIQUE":
		return "unique"
	case "CHECK":
		return "check"
	default:
		return strings.ToLower(mysqlType)
	}
}

// containsStrMySQL checks if a slice contains a string.
func containsStrMySQL(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func (m *MySQLProvider) ListViews(ctx context.Context, _ string) ([]db.ViewInfo, error) {
	rows, err := m.Query(ctx,
		"SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = DATABASE()")
	if err != nil {
		return nil, err
	}
	views := make([]db.ViewInfo, 0, len(rows))
	for _, row := range rows {
		v := db.ViewInfo{}
		if name, ok := row["TABLE_NAME"].(string); ok {
			v.Name = name
		}
		if def, ok := row["VIEW_DEFINITION"].(string); ok {
			v.Definition = strings.TrimSpace(def)
		}
		views = append(views, v)
	}
	return views, nil
}

func (m *MySQLProvider) TableSize(ctx context.Context, table string) (*db.TableStats, error) {
	rows, err := m.Query(ctx,
		`SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, DATA_LENGTH + INDEX_LENGTH AS TOTAL
		 FROM information_schema.TABLES
		 WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()`, table)
	if err != nil {
		return nil, fmt.Errorf("table size: %w", err)
	}

	stats := &db.TableStats{}
	if len(rows) > 0 {
		row := rows[0]
		stats.RowCount = toInt64MySQL(row["TABLE_ROWS"])
		stats.SizeBytes = toInt64MySQL(row["DATA_LENGTH"])
		stats.IndexSize = toInt64MySQL(row["INDEX_LENGTH"])
		stats.TotalSize = toInt64MySQL(row["TOTAL"])
	}
	return stats, nil
}

func (m *MySQLProvider) DatabaseStats(ctx context.Context) (*db.DbStats, error) {
	stats := &db.DbStats{
		Provider: string(db.ProviderMySQL),
	}

	// Total size from information_schema.
	sizeRows, err := m.Query(ctx,
		`SELECT SUM(DATA_LENGTH + INDEX_LENGTH) AS total_size
		 FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE()`)
	if err == nil && len(sizeRows) > 0 {
		stats.SizeBytes = toInt64MySQL(sizeRows[0]["total_size"])
	}

	// Table count.
	tblRows, err := m.Query(ctx,
		`SELECT COUNT(*) AS cnt
		 FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`)
	if err == nil && len(tblRows) > 0 {
		stats.TableCount = int(toInt64MySQL(tblRows[0]["cnt"]))
	}

	// Index count.
	idxRows, err := m.Query(ctx,
		`SELECT COUNT(DISTINCT INDEX_NAME) AS cnt
		 FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = DATABASE()`)
	if err == nil && len(idxRows) > 0 {
		stats.IndexCount = int(toInt64MySQL(idxRows[0]["cnt"]))
	}

	// Version.
	verRows, err := m.Query(ctx, "SELECT VERSION() AS ver")
	if err == nil && len(verRows) > 0 {
		if v, ok := verRows[0]["ver"].(string); ok {
			stats.Version = v
		}
	}

	return stats, nil
}

func (m *MySQLProvider) CreateTable(ctx context.Context, name string, columns []db.ColumnDef, ifNotExists bool) error {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if ifNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(quoteIdentMySQL(name))
	b.WriteString(" (\n")

	var pkColumns []string

	for i, col := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  ")
		b.WriteString(quoteIdentMySQL(col.Name))
		b.WriteByte(' ')

		nativeType := db.MapCanonicalType(col.Type, db.ProviderMySQL)

		// MySQL AUTO_INCREMENT requires the column to be a key.
		isAutoInc := strings.Contains(strings.ToUpper(nativeType), "AUTO_INCREMENT")

		b.WriteString(nativeType)

		if !col.Nullable && !isAutoInc {
			b.WriteString(" NOT NULL")
		}

		if col.Unique && !col.PrimaryKey {
			b.WriteString(" UNIQUE")
		}

		if col.Default != "" && !isAutoInc {
			b.WriteString(" DEFAULT ")
			b.WriteString(col.Default)
		}

		if col.PrimaryKey || isAutoInc {
			pkColumns = append(pkColumns, col.Name)
		}

		if col.References != "" {
			// Foreign key references are added inline via REFERENCES syntax in MySQL 8.0+.
			b.WriteString(" REFERENCES ")
			b.WriteString(col.References)
		}
	}

	// Add PRIMARY KEY constraint at the end if there are primary key columns.
	if len(pkColumns) > 0 {
		b.WriteString(",\n  PRIMARY KEY (")
		for i, pk := range pkColumns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteIdentMySQL(pk))
		}
		b.WriteByte(')')
	}

	b.WriteString("\n)")
	_, err := m.Exec(ctx, b.String())
	return err
}

func (m *MySQLProvider) AlterTableAdd(ctx context.Context, table string, column db.ColumnDef) error {
	nativeType := db.MapCanonicalType(column.Type, db.ProviderMySQL)
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s ADD COLUMN %s %s",
		quoteIdentMySQL(table), quoteIdentMySQL(column.Name), nativeType)

	if !column.Nullable {
		b.WriteString(" NOT NULL")
	}
	if column.Unique {
		b.WriteString(" UNIQUE")
	}
	if column.Default != "" {
		b.WriteString(" DEFAULT ")
		b.WriteString(column.Default)
	}
	if column.References != "" {
		b.WriteString(" REFERENCES ")
		b.WriteString(column.References)
	}

	_, err := m.Exec(ctx, b.String())
	return err
}

func (m *MySQLProvider) AlterTableDrop(ctx context.Context, table string, columnName string) error {
	q := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
		quoteIdentMySQL(table), quoteIdentMySQL(columnName))
	_, err := m.Exec(ctx, q)
	return err
}

func (m *MySQLProvider) AlterTableRename(ctx context.Context, table, oldCol, newCol string) error {
	// MySQL 8.0+ supports RENAME COLUMN syntax.
	q := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		quoteIdentMySQL(table), quoteIdentMySQL(oldCol), quoteIdentMySQL(newCol))
	_, err := m.Exec(ctx, q)
	return err
}

func (m *MySQLProvider) DropTable(ctx context.Context, name string, ifExists bool) error {
	var b strings.Builder
	b.WriteString("DROP TABLE ")
	if ifExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(quoteIdentMySQL(name))

	_, err := m.Exec(ctx, b.String())
	return err
}

func (m *MySQLProvider) CreateIndex(ctx context.Context, table string, index db.IndexDef) error {
	var b strings.Builder
	b.WriteString("CREATE ")
	if index.Unique {
		b.WriteString("UNIQUE ")
	}
	b.WriteString("INDEX ")
	b.WriteString(quoteIdentMySQL(index.Name))
	b.WriteString(" ON ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" (")
	for i, col := range index.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdentMySQL(col))
	}
	b.WriteByte(')')

	_, err := m.Exec(ctx, b.String())
	return err
}

func (m *MySQLProvider) DropIndex(ctx context.Context, table, indexName string) error {
	// MySQL requires the table name for DROP INDEX.
	q := fmt.Sprintf("DROP INDEX %s ON %s", quoteIdentMySQL(indexName), quoteIdentMySQL(table))
	_, err := m.Exec(ctx, q)
	return err
}

func (m *MySQLProvider) CreateView(ctx context.Context, view db.ViewDef) error {
	q := fmt.Sprintf("CREATE VIEW %s AS %s", quoteIdentMySQL(view.Name), view.Definition)
	_, err := m.Exec(ctx, q)
	return err
}

func (m *MySQLProvider) DropView(ctx context.Context, name string) error {
	q := fmt.Sprintf("DROP VIEW IF EXISTS %s", quoteIdentMySQL(name))
	_, err := m.Exec(ctx, q)
	return err
}

// quoteIdentMySQL quotes a MySQL identifier with backticks,
// escaping any embedded backtick characters.
func quoteIdentMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// toInt64MySQL converts an interface value to int64, handling common MySQL driver types.
func toInt64MySQL(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	case string:
		return 0
	default:
		return 0
	}
}

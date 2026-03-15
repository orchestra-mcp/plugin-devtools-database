package providers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
)

// PostgresProvider implements db.Provider for PostgreSQL databases.
type PostgresProvider struct {
	sqlDB *sql.DB
}

func init() {
	db.RegisterProviderFactory("postgres", func(sqlDB *sql.DB) db.Provider {
		return NewPostgres(sqlDB)
	})
}

// NewPostgres creates a new PostgresProvider wrapping the given *sql.DB.
func NewPostgres(sqlDB *sql.DB) *PostgresProvider {
	return &PostgresProvider{sqlDB: sqlDB}
}

func (p *PostgresProvider) Kind() db.ProviderKind {
	return db.ProviderPostgres
}

func (p *PostgresProvider) Close() error {
	return p.sqlDB.Close()
}

func (p *PostgresProvider) Ping(ctx context.Context) error {
	return p.sqlDB.PingContext(ctx)
}

func (p *PostgresProvider) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	rows, err := p.sqlDB.QueryContext(ctx, query, args...)
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

func (p *PostgresProvider) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := p.sqlDB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return affected, nil
}

func (p *PostgresProvider) ListTables(ctx context.Context, schema string) ([]db.TableInfo, error) {
	if schema == "" {
		schema = "public"
	}
	rows, err := p.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename", schema)
	if err != nil {
		return nil, err
	}
	tables := make([]db.TableInfo, 0, len(rows))
	for _, row := range rows {
		if name, ok := row["tablename"].(string); ok {
			tables = append(tables, db.TableInfo{Name: name})
		}
	}
	return tables, nil
}

func (p *PostgresProvider) DescribeTable(ctx context.Context, table string) ([]db.ColumnInfo, error) {
	rows, err := p.Query(ctx,
		`SELECT column_name, data_type, is_nullable, column_default
		 FROM information_schema.columns
		 WHERE table_name = $1 AND table_schema = 'public'
		 ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}

	// Fetch primary key columns.
	pkCols := make(map[string]bool)
	pkRows, err := p.Query(ctx,
		`SELECT kcu.column_name
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON tc.constraint_name = kcu.constraint_name
		   AND tc.table_schema = kcu.table_schema
		 WHERE tc.table_name = $1
		   AND tc.table_schema = 'public'
		   AND tc.constraint_type = 'PRIMARY KEY'`, table)
	if err == nil {
		for _, pkRow := range pkRows {
			if col, ok := pkRow["column_name"].(string); ok {
				pkCols[col] = true
			}
		}
	}

	columns := make([]db.ColumnInfo, 0, len(rows))
	for _, row := range rows {
		col := db.ColumnInfo{}
		if v, ok := row["column_name"].(string); ok {
			col.Name = v
		}
		if v, ok := row["data_type"].(string); ok {
			col.Type = v
		}
		if v, ok := row["is_nullable"].(string); ok {
			col.Nullable = v
		}
		if v, ok := row["column_default"]; ok && v != nil {
			col.Default = fmt.Sprintf("%v", v)
		}
		col.PrimaryKey = pkCols[col.Name]
		columns = append(columns, col)
	}
	return columns, nil
}

func (p *PostgresProvider) ListIndexes(ctx context.Context, table string) ([]db.IndexInfo, error) {
	rows, err := p.Query(ctx,
		"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = $1", table)
	if err != nil {
		return nil, err
	}

	indexes := make([]db.IndexInfo, 0, len(rows))
	for _, row := range rows {
		info := db.IndexInfo{}
		if v, ok := row["indexname"].(string); ok {
			info.Name = v
		}
		if v, ok := row["indexdef"].(string); ok {
			info.Columns = parseIndexColumns(v)
			info.Unique = strings.Contains(strings.ToUpper(v), "UNIQUE")
		}
		indexes = append(indexes, info)
	}
	return indexes, nil
}

// parseIndexColumns extracts column names from a PostgreSQL CREATE INDEX definition.
// Example: "CREATE UNIQUE INDEX idx ON tbl USING btree (col1, col2)" -> ["col1", "col2"]
func parseIndexColumns(indexdef string) []string {
	start := strings.LastIndex(indexdef, "(")
	end := strings.LastIndex(indexdef, ")")
	if start < 0 || end < 0 || end <= start {
		return nil
	}
	colStr := indexdef[start+1 : end]
	parts := strings.Split(colStr, ",")
	cols := make([]string, 0, len(parts))
	for _, part := range parts {
		col := strings.TrimSpace(part)
		// Strip sort direction and other qualifiers.
		if spaceIdx := strings.IndexByte(col, ' '); spaceIdx > 0 {
			col = col[:spaceIdx]
		}
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

func (p *PostgresProvider) ListConstraints(ctx context.Context, table string) ([]db.ConstraintInfo, error) {
	rows, err := p.Query(ctx,
		`SELECT tc.constraint_name, tc.constraint_type,
		        kcu.column_name,
		        ccu.table_name AS ref_table,
		        ccu.column_name AS ref_column
		 FROM information_schema.table_constraints tc
		 LEFT JOIN information_schema.key_column_usage kcu
		   ON tc.constraint_name = kcu.constraint_name
		   AND tc.table_schema = kcu.table_schema
		 LEFT JOIN information_schema.constraint_column_usage ccu
		   ON tc.constraint_name = ccu.constraint_name
		   AND tc.table_schema = ccu.table_schema
		 WHERE tc.table_name = $1
		   AND tc.table_schema = 'public'
		 ORDER BY tc.constraint_name, kcu.ordinal_position`, table)
	if err != nil {
		return nil, err
	}

	// Group by constraint name since a multi-column constraint produces multiple rows.
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
		name, _ := row["constraint_name"].(string)
		if name == "" {
			continue
		}
		cd, exists := grouped[name]
		if !exists {
			ctype, _ := row["constraint_type"].(string)
			cd = &constraintData{
				Name: name,
				Type: mapConstraintType(ctype),
			}
			grouped[name] = cd
			order = append(order, name)
		}
		if col, ok := row["column_name"].(string); ok && col != "" {
			if !containsStr(cd.Columns, col) {
				cd.Columns = append(cd.Columns, col)
			}
		}
		if refTable, ok := row["ref_table"].(string); ok && refTable != "" && refTable != table {
			cd.RefTable = refTable
		}
		if refCol, ok := row["ref_column"].(string); ok && refCol != "" {
			if cd.Type == "foreign_key" && !containsStr(cd.RefColumns, refCol) {
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

// mapConstraintType converts a PostgreSQL constraint_type to a normalized string.
func mapConstraintType(pgType string) string {
	switch strings.ToUpper(pgType) {
	case "PRIMARY KEY":
		return "primary_key"
	case "FOREIGN KEY":
		return "foreign_key"
	case "UNIQUE":
		return "unique"
	case "CHECK":
		return "check"
	default:
		return strings.ToLower(pgType)
	}
}

// containsStr checks if a slice contains a string.
func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func (p *PostgresProvider) ListViews(ctx context.Context, schema string) ([]db.ViewInfo, error) {
	if schema == "" {
		schema = "public"
	}
	rows, err := p.Query(ctx,
		"SELECT viewname, definition FROM pg_views WHERE schemaname = $1", schema)
	if err != nil {
		return nil, err
	}
	views := make([]db.ViewInfo, 0, len(rows))
	for _, row := range rows {
		v := db.ViewInfo{}
		if name, ok := row["viewname"].(string); ok {
			v.Name = name
		}
		if def, ok := row["definition"].(string); ok {
			v.Definition = strings.TrimSpace(def)
		}
		views = append(views, v)
	}
	return views, nil
}

func (p *PostgresProvider) TableSize(ctx context.Context, table string) (*db.TableStats, error) {
	sizeRows, err := p.Query(ctx,
		`SELECT pg_relation_size($1) AS size,
		        pg_indexes_size($1) AS index_size,
		        pg_total_relation_size($1) AS total_size`, table)
	if err != nil {
		return nil, fmt.Errorf("table size: %w", err)
	}

	stats := &db.TableStats{}
	if len(sizeRows) > 0 {
		row := sizeRows[0]
		stats.SizeBytes = toInt64(row["size"])
		stats.IndexSize = toInt64(row["index_size"])
		stats.TotalSize = toInt64(row["total_size"])
	}

	// Row count via a separate query (the table name must be sanitized).
	countRows, err := p.Query(ctx, fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s", quoteIdentPG(table)))
	if err == nil && len(countRows) > 0 {
		stats.RowCount = toInt64(countRows[0]["cnt"])
	}

	return stats, nil
}

func (p *PostgresProvider) DatabaseStats(ctx context.Context) (*db.DbStats, error) {
	stats := &db.DbStats{
		Provider: string(db.ProviderPostgres),
	}

	// Database size.
	sizeRows, err := p.Query(ctx, "SELECT pg_database_size(current_database()) AS size")
	if err == nil && len(sizeRows) > 0 {
		stats.SizeBytes = toInt64(sizeRows[0]["size"])
	}

	// Table count.
	tblRows, err := p.Query(ctx,
		"SELECT COUNT(*) AS cnt FROM pg_tables WHERE schemaname = 'public'")
	if err == nil && len(tblRows) > 0 {
		stats.TableCount = int(toInt64(tblRows[0]["cnt"]))
	}

	// Index count.
	idxRows, err := p.Query(ctx,
		"SELECT COUNT(*) AS cnt FROM pg_indexes WHERE schemaname = 'public'")
	if err == nil && len(idxRows) > 0 {
		stats.IndexCount = int(toInt64(idxRows[0]["cnt"]))
	}

	// Version.
	verRows, err := p.Query(ctx, "SELECT version()")
	if err == nil && len(verRows) > 0 {
		for _, v := range verRows[0] {
			if s, ok := v.(string); ok {
				stats.Version = s
				break
			}
		}
	}

	return stats, nil
}

func (p *PostgresProvider) CreateTable(ctx context.Context, name string, columns []db.ColumnDef, ifNotExists bool) error {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if ifNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(quoteIdentPG(name))
	b.WriteString(" (\n")

	for i, col := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  ")
		b.WriteString(quoteIdentPG(col.Name))
		b.WriteByte(' ')

		nativeType := db.MapCanonicalType(col.Type, db.ProviderPostgres)

		// SERIAL is a PostgreSQL shorthand that implies NOT NULL and auto-increment.
		isSerial := strings.ToUpper(nativeType) == "SERIAL"
		b.WriteString(nativeType)

		if col.PrimaryKey && !isSerial {
			b.WriteString(" PRIMARY KEY")
		} else if col.PrimaryKey && isSerial {
			b.WriteString(" PRIMARY KEY")
		}

		if !col.Nullable && !col.PrimaryKey && !isSerial {
			b.WriteString(" NOT NULL")
		}

		if col.Unique && !col.PrimaryKey {
			b.WriteString(" UNIQUE")
		}

		if col.Default != "" {
			b.WriteString(" DEFAULT ")
			b.WriteString(col.Default)
		}

		if col.References != "" {
			b.WriteString(" REFERENCES ")
			b.WriteString(col.References)
		}
	}

	b.WriteString("\n)")
	_, err := p.Exec(ctx, b.String())
	return err
}

func (p *PostgresProvider) AlterTableAdd(ctx context.Context, table string, column db.ColumnDef) error {
	nativeType := db.MapCanonicalType(column.Type, db.ProviderPostgres)
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s ADD COLUMN %s %s",
		quoteIdentPG(table), quoteIdentPG(column.Name), nativeType)

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

	_, err := p.Exec(ctx, b.String())
	return err
}

func (p *PostgresProvider) AlterTableDrop(ctx context.Context, table string, columnName string) error {
	q := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
		quoteIdentPG(table), quoteIdentPG(columnName))
	_, err := p.Exec(ctx, q)
	return err
}

func (p *PostgresProvider) AlterTableRename(ctx context.Context, table, oldCol, newCol string) error {
	q := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		quoteIdentPG(table), quoteIdentPG(oldCol), quoteIdentPG(newCol))
	_, err := p.Exec(ctx, q)
	return err
}

func (p *PostgresProvider) DropTable(ctx context.Context, name string, ifExists bool) error {
	var b strings.Builder
	b.WriteString("DROP TABLE ")
	if ifExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(quoteIdentPG(name))

	_, err := p.Exec(ctx, b.String())
	return err
}

func (p *PostgresProvider) CreateIndex(ctx context.Context, table string, index db.IndexDef) error {
	var b strings.Builder
	b.WriteString("CREATE ")
	if index.Unique {
		b.WriteString("UNIQUE ")
	}
	b.WriteString("INDEX ")
	b.WriteString(quoteIdentPG(index.Name))
	b.WriteString(" ON ")
	b.WriteString(quoteIdentPG(table))
	b.WriteString(" (")
	for i, col := range index.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdentPG(col))
	}
	b.WriteByte(')')

	_, err := p.Exec(ctx, b.String())
	return err
}

func (p *PostgresProvider) DropIndex(ctx context.Context, _ string, indexName string) error {
	// PostgreSQL indexes are schema-scoped, not table-scoped; table param is ignored.
	q := fmt.Sprintf("DROP INDEX IF EXISTS %s", quoteIdentPG(indexName))
	_, err := p.Exec(ctx, q)
	return err
}

func (p *PostgresProvider) CreateView(ctx context.Context, view db.ViewDef) error {
	q := fmt.Sprintf("CREATE VIEW %s AS %s", quoteIdentPG(view.Name), view.Definition)
	_, err := p.Exec(ctx, q)
	return err
}

func (p *PostgresProvider) DropView(ctx context.Context, name string) error {
	q := fmt.Sprintf("DROP VIEW IF EXISTS %s", quoteIdentPG(name))
	_, err := p.Exec(ctx, q)
	return err
}

// ---------------------------------------------------------------------------
// Advanced PostgreSQL-Specific Methods
// ---------------------------------------------------------------------------

// --- Maintenance ---

// Vacuum runs VACUUM on a specific table or the entire database.
func (p *PostgresProvider) Vacuum(ctx context.Context, table string, analyze, full bool) error {
	var b strings.Builder
	b.WriteString("VACUUM")
	if full {
		b.WriteString(" FULL")
	}
	if analyze {
		b.WriteString(" ANALYZE")
	}
	if table != "" {
		b.WriteByte(' ')
		b.WriteString(quoteIdentPG(table))
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// Analyze runs ANALYZE on a specific table or the entire database.
func (p *PostgresProvider) Analyze(ctx context.Context, table string, verbose bool) error {
	var b strings.Builder
	b.WriteString("ANALYZE")
	if verbose {
		b.WriteString(" VERBOSE")
	}
	if table != "" {
		b.WriteByte(' ')
		b.WriteString(quoteIdentPG(table))
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// Reindex rebuilds an index, table indexes, or entire database indexes.
func (p *PostgresProvider) Reindex(ctx context.Context, target, name string, concurrent bool) error {
	var b strings.Builder
	b.WriteString("REINDEX")
	if concurrent {
		b.WriteString(" (CONCURRENTLY)")
	}
	switch strings.ToLower(target) {
	case "table":
		b.WriteString(" TABLE ")
	case "index":
		b.WriteString(" INDEX ")
	case "database":
		b.WriteString(" DATABASE ")
	default:
		return fmt.Errorf("invalid reindex target %q (expected table, index, or database)", target)
	}
	b.WriteString(quoteIdentPG(name))
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// Cluster re-orders a table on disk based on an index.
func (p *PostgresProvider) Cluster(ctx context.Context, table, index string) error {
	var b strings.Builder
	b.WriteString("CLUSTER")
	if table != "" {
		b.WriteByte(' ')
		b.WriteString(quoteIdentPG(table))
		if index != "" {
			b.WriteString(" USING ")
			b.WriteString(quoteIdentPG(index))
		}
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// --- Schema Management ---

// CreateSchema creates a new PostgreSQL schema.
func (p *PostgresProvider) CreateSchema(ctx context.Context, name, authorization string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE SCHEMA IF NOT EXISTS %s", quoteIdentPG(name))
	if authorization != "" {
		fmt.Fprintf(&b, " AUTHORIZATION %s", quoteIdentPG(authorization))
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// SchemaInfo holds metadata about a PostgreSQL schema.
type SchemaInfo struct {
	Name    string
	Owner   string
	Default bool
}

// ListSchemas returns all schemas in the database.
func (p *PostgresProvider) ListSchemas(ctx context.Context) ([]SchemaInfo, error) {
	rows, err := p.Query(ctx,
		`SELECT nspname AS name, pg_catalog.pg_get_userbyid(nspowner) AS owner
		 FROM pg_catalog.pg_namespace
		 WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'
		 ORDER BY nspname`)
	if err != nil {
		return nil, err
	}
	schemas := make([]SchemaInfo, 0, len(rows))
	for _, row := range rows {
		s := SchemaInfo{}
		if v, ok := row["name"].(string); ok {
			s.Name = v
		}
		if v, ok := row["owner"].(string); ok {
			s.Owner = v
		}
		schemas = append(schemas, s)
	}
	return schemas, nil
}

// DropSchema drops a PostgreSQL schema.
func (p *PostgresProvider) DropSchema(ctx context.Context, name string, cascade bool) error {
	var b strings.Builder
	fmt.Fprintf(&b, "DROP SCHEMA IF EXISTS %s", quoteIdentPG(name))
	if cascade {
		b.WriteString(" CASCADE")
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// SetSearchPath sets the PostgreSQL search_path for the current session.
func (p *PostgresProvider) SetSearchPath(ctx context.Context, schemas []string) error {
	quoted := make([]string, len(schemas))
	for i, s := range schemas {
		quoted[i] = quoteIdentPG(s)
	}
	q := fmt.Sprintf("SET search_path TO %s", strings.Join(quoted, ", "))
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// GetSearchPath returns the current search_path setting.
func (p *PostgresProvider) GetSearchPath(ctx context.Context) (string, error) {
	rows, err := p.Query(ctx, "SHOW search_path")
	if err != nil {
		return "", err
	}
	if len(rows) > 0 {
		if v, ok := rows[0]["search_path"].(string); ok {
			return v, nil
		}
	}
	return "", nil
}

// --- Partitioning ---

// CreatePartitionedTable creates a partitioned table.
func (p *PostgresProvider) CreatePartitionedTable(ctx context.Context, name string, columns []db.ColumnDef, partitionBy, partitionKey string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", quoteIdentPG(name))
	for i, col := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  ")
		b.WriteString(quoteIdentPG(col.Name))
		b.WriteByte(' ')
		b.WriteString(db.MapCanonicalType(col.Type, db.ProviderPostgres))
		if col.PrimaryKey {
			b.WriteString(" PRIMARY KEY")
		}
		if !col.Nullable && !col.PrimaryKey {
			b.WriteString(" NOT NULL")
		}
		if col.Default != "" {
			b.WriteString(" DEFAULT ")
			b.WriteString(col.Default)
		}
	}
	b.WriteString("\n)")

	strategy := strings.ToUpper(partitionBy)
	if strategy != "RANGE" && strategy != "LIST" && strategy != "HASH" {
		return fmt.Errorf("invalid partition strategy %q (expected RANGE, LIST, or HASH)", partitionBy)
	}
	fmt.Fprintf(&b, " PARTITION BY %s (%s)", strategy, quoteIdentPG(partitionKey))

	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// PartitionInfo holds metadata about a table partition.
type PartitionInfo struct {
	Name       string
	Parent     string
	Expression string
}

// CreatePartition creates a partition of a parent table.
func (p *PostgresProvider) CreatePartition(ctx context.Context, parent, name, bound string) error {
	q := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s",
		quoteIdentPG(name), quoteIdentPG(parent), bound)
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// ListPartitions lists partitions of a table.
func (p *PostgresProvider) ListPartitions(ctx context.Context, table string) ([]PartitionInfo, error) {
	rows, err := p.Query(ctx,
		`SELECT c.relname AS partition_name,
		        pg_get_expr(c.relpartbound, c.oid) AS partition_expression
		 FROM pg_inherits i
		 JOIN pg_class c ON c.oid = i.inhrelid
		 JOIN pg_class parent ON parent.oid = i.inhparent
		 WHERE parent.relname = $1
		 ORDER BY c.relname`, table)
	if err != nil {
		return nil, err
	}
	partitions := make([]PartitionInfo, 0, len(rows))
	for _, row := range rows {
		p := PartitionInfo{Parent: table}
		if v, ok := row["partition_name"].(string); ok {
			p.Name = v
		}
		if v, ok := row["partition_expression"].(string); ok {
			p.Expression = v
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}

// DetachPartition detaches a partition from its parent table.
func (p *PostgresProvider) DetachPartition(ctx context.Context, parent, partition string, concurrent bool) error {
	var b strings.Builder
	fmt.Fprintf(&b, "ALTER TABLE %s DETACH PARTITION %s",
		quoteIdentPG(parent), quoteIdentPG(partition))
	if concurrent {
		b.WriteString(" CONCURRENTLY")
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// --- Row-Level Security ---

// EnableRLS enables row-level security on a table.
func (p *PostgresProvider) EnableRLS(ctx context.Context, table string, force bool) error {
	q := fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", quoteIdentPG(table))
	if _, err := p.sqlDB.ExecContext(ctx, q); err != nil {
		return err
	}
	if force {
		q = fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", quoteIdentPG(table))
		_, err := p.sqlDB.ExecContext(ctx, q)
		return err
	}
	return nil
}

// DisableRLS disables row-level security on a table.
func (p *PostgresProvider) DisableRLS(ctx context.Context, table string) error {
	q := fmt.Sprintf("ALTER TABLE %s DISABLE ROW LEVEL SECURITY", quoteIdentPG(table))
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// PolicyDef defines a Row-Level Security policy.
type PolicyDef struct {
	Table      string
	Name       string
	Permissive bool
	Command    string // ALL, SELECT, INSERT, UPDATE, DELETE
	Roles      []string
	Using      string
	WithCheck  string
}

// CreatePolicy creates a Row-Level Security policy.
func (p *PostgresProvider) CreatePolicy(ctx context.Context, pol PolicyDef) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE POLICY %s ON %s",
		quoteIdentPG(pol.Name), quoteIdentPG(pol.Table))
	if pol.Permissive {
		b.WriteString(" AS PERMISSIVE")
	} else {
		b.WriteString(" AS RESTRICTIVE")
	}
	if pol.Command != "" && strings.ToUpper(pol.Command) != "ALL" {
		fmt.Fprintf(&b, " FOR %s", strings.ToUpper(pol.Command))
	}
	if len(pol.Roles) > 0 {
		fmt.Fprintf(&b, " TO %s", strings.Join(pol.Roles, ", "))
	}
	if pol.Using != "" {
		fmt.Fprintf(&b, " USING (%s)", pol.Using)
	}
	if pol.WithCheck != "" {
		fmt.Fprintf(&b, " WITH CHECK (%s)", pol.WithCheck)
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// PolicyInfo holds metadata about an RLS policy.
type PolicyInfo struct {
	Name       string
	Table      string
	Permissive string
	Roles      string
	Command    string
	Using      string
	WithCheck  string
}

// ListPolicies lists RLS policies, optionally filtered by table.
func (p *PostgresProvider) ListPolicies(ctx context.Context, table string) ([]PolicyInfo, error) {
	var q string
	var args []any
	if table != "" {
		q = `SELECT policyname, tablename, permissive, roles::text, cmd, qual, with_check
		     FROM pg_policies WHERE tablename = $1 ORDER BY policyname`
		args = []any{table}
	} else {
		q = `SELECT policyname, tablename, permissive, roles::text, cmd, qual, with_check
		     FROM pg_policies ORDER BY tablename, policyname`
	}
	rows, err := p.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	policies := make([]PolicyInfo, 0, len(rows))
	for _, row := range rows {
		pol := PolicyInfo{}
		if v, ok := row["policyname"].(string); ok {
			pol.Name = v
		}
		if v, ok := row["tablename"].(string); ok {
			pol.Table = v
		}
		if v, ok := row["permissive"].(string); ok {
			pol.Permissive = v
		}
		if v, ok := row["roles"].(string); ok {
			pol.Roles = v
		}
		if v, ok := row["cmd"].(string); ok {
			pol.Command = v
		}
		if v, ok := row["qual"]; ok && v != nil {
			pol.Using = fmt.Sprintf("%v", v)
		}
		if v, ok := row["with_check"]; ok && v != nil {
			pol.WithCheck = fmt.Sprintf("%v", v)
		}
		policies = append(policies, pol)
	}
	return policies, nil
}

// --- Replication ---

// ReplicationStat holds replication status information.
type ReplicationStat struct {
	PID       int64
	UserName  string
	AppName   string
	ClientIP  string
	State     string
	SentLSN   string
	WriteLSN  string
	FlushLSN  string
	ReplayLSN string
}

// ReplicationStatus returns pg_stat_replication data.
func (p *PostgresProvider) ReplicationStatus(ctx context.Context) ([]ReplicationStat, error) {
	rows, err := p.Query(ctx,
		`SELECT pid, usename, application_name, client_addr::text,
		        state, sent_lsn::text, write_lsn::text, flush_lsn::text, replay_lsn::text
		 FROM pg_stat_replication ORDER BY pid`)
	if err != nil {
		return nil, err
	}
	stats := make([]ReplicationStat, 0, len(rows))
	for _, row := range rows {
		s := ReplicationStat{}
		s.PID = toInt64(row["pid"])
		if v, ok := row["usename"].(string); ok {
			s.UserName = v
		}
		if v, ok := row["application_name"].(string); ok {
			s.AppName = v
		}
		if v, ok := row["client_addr"].(string); ok {
			s.ClientIP = v
		}
		if v, ok := row["state"].(string); ok {
			s.State = v
		}
		if v, ok := row["sent_lsn"].(string); ok {
			s.SentLSN = v
		}
		if v, ok := row["write_lsn"].(string); ok {
			s.WriteLSN = v
		}
		if v, ok := row["flush_lsn"].(string); ok {
			s.FlushLSN = v
		}
		if v, ok := row["replay_lsn"].(string); ok {
			s.ReplayLSN = v
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// ReplicationSlot holds replication slot information.
type ReplicationSlot struct {
	Name     string
	Plugin   string
	SlotType string
	Active   bool
	RestLSN  string
}

// ListReplicationSlots returns all replication slots.
func (p *PostgresProvider) ListReplicationSlots(ctx context.Context) ([]ReplicationSlot, error) {
	rows, err := p.Query(ctx,
		`SELECT slot_name, plugin, slot_type, active, restart_lsn::text
		 FROM pg_replication_slots ORDER BY slot_name`)
	if err != nil {
		return nil, err
	}
	slots := make([]ReplicationSlot, 0, len(rows))
	for _, row := range rows {
		s := ReplicationSlot{}
		if v, ok := row["slot_name"].(string); ok {
			s.Name = v
		}
		if v, ok := row["plugin"].(string); ok {
			s.Plugin = v
		}
		if v, ok := row["slot_type"].(string); ok {
			s.SlotType = v
		}
		if v, ok := row["active"].(bool); ok {
			s.Active = v
		}
		if v, ok := row["restart_lsn"].(string); ok {
			s.RestLSN = v
		}
		slots = append(slots, s)
	}
	return slots, nil
}

// PublicationInfo holds logical replication publication info.
type PublicationInfo struct {
	Name      string
	Owner     string
	AllTables bool
	Insert    bool
	Update    bool
	Delete    bool
	Tables    []string
}

// ListPublications returns logical replication publications.
func (p *PostgresProvider) ListPublications(ctx context.Context) ([]PublicationInfo, error) {
	rows, err := p.Query(ctx,
		`SELECT pubname, pg_catalog.pg_get_userbyid(pubowner) AS owner,
		        puballtables, pubinsert, pubupdate, pubdelete
		 FROM pg_publication ORDER BY pubname`)
	if err != nil {
		return nil, err
	}
	pubs := make([]PublicationInfo, 0, len(rows))
	for _, row := range rows {
		pub := PublicationInfo{}
		if v, ok := row["pubname"].(string); ok {
			pub.Name = v
		}
		if v, ok := row["owner"].(string); ok {
			pub.Owner = v
		}
		if v, ok := row["puballtables"].(bool); ok {
			pub.AllTables = v
		}
		if v, ok := row["pubinsert"].(bool); ok {
			pub.Insert = v
		}
		if v, ok := row["pubupdate"].(bool); ok {
			pub.Update = v
		}
		if v, ok := row["pubdelete"].(bool); ok {
			pub.Delete = v
		}

		// Fetch tables for this publication.
		if !pub.AllTables {
			tblRows, err := p.Query(ctx,
				"SELECT schemaname || '.' || tablename AS tbl FROM pg_publication_tables WHERE pubname = $1", pub.Name)
			if err == nil {
				for _, tr := range tblRows {
					if t, ok := tr["tbl"].(string); ok {
						pub.Tables = append(pub.Tables, t)
					}
				}
			}
		}
		pubs = append(pubs, pub)
	}
	return pubs, nil
}

// --- Extensions ---

// ExtensionInfo holds metadata about a PostgreSQL extension.
type ExtensionInfo struct {
	Name      string
	Version   string
	Schema    string
	Comment   string
	Installed bool
}

// ListExtensions lists installed or available extensions.
func (p *PostgresProvider) ListExtensions(ctx context.Context, available bool) ([]ExtensionInfo, error) {
	var q string
	if available {
		q = `SELECT e.name, e.default_version AS version,
		            COALESCE(e.comment, '') AS comment,
		            (i.extname IS NOT NULL) AS installed
		     FROM pg_available_extensions e
		     LEFT JOIN pg_extension i ON i.extname = e.name
		     ORDER BY e.name`
	} else {
		q = `SELECT extname AS name, extversion AS version,
		            n.nspname AS schema, '' AS comment, true AS installed
		     FROM pg_extension e
		     JOIN pg_namespace n ON n.oid = e.extnamespace
		     ORDER BY extname`
	}
	rows, err := p.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	exts := make([]ExtensionInfo, 0, len(rows))
	for _, row := range rows {
		ext := ExtensionInfo{}
		if v, ok := row["name"].(string); ok {
			ext.Name = v
		}
		if v, ok := row["version"].(string); ok {
			ext.Version = v
		}
		if v, ok := row["schema"].(string); ok {
			ext.Schema = v
		}
		if v, ok := row["comment"].(string); ok {
			ext.Comment = v
		}
		if v, ok := row["installed"].(bool); ok {
			ext.Installed = v
		}
		exts = append(exts, ext)
	}
	return exts, nil
}

// EnableExtension installs a PostgreSQL extension.
func (p *PostgresProvider) EnableExtension(ctx context.Context, name, schema string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE EXTENSION IF NOT EXISTS %s", quoteIdentPG(name))
	if schema != "" {
		fmt.Fprintf(&b, " SCHEMA %s", quoteIdentPG(schema))
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// --- Performance / Bloat ---

// BloatInfo holds table or index bloat statistics.
type BloatInfo struct {
	Name          string
	SchemaName    string
	TableSize     int64
	DeadTuples    int64
	LiveTuples    int64
	LastVacuum    string
	LastAutoVac   string
	LastAnalyze   string
	BloatRatio    float64
}

// TableBloat returns bloat statistics for tables.
func (p *PostgresProvider) TableBloat(ctx context.Context, table string) ([]BloatInfo, error) {
	var q string
	var args []any
	if table != "" {
		q = `SELECT schemaname, relname,
		            pg_relation_size(relid) AS table_bytes,
		            n_dead_tup, n_live_tup,
		            COALESCE(last_vacuum::text, '') AS last_vacuum,
		            COALESCE(last_autovacuum::text, '') AS last_autovacuum,
		            COALESCE(last_analyze::text, '') AS last_analyze,
		            CASE WHEN n_live_tup > 0 THEN ROUND(n_dead_tup::numeric / n_live_tup * 100, 2) ELSE 0 END AS bloat_ratio
		     FROM pg_stat_user_tables
		     WHERE relname = $1
		     ORDER BY n_dead_tup DESC`
		args = []any{table}
	} else {
		q = `SELECT schemaname, relname,
		            pg_relation_size(relid) AS table_bytes,
		            n_dead_tup, n_live_tup,
		            COALESCE(last_vacuum::text, '') AS last_vacuum,
		            COALESCE(last_autovacuum::text, '') AS last_autovacuum,
		            COALESCE(last_analyze::text, '') AS last_analyze,
		            CASE WHEN n_live_tup > 0 THEN ROUND(n_dead_tup::numeric / n_live_tup * 100, 2) ELSE 0 END AS bloat_ratio
		     FROM pg_stat_user_tables
		     ORDER BY n_dead_tup DESC
		     LIMIT 50`
	}
	rows, err := p.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	results := make([]BloatInfo, 0, len(rows))
	for _, row := range rows {
		b := BloatInfo{}
		if v, ok := row["schemaname"].(string); ok {
			b.SchemaName = v
		}
		if v, ok := row["relname"].(string); ok {
			b.Name = v
		}
		b.TableSize = toInt64(row["table_bytes"])
		b.DeadTuples = toInt64(row["n_dead_tup"])
		b.LiveTuples = toInt64(row["n_live_tup"])
		if v, ok := row["last_vacuum"].(string); ok {
			b.LastVacuum = v
		}
		if v, ok := row["last_autovacuum"].(string); ok {
			b.LastAutoVac = v
		}
		if v, ok := row["last_analyze"].(string); ok {
			b.LastAnalyze = v
		}
		if v, ok := row["bloat_ratio"]; ok {
			switch n := v.(type) {
			case float64:
				b.BloatRatio = n
			case string:
				fmt.Sscanf(n, "%f", &b.BloatRatio)
			}
		}
		results = append(results, b)
	}
	return results, nil
}

// IndexBloatInfo holds index-level bloat statistics.
type IndexBloatInfo struct {
	SchemaName string
	TableName  string
	IndexName  string
	IndexSize  int64
	TableSize  int64
	BloatPct   float64
}

// IndexBloat returns estimated index bloat data.
func (p *PostgresProvider) IndexBloat(ctx context.Context, minBloatPct float64) ([]IndexBloatInfo, error) {
	if minBloatPct <= 0 {
		minBloatPct = 30
	}
	rows, err := p.Query(ctx,
		`SELECT s.schemaname, s.relname AS tablename, s.indexrelname AS indexname,
		        pg_relation_size(s.indexrelid) AS index_bytes,
		        pg_relation_size(s.relid) AS table_bytes,
		        CASE WHEN idx.idx_scan = 0 AND pg_relation_size(s.indexrelid) > 0
		             THEN 100
		             ELSE ROUND(
		                (1.0 - (COALESCE(idx.idx_tup_read,0)::numeric /
		                         GREATEST(idx.idx_scan,1)::numeric) /
		                         GREATEST(pg_relation_size(s.indexrelid)::numeric / 8192, 1)
		                ) * 100, 2)
		        END AS bloat_pct
		 FROM pg_stat_user_indexes s
		 JOIN pg_stat_all_indexes idx ON idx.indexrelid = s.indexrelid
		 WHERE pg_relation_size(s.indexrelid) > 65536
		 ORDER BY pg_relation_size(s.indexrelid) DESC
		 LIMIT 50`)
	if err != nil {
		return nil, err
	}
	results := make([]IndexBloatInfo, 0)
	for _, row := range rows {
		ib := IndexBloatInfo{}
		if v, ok := row["schemaname"].(string); ok {
			ib.SchemaName = v
		}
		if v, ok := row["tablename"].(string); ok {
			ib.TableName = v
		}
		if v, ok := row["indexname"].(string); ok {
			ib.IndexName = v
		}
		ib.IndexSize = toInt64(row["index_bytes"])
		ib.TableSize = toInt64(row["table_bytes"])
		if v, ok := row["bloat_pct"]; ok {
			switch n := v.(type) {
			case float64:
				ib.BloatPct = n
			case string:
				fmt.Sscanf(n, "%f", &ib.BloatPct)
			}
		}
		results = append(results, ib)
	}
	return results, nil
}

// --- Triggers ---

// TriggerInfo holds metadata about a PostgreSQL trigger.
type TriggerInfo struct {
	Name      string
	Table     string
	Timing    string   // BEFORE, AFTER, INSTEAD OF
	Events    []string // INSERT, UPDATE, DELETE
	Level     string   // ROW, STATEMENT
	Function  string
	Condition string
	Enabled   string // O=origin, D=disabled, R=replica, A=always
}

// CreateTriggerFunction creates a PL/pgSQL function that returns TRIGGER.
func (p *PostgresProvider) CreateTriggerFunction(ctx context.Context, name, body, language string, replace bool) error {
	if language == "" {
		language = "plpgsql"
	}
	var b strings.Builder
	b.WriteString("CREATE ")
	if replace {
		b.WriteString("OR REPLACE ")
	}
	fmt.Fprintf(&b, "FUNCTION %s() RETURNS TRIGGER AS $fn_body$\n%s\n$fn_body$ LANGUAGE %s",
		quoteIdentPG(name), body, language)
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// CreateTrigger creates a trigger on a table.
func (p *PostgresProvider) CreateTrigger(ctx context.Context, table, name, timing string, events []string, level, function, whenCondition string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TRIGGER %s %s %s ON %s",
		quoteIdentPG(name),
		strings.ToUpper(timing),
		strings.Join(events, " OR "),
		quoteIdentPG(table))
	if level == "" {
		level = "ROW"
	}
	fmt.Fprintf(&b, " FOR EACH %s", strings.ToUpper(level))
	if whenCondition != "" {
		fmt.Fprintf(&b, " WHEN (%s)", whenCondition)
	}
	fmt.Fprintf(&b, " EXECUTE FUNCTION %s()", quoteIdentPG(function))
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// ListTriggers lists triggers, optionally filtered by table.
func (p *PostgresProvider) ListTriggers(ctx context.Context, table string) ([]TriggerInfo, error) {
	q := `SELECT t.tgname AS name,
	             c.relname AS table_name,
	             CASE t.tgtype::int & 66
	               WHEN 2 THEN 'BEFORE'
	               WHEN 64 THEN 'INSTEAD OF'
	               ELSE 'AFTER'
	             END AS timing,
	             ARRAY_REMOVE(ARRAY[
	               CASE WHEN (t.tgtype::int & 4) != 0 THEN 'INSERT' END,
	               CASE WHEN (t.tgtype::int & 8) != 0 THEN 'DELETE' END,
	               CASE WHEN (t.tgtype::int & 16) != 0 THEN 'UPDATE' END,
	               CASE WHEN (t.tgtype::int & 32) != 0 THEN 'TRUNCATE' END
	             ], NULL)::text[] AS events,
	             CASE WHEN (t.tgtype::int & 1) != 0 THEN 'ROW' ELSE 'STATEMENT' END AS level,
	             p.proname AS function_name,
	             pg_get_triggerdef(t.oid) AS definition,
	             CASE t.tgenabled
	               WHEN 'O' THEN 'ORIGIN'
	               WHEN 'D' THEN 'DISABLED'
	               WHEN 'R' THEN 'REPLICA'
	               WHEN 'A' THEN 'ALWAYS'
	               ELSE t.tgenabled::text
	             END AS enabled
	      FROM pg_trigger t
	      JOIN pg_class c ON c.oid = t.tgrelid
	      JOIN pg_proc p ON p.oid = t.tgfoid
	      JOIN pg_namespace n ON n.oid = c.relnamespace
	      WHERE NOT t.tgisinternal
	        AND n.nspname = 'public'`
	var args []any
	if table != "" {
		q += " AND c.relname = $1"
		args = []any{table}
	}
	q += " ORDER BY c.relname, t.tgname"

	rows, err := p.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	triggers := make([]TriggerInfo, 0, len(rows))
	for _, row := range rows {
		ti := TriggerInfo{}
		if v, ok := row["name"].(string); ok {
			ti.Name = v
		}
		if v, ok := row["table_name"].(string); ok {
			ti.Table = v
		}
		if v, ok := row["timing"].(string); ok {
			ti.Timing = v
		}
		if v, ok := row["events"].(string); ok {
			// Parse PostgreSQL text array format: {INSERT,UPDATE}
			v = strings.Trim(v, "{}")
			if v != "" {
				ti.Events = strings.Split(v, ",")
			}
		}
		if v, ok := row["level"].(string); ok {
			ti.Level = v
		}
		if v, ok := row["function_name"].(string); ok {
			ti.Function = v
		}
		if v, ok := row["enabled"].(string); ok {
			ti.Enabled = v
		}
		triggers = append(triggers, ti)
	}
	return triggers, nil
}

// DropTrigger drops a trigger from a table.
func (p *PostgresProvider) DropTrigger(ctx context.Context, table, name string, cascade bool) error {
	var b strings.Builder
	fmt.Fprintf(&b, "DROP TRIGGER IF EXISTS %s ON %s", quoteIdentPG(name), quoteIdentPG(table))
	if cascade {
		b.WriteString(" CASCADE")
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// --- Events (LISTEN/NOTIFY) ---

// Notify sends a NOTIFY on a channel with optional payload.
func (p *PostgresProvider) Notify(ctx context.Context, channel, payload string) error {
	if payload != "" {
		_, err := p.sqlDB.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, payload)
		return err
	}
	_, err := p.sqlDB.ExecContext(ctx, "SELECT pg_notify($1, '')", channel)
	return err
}

// Listen registers a LISTEN on a channel.
func (p *PostgresProvider) Listen(ctx context.Context, channel string) error {
	_, err := p.sqlDB.ExecContext(ctx, fmt.Sprintf("LISTEN %s", quoteIdentPG(channel)))
	return err
}

// ListChannels returns channels currently being listened on.
func (p *PostgresProvider) ListChannels(ctx context.Context) ([]string, error) {
	rows, err := p.Query(ctx, "SELECT pg_listening_channels() AS channel")
	if err != nil {
		return nil, err
	}
	channels := make([]string, 0, len(rows))
	for _, row := range rows {
		if v, ok := row["channel"].(string); ok {
			channels = append(channels, v)
		}
	}
	return channels, nil
}

// --- Materialized Views ---

// CreateMaterializedView creates a materialized view.
func (p *PostgresProvider) CreateMaterializedView(ctx context.Context, name, query string, withData bool) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE MATERIALIZED VIEW %s AS %s", quoteIdentPG(name), query)
	if !withData {
		b.WriteString(" WITH NO DATA")
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// RefreshMaterializedView refreshes a materialized view.
func (p *PostgresProvider) RefreshMaterializedView(ctx context.Context, name string, concurrently bool) error {
	var b strings.Builder
	b.WriteString("REFRESH MATERIALIZED VIEW ")
	if concurrently {
		b.WriteString("CONCURRENTLY ")
	}
	b.WriteString(quoteIdentPG(name))
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// --- Full-Text Search ---

// AddTsvectorColumn adds a tsvector column with an auto-update trigger.
func (p *PostgresProvider) AddTsvectorColumn(ctx context.Context, table, column, language string, sourceColumns []string) error {
	if column == "" {
		column = "search_vector"
	}
	if language == "" {
		language = "english"
	}

	// Add the tsvector column.
	q := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s tsvector",
		quoteIdentPG(table), quoteIdentPG(column))
	if _, err := p.sqlDB.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("add column: %w", err)
	}

	// Build two tsvector expressions: one for trigger (NEW.col), one for backfill (bare col).
	var trigParts, backfillParts []string
	for i, col := range sourceColumns {
		weight := string(rune('A' + i)) // A, B, C, D...
		if i > 3 {
			weight = "D"
		}
		trigParts = append(trigParts, fmt.Sprintf("setweight(to_tsvector('%s', COALESCE(NEW.%s, '')), '%s')",
			language, quoteIdentPG(col), weight))
		backfillParts = append(backfillParts, fmt.Sprintf("setweight(to_tsvector('%s', COALESCE(%s, '')), '%s')",
			language, quoteIdentPG(col), weight))
	}
	trigExpr := strings.Join(trigParts, " || ")
	backfillExpr := strings.Join(backfillParts, " || ")

	// Create trigger function for auto-update.
	fnName := fmt.Sprintf("%s_%s_update", table, column)
	fnBody := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $$
BEGIN
  NEW.%s := %s;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql`, quoteIdentPG(fnName), quoteIdentPG(column), trigExpr)
	if _, err := p.sqlDB.ExecContext(ctx, fnBody); err != nil {
		return fmt.Errorf("create function: %w", err)
	}

	// Create trigger.
	trigName := fmt.Sprintf("trg_%s_%s", table, column)
	trigSQL := fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s; CREATE TRIGGER %s BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION %s()",
		quoteIdentPG(trigName), quoteIdentPG(table),
		quoteIdentPG(trigName), quoteIdentPG(table),
		quoteIdentPG(fnName))
	if _, err := p.sqlDB.ExecContext(ctx, trigSQL); err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	// Backfill existing rows.
	backfill := fmt.Sprintf("UPDATE %s SET %s = %s",
		quoteIdentPG(table), quoteIdentPG(column), backfillExpr)
	if _, err := p.sqlDB.ExecContext(ctx, backfill); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}

	return nil
}

// CreateGINIndex creates a GIN index on a tsvector or JSONB column.
func (p *PostgresProvider) CreateGINIndex(ctx context.Context, table, column, indexName string) error {
	if indexName == "" {
		indexName = fmt.Sprintf("idx_%s_%s_gin", table, column)
	}
	q := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIN (%s)",
		quoteIdentPG(indexName), quoteIdentPG(table), quoteIdentPG(column))
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// FTSSearch performs a full-text search on a tsvector column.
func (p *PostgresProvider) FTSSearch(ctx context.Context, table, column, query, language string, limit int) ([]map[string]any, error) {
	if column == "" {
		column = "search_vector"
	}
	if language == "" {
		language = "english"
	}
	if limit <= 0 {
		limit = 20
	}

	q := fmt.Sprintf(
		`SELECT *, ts_rank(%s, plainto_tsquery('%s', $1)) AS rank
		 FROM %s
		 WHERE %s @@ plainto_tsquery('%s', $1)
		 ORDER BY rank DESC
		 LIMIT %d`,
		quoteIdentPG(column), language,
		quoteIdentPG(table),
		quoteIdentPG(column), language,
		limit)

	return p.Query(ctx, q, query)
}

// --- pgvector ---

// Distance operator mappings for pgvector.
var distanceOps = map[string]string{
	"cosine": "<=>",
	"l2":     "<->",
	"ip":     "<#>",
}

var indexOps = map[string]string{
	"cosine": "vector_cosine_ops",
	"l2":     "vector_l2_ops",
	"ip":     "vector_ip_ops",
}

// VectorStats holds statistics about a vector column.
type VectorStats struct {
	Table      string
	Column     string
	Dimensions int
	RowCount   int64
	IndexName  string
	IndexType  string
	IndexSize  string
}

// EnableVectors enables the pgvector extension.
func (p *PostgresProvider) EnableVectors(ctx context.Context) error {
	_, err := p.sqlDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	return err
}

// AddVectorColumn adds a vector column to a table.
func (p *PostgresProvider) AddVectorColumn(ctx context.Context, table, column string, dimensions int) error {
	if column == "" {
		column = "embedding"
	}
	q := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s vector(%d)",
		quoteIdentPG(table), quoteIdentPG(column), dimensions)
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// CreateVectorIndex creates an HNSW or IVFFlat index on a vector column.
func (p *PostgresProvider) CreateVectorIndex(ctx context.Context, table, column, method, distance string, m, efConstruction, lists int) error {
	if method == "" {
		method = "hnsw"
	}
	if distance == "" {
		distance = "cosine"
	}

	ops, ok := indexOps[distance]
	if !ok {
		return fmt.Errorf("unsupported distance metric %q (use cosine, l2, or ip)", distance)
	}

	indexName := fmt.Sprintf("idx_%s_%s_%s", table, column, method)

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE INDEX %s ON %s USING %s (%s %s)",
		quoteIdentPG(indexName), quoteIdentPG(table), method, quoteIdentPG(column), ops)

	switch strings.ToLower(method) {
	case "hnsw":
		if m <= 0 {
			m = 16
		}
		if efConstruction <= 0 {
			efConstruction = 64
		}
		fmt.Fprintf(&b, " WITH (m = %d, ef_construction = %d)", m, efConstruction)
	case "ivfflat":
		if lists <= 0 {
			lists = 100
		}
		fmt.Fprintf(&b, " WITH (lists = %d)", lists)
	default:
		return fmt.Errorf("unsupported index method %q (use hnsw or ivfflat)", method)
	}

	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// VectorSearch performs a similarity search on a vector column.
func (p *PostgresProvider) VectorSearch(ctx context.Context, table, column string, queryVector []float64, distance string, limit int, selectColumns []string, where string) ([]map[string]any, error) {
	if distance == "" {
		distance = "cosine"
	}
	op, ok := distanceOps[distance]
	if !ok {
		return nil, fmt.Errorf("unsupported distance metric %q (use cosine, l2, or ip)", distance)
	}
	if limit <= 0 {
		limit = 10
	}

	// Format vector as PostgreSQL literal.
	vecStr := formatVector(queryVector)

	// Build SELECT columns.
	selCols := "*"
	if len(selectColumns) > 0 {
		quoted := make([]string, len(selectColumns))
		for i, c := range selectColumns {
			quoted[i] = quoteIdentPG(c)
		}
		selCols = strings.Join(quoted, ", ")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s, %s %s '%s' AS distance FROM %s",
		selCols, quoteIdentPG(column), op, vecStr, quoteIdentPG(table))
	if where != "" {
		fmt.Fprintf(&b, " WHERE %s", where)
	}
	fmt.Fprintf(&b, " ORDER BY %s %s '%s' LIMIT %d",
		quoteIdentPG(column), op, vecStr, limit)

	return p.Query(ctx, b.String())
}

// UpsertEmbedding inserts or updates an embedding vector for a row.
func (p *PostgresProvider) UpsertEmbedding(ctx context.Context, table, idColumn, idValue, column string, vector []float64, metadataCol string, metadata map[string]any) error {
	if column == "" {
		column = "embedding"
	}
	vecStr := formatVector(vector)

	if metadataCol != "" && metadata != nil {
		// With metadata column.
		metaJSON := mapToJSON(metadata)
		q := fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s) VALUES ($1, '%s', '%s') ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s",
			quoteIdentPG(table),
			quoteIdentPG(idColumn), quoteIdentPG(column), quoteIdentPG(metadataCol),
			vecStr, metaJSON,
			quoteIdentPG(idColumn),
			quoteIdentPG(column), quoteIdentPG(column),
			quoteIdentPG(metadataCol), quoteIdentPG(metadataCol))
		_, err := p.sqlDB.ExecContext(ctx, q, idValue)
		return err
	}

	q := fmt.Sprintf(
		"INSERT INTO %s (%s, %s) VALUES ($1, '%s') ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s",
		quoteIdentPG(table),
		quoteIdentPG(idColumn), quoteIdentPG(column),
		vecStr,
		quoteIdentPG(idColumn),
		quoteIdentPG(column), quoteIdentPG(column))
	_, err := p.sqlDB.ExecContext(ctx, q, idValue)
	return err
}

// BulkUpsertEmbeddings inserts or updates multiple embeddings in a transaction.
func (p *PostgresProvider) BulkUpsertEmbeddings(ctx context.Context, table, idColumn, column string, rows []EmbeddingRow) error {
	if column == "" {
		column = "embedding"
	}

	tx, err := p.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, row := range rows {
		vecStr := formatVector(row.Vector)
		q := fmt.Sprintf(
			"INSERT INTO %s (%s, %s) VALUES ($1, '%s') ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s",
			quoteIdentPG(table),
			quoteIdentPG(idColumn), quoteIdentPG(column),
			vecStr,
			quoteIdentPG(idColumn),
			quoteIdentPG(column), quoteIdentPG(column))
		if _, err := tx.ExecContext(ctx, q, row.ID); err != nil {
			return fmt.Errorf("upsert row %s: %w", row.ID, err)
		}
	}

	return tx.Commit()
}

// EmbeddingRow represents a row for bulk upsert.
type EmbeddingRow struct {
	ID     string
	Vector []float64
}

// GetVectorStats returns statistics about a vector column.
func (p *PostgresProvider) GetVectorStats(ctx context.Context, table, column string) (*VectorStats, error) {
	if column == "" {
		column = "embedding"
	}

	stats := &VectorStats{
		Table:  table,
		Column: column,
	}

	// Get dimensions from column type.
	dimRows, err := p.Query(ctx,
		`SELECT atttypmod AS dims
		 FROM pg_attribute
		 WHERE attrelid = $1::regclass AND attname = $2`, table, column)
	if err == nil && len(dimRows) > 0 {
		stats.Dimensions = int(toInt64(dimRows[0]["dims"]))
	}

	// Get row count.
	countRows, err := p.Query(ctx,
		fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s WHERE %s IS NOT NULL",
			quoteIdentPG(table), quoteIdentPG(column)))
	if err == nil && len(countRows) > 0 {
		stats.RowCount = toInt64(countRows[0]["cnt"])
	}

	// Get index info.
	idxRows, err := p.Query(ctx,
		`SELECT indexname, indexdef, pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size
		 FROM pg_indexes
		 WHERE tablename = $1 AND indexdef LIKE '%' || $2 || '%'`, table, column)
	if err == nil && len(idxRows) > 0 {
		row := idxRows[0]
		if v, ok := row["indexname"].(string); ok {
			stats.IndexName = v
		}
		if v, ok := row["indexdef"].(string); ok {
			if strings.Contains(strings.ToLower(v), "hnsw") {
				stats.IndexType = "hnsw"
			} else if strings.Contains(strings.ToLower(v), "ivfflat") {
				stats.IndexType = "ivfflat"
			} else {
				stats.IndexType = "btree"
			}
		}
		if v, ok := row["index_size"].(string); ok {
			stats.IndexSize = v
		}
	} else {
		stats.IndexType = "none"
	}

	return stats, nil
}

// DeleteEmbeddings deletes rows by ID list or WHERE filter.
func (p *PostgresProvider) DeleteEmbeddings(ctx context.Context, table, idColumn string, ids []string, where string) (int64, error) {
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		args := make([]any, len(ids))
		for i, id := range ids {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}
		q := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
			quoteIdentPG(table), quoteIdentPG(idColumn), strings.Join(placeholders, ", "))
		return p.Exec(ctx, q, args...)
	}
	if where != "" {
		q := fmt.Sprintf("DELETE FROM %s WHERE %s",
			quoteIdentPG(table), where)
		return p.Exec(ctx, q)
	}
	return 0, fmt.Errorf("either ids or where filter is required")
}

// formatVector formats a float64 slice as a PostgreSQL vector literal: '[1.0,2.0,3.0]'
func formatVector(v []float64) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// mapToJSON converts a map to a JSON string for SQL embedding.
func mapToJSON(m map[string]any) string {
	if m == nil {
		return "{}"
	}
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%q: %q", k, fmt.Sprintf("%v", v)))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// --- Roles & Permissions (universal but with PG-specific SQL) ---

// RoleInfo holds metadata about a database role.
type RoleInfo struct {
	Name        string
	SuperUser   bool
	CreateDB    bool
	CreateRole  bool
	Login       bool
	Replication bool
	ConnLimit   int
}

// ListRoles lists all roles/users.
func (p *PostgresProvider) ListRoles(ctx context.Context) ([]RoleInfo, error) {
	rows, err := p.Query(ctx,
		`SELECT rolname, rolsuper, rolcreatedb, rolcreaterole,
		        rolcanlogin, rolreplication, rolconnlimit
		 FROM pg_roles
		 WHERE rolname NOT LIKE 'pg_%'
		 ORDER BY rolname`)
	if err != nil {
		return nil, err
	}
	roles := make([]RoleInfo, 0, len(rows))
	for _, row := range rows {
		r := RoleInfo{}
		if v, ok := row["rolname"].(string); ok {
			r.Name = v
		}
		if v, ok := row["rolsuper"].(bool); ok {
			r.SuperUser = v
		}
		if v, ok := row["rolcreatedb"].(bool); ok {
			r.CreateDB = v
		}
		if v, ok := row["rolcreaterole"].(bool); ok {
			r.CreateRole = v
		}
		if v, ok := row["rolcanlogin"].(bool); ok {
			r.Login = v
		}
		if v, ok := row["rolreplication"].(bool); ok {
			r.Replication = v
		}
		r.ConnLimit = int(toInt64(row["rolconnlimit"]))
		roles = append(roles, r)
	}
	return roles, nil
}

// CreateRole creates a new database role.
func (p *PostgresProvider) CreateRole(ctx context.Context, name, password string, login, createdb, superuser bool) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE ROLE %s", quoteIdentPG(name))
	var opts []string
	if login {
		opts = append(opts, "LOGIN")
	}
	if createdb {
		opts = append(opts, "CREATEDB")
	}
	if superuser {
		opts = append(opts, "SUPERUSER")
	}
	if password != "" {
		opts = append(opts, fmt.Sprintf("PASSWORD '%s'", strings.ReplaceAll(password, "'", "''")))
	}
	if len(opts) > 0 {
		b.WriteString(" WITH ")
		b.WriteString(strings.Join(opts, " "))
	}
	_, err := p.sqlDB.ExecContext(ctx, b.String())
	return err
}

// Grant grants privileges to a role.
func (p *PostgresProvider) Grant(ctx context.Context, privileges, on, to string) error {
	q := fmt.Sprintf("GRANT %s ON %s TO %s", privileges, on, quoteIdentPG(to))
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// Revoke revokes privileges from a role.
func (p *PostgresProvider) Revoke(ctx context.Context, privileges, on, from string) error {
	q := fmt.Sprintf("REVOKE %s ON %s FROM %s", privileges, on, quoteIdentPG(from))
	_, err := p.sqlDB.ExecContext(ctx, q)
	return err
}

// quoteIdentPG quotes a PostgreSQL identifier with double-quotes,
// escaping any embedded double-quote characters.
func quoteIdentPG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// toInt64 converts an interface value to int64, handling common database driver types.
func toInt64(v any) int64 {
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

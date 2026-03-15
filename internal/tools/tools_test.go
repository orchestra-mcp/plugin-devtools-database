package tools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"google.golang.org/protobuf/types/known/structpb"

	// Register the modernc pure-Go SQLite driver.
	_ "modernc.org/sqlite"

	// Register provider factories (SQLite, Postgres, MySQL).
	_ "github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
)

// ---------- helpers ----------

func callTool(t *testing.T, handler func(context.Context, *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error), args map[string]any) *pluginv1.ToolResponse {
	t.Helper()
	s, err := structpb.NewStruct(args)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	resp, err := handler(context.Background(), &pluginv1.ToolRequest{Arguments: s})
	if err != nil {
		t.Fatalf("handler returned Go error: %v", err)
	}
	return resp
}

func isError(resp *pluginv1.ToolResponse) bool {
	return resp != nil && !resp.Success
}

func getText(resp *pluginv1.ToolResponse) string {
	if resp == nil {
		return ""
	}
	if r := resp.GetResult(); r != nil {
		if f := r.GetFields(); f != nil {
			if tf, ok := f["text"]; ok {
				return tf.GetStringValue()
			}
		}
	}
	return ""
}

// newSQLiteManager creates a Manager with one in-memory SQLite connection.
// The modernc driver is named "sqlite" (not "sqlite3").
func newSQLiteManager(t *testing.T) (*db.Manager, string) {
	t.Helper()
	mgr := db.NewManager()
	connID, err := mgr.Connect("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Connect sqlite: %v", err)
	}
	return mgr, connID
}

// createTestTable creates a simple users table and inserts rows.
func createTestTable(t *testing.T, mgr *db.Manager, connID string) {
	t.Helper()
	if _, err := mgr.Exec(connID, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i, name := range []string{"Alice", "Bob", "Charlie"} {
		if _, err := mgr.Exec(connID, `INSERT INTO users (id, name, age) VALUES (?, ?, ?)`, i+1, name, 20+i); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}
}

// ---------- db_connect ----------

func TestDbConnect_UnsupportedDriver(t *testing.T) {
	mgr := db.NewManager()
	resp := callTool(t, DbConnect(mgr), map[string]any{
		"driver": "oracle",
		"dsn":    "whatever",
	})
	if !isError(resp) {
		t.Error("expected error for unsupported driver")
	}
}

func TestDbConnect_MissingArgs(t *testing.T) {
	mgr := db.NewManager()
	resp := callTool(t, DbConnect(mgr), map[string]any{"driver": "sqlite"})
	if !isError(resp) {
		t.Error("expected validation error when dsn is missing")
	}
}

func TestDbConnect_SQLite_InMemory(t *testing.T) {
	mgr := db.NewManager()
	resp := callTool(t, DbConnect(mgr), map[string]any{
		"driver": "sqlite",
		"dsn":    ":memory:",
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	text := getText(resp)
	if !strings.Contains(text, "db-") {
		t.Errorf("expected connection ID in response, got: %s", text)
	}
}

// ---------- db_disconnect ----------

func TestDbDisconnect_NotFound(t *testing.T) {
	mgr := db.NewManager()
	resp := callTool(t, DbDisconnect(mgr), map[string]any{"connection_id": "db-nonexistent"})
	if !isError(resp) {
		t.Error("expected error for unknown connection ID")
	}
}

func TestDbDisconnect_Success(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	resp := callTool(t, DbDisconnect(mgr), map[string]any{"connection_id": connID})
	if isError(resp) {
		t.Errorf("unexpected error: %s", getText(resp))
	}
	// Second disconnect must fail.
	resp2 := callTool(t, DbDisconnect(mgr), map[string]any{"connection_id": connID})
	if !isError(resp2) {
		t.Error("expected error on second disconnect")
	}
}

// ---------- db_query ----------

func TestDbQuery_Select(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	resp := callTool(t, DbQuery(mgr), map[string]any{
		"connection_id": connID,
		"query":         "SELECT * FROM users",
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	text := getText(resp)
	if !strings.Contains(text, "3 rows") {
		t.Errorf("expected 3 rows in response, got: %s", text)
	}
}

func TestDbQuery_NoRows(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	resp := callTool(t, DbQuery(mgr), map[string]any{
		"connection_id": connID,
		"query":         "SELECT * FROM users WHERE age > 999",
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "0 rows") {
		t.Errorf("expected 0 rows, got: %s", getText(resp))
	}
}

func TestDbQuery_MissingConnectionID(t *testing.T) {
	mgr := db.NewManager()
	resp := callTool(t, DbQuery(mgr), map[string]any{"query": "SELECT 1"})
	if !isError(resp) {
		t.Error("expected validation error for missing connection_id")
	}
}

// ---------- db_list_tables ----------

func TestDbListTables_WithTables(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	resp := callTool(t, DbListTables(mgr), map[string]any{"connection_id": connID})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "users") {
		t.Errorf("expected 'users' in table list, got: %s", getText(resp))
	}
}

func TestDbListTables_Empty(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	resp := callTool(t, DbListTables(mgr), map[string]any{"connection_id": connID})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "No tables") {
		t.Errorf("expected 'No tables', got: %s", getText(resp))
	}
}

// ---------- db_describe_table ----------

func TestDbDescribeTable_Success(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	resp := callTool(t, DbDescribeTable(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users",
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	text := getText(resp)
	if !strings.Contains(text, "name") || !strings.Contains(text, "age") {
		t.Errorf("expected column names in output, got: %s", text)
	}
}

func TestDbDescribeTable_NonexistentTable(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	resp := callTool(t, DbDescribeTable(mgr), map[string]any{
		"connection_id": connID,
		"table":         "nonexistent",
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "not found") && !strings.Contains(getText(resp), "no columns") {
		t.Errorf("expected not-found message, got: %s", getText(resp))
	}
}

// ---------- db_list_connections ----------

func TestDbListConnections_Empty(t *testing.T) {
	mgr := db.NewManager()
	resp := callTool(t, DbListConnections(mgr), map[string]any{})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "No active") {
		t.Errorf("expected 'No active connections', got: %s", getText(resp))
	}
}

func TestDbListConnections_OneConnection(t *testing.T) {
	mgr, _ := newSQLiteManager(t)
	resp := callTool(t, DbListConnections(mgr), map[string]any{})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "sqlite") {
		t.Errorf("expected 'sqlite' in connections, got: %s", getText(resp))
	}
}

// ---------- db_export ----------

func TestDbExport_CSV(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	outPath := filepath.Join(t.TempDir(), "users.csv")
	resp := callTool(t, DbExport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users",
		"format":        "csv",
		"path":          outPath,
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "name") || !strings.Contains(content, "Alice") {
		t.Errorf("CSV missing expected content, got:\n%s", content)
	}
}

func TestDbExport_JSON(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	outPath := filepath.Join(t.TempDir(), "users.json")
	resp := callTool(t, DbExport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users",
		"format":        "json",
		"path":          outPath,
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.Contains(string(data), "Alice") {
		t.Errorf("JSON missing expected content: %s", string(data))
	}
}

func TestDbExport_InvalidFormat(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	resp := callTool(t, DbExport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users",
		"format":        "xml",
	})
	if !isError(resp) {
		t.Error("expected error for invalid format")
	}
}

// ---------- db_import ----------

func TestDbImport_CSV(t *testing.T) {
	mgr, connID := newSQLiteManager(t)

	// Create target table.
	if _, err := mgr.Exec(connID, `CREATE TABLE products (id TEXT, name TEXT, price TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Write CSV file.
	csvPath := filepath.Join(t.TempDir(), "products.csv")
	csvContent := "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n"
	if err := os.WriteFile(csvPath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	resp := callTool(t, DbImport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "products",
		"path":          csvPath,
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "2 rows") {
		t.Errorf("expected '2 rows' imported, got: %s", getText(resp))
	}
}

func TestDbImport_JSON(t *testing.T) {
	mgr, connID := newSQLiteManager(t)

	if _, err := mgr.Exec(connID, `CREATE TABLE items (id TEXT, label TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	jsonPath := filepath.Join(t.TempDir(), "items.json")
	jsonContent := `[{"id":"1","label":"foo"},{"id":"2","label":"bar"}]`
	if err := os.WriteFile(jsonPath, []byte(jsonContent), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	resp := callTool(t, DbImport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "items",
		"path":          jsonPath,
	})
	if isError(resp) {
		t.Fatalf("unexpected error: %s", getText(resp))
	}
	if !strings.Contains(getText(resp), "2 rows") {
		t.Errorf("expected '2 rows' imported, got: %s", getText(resp))
	}
}

func TestDbImport_UnknownExtension(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	resp := callTool(t, DbImport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users",
		"path":          "/tmp/data.xml",
	})
	if !isError(resp) {
		t.Error("expected error for unknown file extension")
	}
}

// ---------- round-trip: export then import ----------

func TestExportThenImport_CSV(t *testing.T) {
	mgr, connID := newSQLiteManager(t)
	createTestTable(t, mgr, connID)

	// Export to CSV.
	outPath := filepath.Join(t.TempDir(), "users.csv")
	callTool(t, DbExport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users",
		"format":        "csv",
		"path":          outPath,
	})

	// Create a second table and import.
	if _, err := mgr.Exec(connID, `CREATE TABLE users2 (age TEXT, id TEXT, name TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	resp := callTool(t, DbImport(mgr), map[string]any{
		"connection_id": connID,
		"table":         "users2",
		"path":          outPath,
	})
	if isError(resp) {
		t.Fatalf("import failed: %s", getText(resp))
	}

	// Verify row count.
	rows, err := mgr.Query(connID, "SELECT COUNT(*) as cnt FROM users2")
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one result row")
	}
	cnt := fmt.Sprintf("%v", rows[0]["cnt"])
	if cnt != "3" {
		t.Errorf("expected 3 rows in users2, got %s", cnt)
	}
}
